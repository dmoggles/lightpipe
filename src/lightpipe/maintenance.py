from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta

from lightpipe.artifacts import ArtifactStore
from lightpipe.backends.base import OrchestrationBackend
from lightpipe.models import ArtifactRef, utcnow
from lightpipe.observability import add_metric


@dataclass(slots=True)
class MaintenanceReport:
    pruned: dict[str, int] = field(default_factory=dict)
    inventoried: int = 0
    deleted_artifacts: int = 0
    deletion_errors: list[str] = field(default_factory=list)


class MaintenanceRunner:
    def __init__(
        self,
        backend: OrchestrationBackend,
        artifact_store: ArtifactStore | None = None,
        *,
        owner: str = "service",
        batch_size: int = 100,
        artifact_grace: timedelta = timedelta(days=1),
    ) -> None:
        if batch_size < 1:
            raise ValueError("maintenance batch size must be positive")
        self.backend = backend
        self.artifact_store = artifact_store
        self.owner = owner
        self.batch_size = batch_size
        self.artifact_grace = artifact_grace

    async def run_once(self, *, dry_run: bool = False) -> MaintenanceReport:
        report = MaintenanceReport()
        lease = await self.backend.claim_maintenance(
            "retention", self.owner, lease_for=timedelta(minutes=5)
        )
        if lease is None:
            return report
        try:
            now = utcnow()
            if not dry_run:
                report.pruned = await self.backend.prune(now=now, limit=self.batch_size)
                for kind, count in report.pruned.items():
                    add_metric("lightpipe.retention.deleted", count, kind=kind)
            if self.artifact_store is not None:
                async for artifact in self.artifact_store.objects():
                    if not dry_run:
                        await self.backend.catalog_artifact(artifact)
                    report.inventoried += 1
                candidates = (
                    []
                    if dry_run
                    else await self.backend.artifact_gc_candidates(
                        now=now, grace=self.artifact_grace, limit=self.batch_size
                    )
                )
                if not dry_run:
                    for artifact in candidates:
                        try:
                            await self.artifact_store.delete(
                                ArtifactRef(
                                    artifact.uri,
                                    digest=artifact.digest,
                                    size=artifact.size,
                                )
                            )
                            await self.backend.forget_artifact(artifact.uri)
                            report.deleted_artifacts += 1
                            add_metric("lightpipe.artifacts.deleted")
                        except Exception as error:
                            report.deletion_errors.append(
                                f"{artifact.uri}: {type(error).__name__}: {error}"
                            )
            return report
        finally:
            await self.backend.complete_maintenance("retention", lease.token)
