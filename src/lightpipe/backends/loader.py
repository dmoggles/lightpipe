from __future__ import annotations

from importlib.metadata import entry_points
from urllib.parse import urlparse

from lightpipe.backends.base import OrchestrationBackend
from lightpipe.backends.memory import MemoryBackend


async def load_backend(url: str) -> OrchestrationBackend:
    scheme = urlparse(url).scheme
    if scheme == "memory":
        return MemoryBackend()
    if scheme in {"postgres", "postgresql"}:
        from lightpipe.backends.postgres import PostgresBackend

        backend = PostgresBackend(url)
        await backend.initialize()
        return backend
    for entry_point in entry_points(group="lightpipe.backends"):
        if entry_point.name == scheme:
            backend = entry_point.load()(url)
            initialize = getattr(backend, "initialize", None)
            if initialize is not None:
                await initialize()
            return backend
    raise ValueError(f"no lightpipe backend is installed for URL scheme {scheme!r}")
