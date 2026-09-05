from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from lightpipe.models import ArtifactObject, ArtifactRef


class ArtifactStore(ABC):
    @abstractmethod
    async def put(
        self, data: bytes, *, media_type: str = "application/octet-stream"
    ) -> ArtifactRef: ...

    @abstractmethod
    async def get(self, ref: ArtifactRef) -> bytes: ...

    @abstractmethod
    async def delete(self, ref: ArtifactRef) -> None: ...

    @abstractmethod
    def objects(self) -> AsyncIterator[ArtifactObject]: ...


class FileArtifactStore(ArtifactStore):
    def __init__(self, root: str | Path = ".lightpipe/artifacts") -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    async def put(
        self, data: bytes, *, media_type: str = "application/octet-stream"
    ) -> ArtifactRef:
        digest = hashlib.sha256(data).hexdigest()
        target = self.root / digest[:2] / digest
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            target.write_bytes(data)
        return ArtifactRef(target.as_uri(), media_type, digest, len(data))

    async def get(self, ref: ArtifactRef) -> bytes:
        path = Path(ref.uri.removeprefix("file://"))
        data = path.read_bytes()
        if ref.digest and hashlib.sha256(data).hexdigest() != ref.digest:
            raise ValueError(f"artifact digest mismatch: {ref.uri}")
        return data

    async def delete(self, ref: ArtifactRef) -> None:
        path = Path(ref.uri.removeprefix("file://"))
        with suppress(FileNotFoundError):
            path.unlink()

    async def objects(self) -> AsyncIterator[ArtifactObject]:
        if not self.root.exists():
            return
        for path in self.root.glob("*/*"):
            if path.is_file():
                stat = path.stat()
                yield ArtifactObject(
                    path.as_uri(),
                    datetime.fromtimestamp(stat.st_mtime, UTC),
                    digest=path.name,
                    size=stat.st_size,
                )


class S3ArtifactStore(ArtifactStore):
    def __init__(
        self, bucket: str, *, prefix: str = "lightpipe", client: object | None = None
    ) -> None:
        try:
            import boto3
        except ImportError as error:
            raise RuntimeError("install lightpipe[s3] to use S3ArtifactStore") from error
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client: Any = client or boto3.client("s3")

    async def put(
        self, data: bytes, *, media_type: str = "application/octet-stream"
    ) -> ArtifactRef:
        digest = hashlib.sha256(data).hexdigest()
        key = f"{self.prefix}/{digest[:2]}/{digest}"
        import asyncio

        await asyncio.to_thread(
            self.client.put_object, Bucket=self.bucket, Key=key, Body=data, ContentType=media_type
        )
        return ArtifactRef(f"s3://{self.bucket}/{key}", media_type, digest, len(data))

    async def get(self, ref: ArtifactRef) -> bytes:
        import asyncio

        bucket, key = ref.uri.removeprefix("s3://").split("/", 1)
        response = await asyncio.to_thread(self.client.get_object, Bucket=bucket, Key=key)
        data = await asyncio.to_thread(response["Body"].read)
        if ref.digest and hashlib.sha256(data).hexdigest() != ref.digest:
            raise ValueError(f"artifact digest mismatch: {ref.uri}")
        return data

    async def delete(self, ref: ArtifactRef) -> None:
        import asyncio

        bucket, key = ref.uri.removeprefix("s3://").split("/", 1)
        await asyncio.to_thread(self.client.delete_object, Bucket=bucket, Key=key)

    async def objects(self) -> AsyncIterator[ArtifactObject]:
        import asyncio

        token: str | None = None
        while True:
            arguments: dict[str, Any] = {"Bucket": self.bucket, "Prefix": f"{self.prefix}/"}
            if token is not None:
                arguments["ContinuationToken"] = token
            page = await asyncio.to_thread(self.client.list_objects_v2, **arguments)
            for item in page.get("Contents", []):
                key = str(item["Key"])
                yield ArtifactObject(
                    f"s3://{self.bucket}/{key}",
                    item["LastModified"],
                    digest=key.rsplit("/", 1)[-1],
                    size=int(item["Size"]),
                )
            if not page.get("IsTruncated"):
                return
            token = str(page["NextContinuationToken"])


def load_artifact_store(url: str) -> ArtifactStore:
    parsed = urlparse(url)
    if parsed.scheme == "file":
        return FileArtifactStore(parsed.path)
    if parsed.scheme == "s3":
        return S3ArtifactStore(parsed.netloc, prefix=parsed.path.strip("/") or "lightpipe")
    raise ValueError(f"unsupported artifact store URL scheme: {parsed.scheme!r}")
