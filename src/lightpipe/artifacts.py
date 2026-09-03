from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from contextlib import suppress
from pathlib import Path
from typing import Any

from lightpipe.models import ArtifactRef


class ArtifactStore(ABC):
    @abstractmethod
    async def put(
        self, data: bytes, *, media_type: str = "application/octet-stream"
    ) -> ArtifactRef: ...

    @abstractmethod
    async def get(self, ref: ArtifactRef) -> bytes: ...

    @abstractmethod
    async def delete(self, ref: ArtifactRef) -> None: ...


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
