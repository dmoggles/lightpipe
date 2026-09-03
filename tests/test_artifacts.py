from __future__ import annotations

import pytest

from lightpipe import FileArtifactStore


@pytest.mark.asyncio
async def test_file_artifacts_are_content_addressed(tmp_path) -> None:
    store = FileArtifactStore(tmp_path)
    first = await store.put(b"payload", media_type="text/plain")
    second = await store.put(b"payload", media_type="text/plain")
    assert first.uri == second.uri
    assert await store.get(first) == b"payload"
    await store.delete(first)
    with pytest.raises(FileNotFoundError):
        await store.get(first)
