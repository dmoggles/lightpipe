from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pytest


def _request(url: str, body: dict[str, object] | None = None) -> dict[str, Any]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"} if data else {},
        method="POST" if data else "GET",
    )
    with urllib.request.urlopen(request, timeout=2) as response:
        return json.load(response)


def _wait_until(predicate: Callable[[], object], timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception:
            pass
        time.sleep(0.05)
    raise TimeoutError("condition was not met")


def _stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


@pytest.mark.postgres
def test_worker_kill_recovers_in_separate_processes(tmp_path: Path) -> None:
    dsn = os.getenv("LIGHTPIPE_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("LIGHTPIPE_TEST_POSTGRES_DSN is not configured")
    if not urlparse(dsn).path.removeprefix("/").endswith("_test"):
        pytest.fail("process recovery test requires a database name ending in _test")

    from lightpipe.migration import upgrade_database

    upgrade_database(dsn)
    import psycopg

    with psycopg.connect(dsn) as connection:
        connection.execute(
            "TRUNCATE lp_stage_logs,lp_task_attempts,lp_events,lp_expansions,lp_tasks,"
            "lp_runs,lp_cache,lp_triggers,lp_pipeline_definitions "
            "RESTART IDENTITY CASCADE"
        )
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]

    executable = str(Path(__file__).parents[1] / ".venv" / "bin" / "lightpipe")
    common = [executable, "--backend", dsn]
    pipeline = "examples.failure_recovery:failure_recovery"
    api = subprocess.Popen(
        [*common, "serve", pipeline, "--workers", "0", "--port", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    first_worker: subprocess.Popen[bytes] | None = None
    replacement: subprocess.Popen[bytes] | None = None
    marker = tmp_path / "first-attempt"
    try:
        _wait_until(lambda: _request(f"http://127.0.0.1:{port}/health/ready"))
        created = _request(
            f"http://127.0.0.1:{port}/api/pipelines/failure_recovery/runs",
            {"parameters": {"value": 21, "marker": str(marker)}},
        )
        run_id = str(created["id"])
        first_worker = subprocess.Popen(
            [*common, "worker", pipeline, "--lease-seconds", "1", "--poll-interval", "0.05"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _wait_until(marker.exists)
        first_worker.kill()
        first_worker.wait(timeout=5)
        time.sleep(1.2)
        replacement = subprocess.Popen(
            [*common, "worker", pipeline, "--lease-seconds", "1", "--poll-interval", "0.05"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        detail: dict[str, Any] = {}

        def succeeded() -> bool:
            nonlocal detail
            detail = _request(f"http://127.0.0.1:{port}/api/runs/{run_id}")
            return isinstance(detail.get("run"), dict) and detail["run"]["state"] == "succeeded"

        _wait_until(succeeded)
        assert detail["run"]["output"] == 42
        assert max(task["attempt"] for task in detail["tasks"]) == 2
    finally:
        if first_worker is not None:
            _stop(first_worker)
        if replacement is not None:
            _stop(replacement)
        _stop(api)
