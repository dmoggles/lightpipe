from __future__ import annotations

import asyncio
import inspect
import multiprocessing
import traceback
from collections.abc import Awaitable, Callable
from typing import Any


class StageExecutionError(RuntimeError):
    pass


class StageTimeoutError(TimeoutError):
    pass


def _child(
    connection: Any, function: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> None:
    try:
        result = function(*args, **kwargs)
        if inspect.isawaitable(result):
            result = asyncio.run(result)
        connection.send((True, result))
    except BaseException:
        connection.send((False, traceback.format_exc()))
    finally:
        connection.close()


async def execute_in_subprocess(
    function: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    timeout: float | None,
    heartbeat: Callable[[], Awaitable[None]] | None = None,
    heartbeat_interval: float = 1.0,
) -> Any:
    """Execute a stage in a supervised Linux child process."""
    context = multiprocessing.get_context("fork")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(target=_child, args=(child, function, args, kwargs), daemon=True)
    process.start()
    child.close()
    loop = asyncio.get_running_loop()
    started = loop.time()
    last_heartbeat = started
    try:
        while True:
            if parent.poll():
                succeeded, value = parent.recv()
                process.join(timeout=1)
                if succeeded:
                    return value
                raise StageExecutionError(value)
            if not process.is_alive():
                process.join(timeout=1)
                raise StageExecutionError(f"stage process exited with code {process.exitcode}")
            now = loop.time()
            if timeout is not None and now - started >= timeout:
                raise StageTimeoutError(f"stage exceeded its {timeout:g}s timeout")
            if heartbeat is not None and now - last_heartbeat >= heartbeat_interval:
                await heartbeat()
                last_heartbeat = now
            await asyncio.sleep(0.02)
    finally:
        parent.close()
        if process.is_alive():
            process.terminate()
        process.join(timeout=1)
