from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import io
import logging
import multiprocessing
import traceback
from collections.abc import Awaitable, Callable, Iterator
from typing import Any


class StageExecutionError(RuntimeError):
    pass


class StageTimeoutError(TimeoutError):
    pass


LogMessage = dict[str, Any]
_capture_marker: contextvars.ContextVar[object | None] = contextvars.ContextVar(
    "lightpipe_log_capture", default=None
)


def _safe_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_safe_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _safe_value(item) for key, item in value.items()}
    return repr(value)


_LOG_RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__)


class _CaptureHandler(logging.Handler):
    def __init__(self, emit_message: Callable[[LogMessage], None], marker: object | None) -> None:
        super().__init__()
        self.emit_message = emit_message
        self.marker = marker

    def emit(self, record: logging.LogRecord) -> None:
        if self.marker is not None and _capture_marker.get() is not self.marker:
            return
        fields = {
            key: _safe_value(value)
            for key, value in record.__dict__.items()
            if key not in _LOG_RECORD_FIELDS and key not in {"message", "asctime"}
        }
        self.emit_message(
            {
                "stream": "log",
                "level": record.levelname.lower(),
                "logger": record.name,
                "message": record.getMessage(),
                "fields": fields,
            }
        )


class _StreamCapture(io.TextIOBase):
    def __init__(self, stream: str, emit_message: Callable[[LogMessage], None]) -> None:
        self.stream = stream
        self.emit_message = emit_message
        self.buffer = ""

    def writable(self) -> bool:
        return True

    def write(self, value: str) -> int:
        self.buffer += value
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            self._emit(line)
        return len(value)

    def flush(self) -> None:
        if self.buffer:
            self._emit(self.buffer)
            self.buffer = ""

    def _emit(self, message: str) -> None:
        # Bound individual transport/database records while preserving all content.
        for offset in range(0, max(1, len(message)), 64 * 1024):
            self.emit_message(
                {
                    "stream": self.stream,
                    "level": "error" if self.stream == "stderr" else "info",
                    "logger": None,
                    "message": message[offset : offset + 64 * 1024],
                    "fields": {},
                }
            )


@contextlib.contextmanager
def capture_structured_logs() -> Iterator[list[LogMessage]]:
    """Capture logging from the current async context without redirecting process streams."""
    records: list[LogMessage] = []
    marker = object()
    token = _capture_marker.set(marker)
    handler = _CaptureHandler(records.append, marker)
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        yield records
    finally:
        root.removeHandler(handler)
        _capture_marker.reset(token)


def _child(
    connection: Any, function: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> None:
    def emit_message(message: LogMessage) -> None:
        connection.send(("log", message))

    stdout = _StreamCapture("stdout", emit_message)
    stderr = _StreamCapture("stderr", emit_message)
    root = logging.getLogger()
    old_handlers = root.handlers[:]
    root.handlers = [_CaptureHandler(emit_message, None)]
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = function(*args, **kwargs)
            if inspect.isawaitable(result):
                result = asyncio.run(result)
        stdout.flush()
        stderr.flush()
        connection.send(("result", True, result))
    except BaseException:
        stdout.flush()
        stderr.flush()
        connection.send(("result", False, traceback.format_exc()))
    finally:
        root.handlers = old_handlers
        connection.close()


async def execute_in_subprocess(
    function: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    timeout: float | None,
    heartbeat: Callable[[], Awaitable[None]] | None = None,
    log: Callable[[LogMessage], Awaitable[None]] | None = None,
    heartbeat_interval: float = 1.0,
    termination_grace: float = 1.0,
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
                frame = parent.recv()
                if frame[0] == "log":
                    if log is not None:
                        await log(frame[1])
                    continue
                _, succeeded, value = frame
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
            process.join(timeout=termination_grace)
        if process.is_alive():
            process.kill()
        process.join(timeout=1)
