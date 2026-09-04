from __future__ import annotations

import contextlib
import logging
import os
from collections.abc import Iterator
from typing import Any

_enabled = False
_tracer: Any = None
_meter: Any = None
_instruments: dict[tuple[str, str], Any] = {}


def configure_observability() -> bool:
    """Configure OTLP export when explicitly requested by environment."""
    global _enabled, _meter, _tracer
    requested = os.getenv("LIGHTPIPE_OTEL_ENABLED", "").lower() in {"1", "true", "yes"}
    requested = requested or bool(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
    if not requested or _enabled:
        return _enabled
    try:
        from opentelemetry import metrics, trace  # ty: ignore[unresolved-import]
        from opentelemetry._logs import set_logger_provider  # ty: ignore[unresolved-import]
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (  # ty: ignore[unresolved-import]
            OTLPLogExporter,
        )
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (  # ty: ignore[unresolved-import]
            OTLPMetricExporter,
        )
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (  # ty: ignore[unresolved-import]
            OTLPSpanExporter,
        )
        from opentelemetry.sdk._logs import (  # ty: ignore[unresolved-import]
            LoggerProvider,
            LoggingHandler,
        )
        from opentelemetry.sdk._logs.export import (  # ty: ignore[unresolved-import]
            BatchLogRecordProcessor,
        )
        from opentelemetry.sdk.metrics import MeterProvider  # ty: ignore[unresolved-import]
        from opentelemetry.sdk.metrics.export import (  # ty: ignore[unresolved-import]
            PeriodicExportingMetricReader,
        )
        from opentelemetry.sdk.resources import Resource  # ty: ignore[unresolved-import]
        from opentelemetry.sdk.trace import TracerProvider  # ty: ignore[unresolved-import]
        from opentelemetry.sdk.trace.export import (  # ty: ignore[unresolved-import]
            BatchSpanProcessor,
        )
    except ImportError as error:
        raise RuntimeError(
            "OpenTelemetry export was requested; install opentelemetry-sdk and "
            "opentelemetry-exporter-otlp"
        ) from error

    resource = Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME", "lightpipe")})
    trace_provider = TracerProvider(resource=resource)
    trace_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(trace_provider)
    metric_provider = MeterProvider(
        resource=resource,
        metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())],
    )
    metrics.set_meter_provider(metric_provider)
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
    set_logger_provider(logger_provider)
    logging.getLogger().addHandler(LoggingHandler(logger_provider=logger_provider))
    _tracer = trace.get_tracer("lightpipe")
    _meter = metrics.get_meter("lightpipe")
    _enabled = True
    return True


@contextlib.contextmanager
def span(name: str, **attributes: Any) -> Iterator[None]:
    if not _enabled:
        yield
        return
    with _tracer.start_as_current_span(name, attributes=attributes):
        yield


def add_metric(name: str, value: int = 1, **attributes: Any) -> None:
    if not _enabled:
        return
    key = ("counter", name)
    instrument = _instruments.get(key)
    if instrument is None:
        instrument = _meter.create_counter(name)
        _instruments[key] = instrument
    instrument.add(value, attributes)


def record_metric(name: str, value: float, **attributes: Any) -> None:
    if not _enabled:
        return
    key = ("histogram", name)
    instrument = _instruments.get(key)
    if instrument is None:
        instrument = _meter.create_histogram(name)
        _instruments[key] = instrument
    instrument.record(value, attributes)


def current_trace_ids() -> tuple[str | None, str | None]:
    if not _enabled:
        return None, None
    from opentelemetry import trace  # ty: ignore[unresolved-import]

    context = trace.get_current_span().get_span_context()
    if not context.is_valid:
        return None, None
    return f"{context.trace_id:032x}", f"{context.span_id:016x}"
