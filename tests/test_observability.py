from __future__ import annotations

import pytest

import lightpipe.observability as observability


def test_observability_is_off_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LIGHTPIPE_OTEL_ENABLED", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.setattr(observability, "_enabled", False)
    assert observability.configure_observability() is False
    with observability.span("test"):
        observability.add_metric("test.counter")


def test_observability_configuration_has_actionable_missing_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LIGHTPIPE_OTEL_ENABLED", "true")
    monkeypatch.setattr(observability, "_enabled", False)
    try:
        configured = observability.configure_observability()
    except RuntimeError as error:
        assert "install opentelemetry-sdk" in str(error)
    else:
        assert configured is True
