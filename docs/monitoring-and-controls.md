# Monitoring and recovery controls

The bundled dashboard at `/` uses the versioned `/api/v1` operator API. The original `/api`
routes remain compatibility shims during the 0.2 release line.

## Queries and streams

`GET /api/v1/runs` returns `{ "items": [...], "next_cursor": "..." }`. It accepts `limit`,
`cursor`, `pipeline`, `definition_hash`, `state`, `created_after`, and `created_before`. Time values
must be ISO 8601 timestamps with a UTC offset. Pass `next_cursor` back unchanged for the following
page. Definitions use the same envelope at `GET /api/v1/pipelines` and can be filtered by `name`
or `definition_hash`.

Run detail at `GET /api/v1/runs/{run_id}` includes the persisted compiled graph, dynamic task
instances, complete available attempt history, cache state, errors, timings, and artifact metadata.
Pre-migration tasks explicitly report incomplete attempt history rather than synthesized values.

Run events and task logs are resumable SSE resources:

```text
GET /api/v1/runs/{run_id}/events
GET /api/v1/tasks/{task_id}/logs/stream?attempt=2
```

Each message has an SSE `id`. Browsers reconnect with `Last-Event-ID`; non-browser clients can pass
the same value as `after`. Persisted logs can also be paged through
`GET /api/v1/tasks/{task_id}/logs`.

Default subprocess workers capture stdout, stderr, and Python logging records. The debugging-only
`--no-process-isolation` mode captures structured Python logs but deliberately does not redirect
process-global stdout or stderr.

## Recovery behavior

- `POST /api/v1/runs/{run_id}/cancel` fences running tasks and cancels pending work.
- `POST /api/v1/runs/{run_id}/rerun` creates a new run linked through `rerun_of`. The exact
  definition hash must be registered by the control service.
- `POST /api/v1/runs/{run_id}/retry-failed` reopens the original failed run. An optional JSON
  `task_ids` array selects failed items; omitting it retries all failed tasks.

A failed-task retry preserves successful mapped siblings and their descendants. Mapped edges keep
their original item indexes, so newly successful items create only their missing downstream work.
Invalid terminal states, stale selections, and concurrent duplicate recovery requests return HTTP
409 without partially reopening a run.

The equivalent CLI commands are `lightpipe cancel`, `lightpipe rerun`, and
`lightpipe retry-failed`. `lightpipe runs` supports the API's run filters, and `lightpipe inspect
RUN_ID --logs` includes graph, attempt, and log data.

## OpenTelemetry

Instrumentation remains off unless `LIGHTPIPE_OTEL_ENABLED=true` or
`OTEL_EXPORTER_OTLP_ENDPOINT` is set. Install `opentelemetry-sdk` and
`opentelemetry-exporter-otlp` in an exporting deployment. Lightpipe then emits OTLP task,
submission, reconciliation, trigger, and HTTP spans; outcome, retry, cache, duration, queue, and
trigger metrics; and stage logs carrying run, task, attempt, trace, and span correlation fields.
