# lightpipe

`lightpipe` is a small Python pipeline orchestrator built around ordinary decorated functions,
dynamic fan-out, durable at-least-once work delivery, and replaceable storage backends.

See [Project state and roadmap](docs/project-state-and-roadmap.md) for the current maturity of each
subsystem, known gaps, and the recommended implementation sequence.

For an operational walkthrough, see [Deploying pipelines with workers](docs/deploying-pipelines.md).
For operator APIs, recovery semantics, and telemetry, see
[Monitoring and recovery controls](docs/monitoring-and-controls.md).

The project currently includes:

- a typed `@stage` / `@pipeline` graph DSL;
- declarative `map` and `collect` operations;
- a backend-neutral orchestration contract and in-memory implementation;
- a Postgres adapter with task leases, fencing, retries, and append-only events;
- versioned Alembic migrations and a reproducible split-process Compose deployment;
- opt-in, TTL-bound result caching;
- filesystem and S3-compatible artifact stores;
- long-lived workers with supervised task subprocesses;
- schedule and stateful-poller definitions;
- timezone-aware cron schedules, signed webhooks, and managed trigger history;
- a CLI and a runnable FastAPI monitoring/control service.
- a bundled React operations dashboard with DAG, attempt, log, artifact, and recovery views;
- optional OpenTelemetry traces, metrics, and correlated logs.

## Development

Python environments and project commands are managed with Astral's `uv`. The lockfile is created
the first time dependencies are resolved.

```console
uv sync --all-groups --all-extras
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv run pytest
uv build
```

Ruff owns formatting, import sorting, and linting. `ty` is the type checker and language server.

## Installation

Installing the wheel creates a `lightpipe` executable in the active Python environment. Keep the
installation minimal or select only the integrations the deployment needs:

```console
pip install lightpipe
pip install "lightpipe[api]"
pip install "lightpipe[postgres]"
pip install "lightpipe[api,postgres]"
```

The base package contains the DSL, runtime, in-memory backend, and CLI. `api` installs FastAPI and
Uvicorn; `postgres` installs psycopg and Alembic. After installation, production commands run
directly—`uv` is not required:

```console
lightpipe --help
python -m lightpipe --help
```

## Defining a pipeline

```python
from datetime import timedelta

from lightpipe import CachePolicy, pipeline, stage


@stage
def scrape(target: str) -> list[dict[str, object]]: ...


@stage(cache=CachePolicy(timedelta(hours=4)), retries=2)
def predict(row: dict[str, object]) -> dict[str, object]: ...


@stage
def save(row: dict[str, object]) -> None: ...


@pipeline
def ingest(target: str):
    predictions = predict.map(scrape(target))
    return save.map(predictions)  # A terminal map needs no collect operation.
```

Calls inside a pipeline definition build a graph. Stage functions do not execute until a worker
claims the corresponding task. A mapped result can instead be passed to an aggregate stage using
`predictions.collect()`.

Run it locally:

```console
uv run lightpipe run examples.scrape_and_predict:scrape_and_predict \
  --parameters '{"target":"example"}'
```

## Launching the service and UI

Start the API, dashboard, reconciler, trigger scheduler, and one local worker with:

```console
uv run lightpipe serve examples.scrape_and_predict:scrape_and_predict
```

Open `http://127.0.0.1:8000` to submit and inspect runs. The service also exposes liveness at
`/health/live`, readiness at `/health/ready`, and worker/trigger status at `/api/workers`. Stop it
with Ctrl-C; active tasks are allowed a grace period and then safely released for another worker.

Pass additional `module:object` arguments to register pollers or schedules alongside pipelines.
Webhook definitions are accepted as well. For production, pass `--no-scheduler` to `serve` and
run independently scalable scheduler replicas:

```bash
uv run lightpipe --backend "$DATABASE_URL" scheduler \
  my_project:pipeline my_project:daily_schedule my_project:incoming_webhook
```

See [Trigger automation](docs/trigger-automation.md) for cron/DST behavior, overlap and missed-run
policies, webhook signing, pause/resume controls, recovery semantics, and a runnable example.
Use `--workers N` for a larger local worker pool and `--no-process-isolation` when debugging stage
functions in the server process. Run `uv run lightpipe serve --help` for all options.

The versioned `/api/v1` endpoints provide cursor-paginated run and definition queries, task-attempt
history, resumable event and log streams, cancellation, linked reruns, and in-place failed-task
retry. Existing `/api` endpoints remain available for compatibility.

The dashboard source is in `dashboard/`; its compiled assets are included in the Python wheel.
Rebuild them after frontend changes:

```console
cd dashboard
npm ci
npm run build
```

OpenTelemetry export is disabled unless `LIGHTPIPE_OTEL_ENABLED=true` or an
`OTEL_EXPORTER_OTLP_ENDPOINT` is configured. Install `opentelemetry-sdk` and
`opentelemetry-exporter-otlp` in deployments that enable export.

## Backends

`OrchestrationBackend` is a semantic boundary rather than a database CRUD interface. Adapters own
atomic run/task transitions, leases, fencing, event persistence, trigger ownership, and cache races.
Pipeline code sees none of those implementation details.

The in-memory backend provides matching execution behavior for tests and local development, without
restart durability. Postgres is the first durable adapter:

Initialize or upgrade its schema explicitly before starting services:

```console
lightpipe --backend postgresql://user:password@localhost/lightpipe db status
lightpipe --backend postgresql://user:password@localhost/lightpipe db upgrade
```

```python
from lightpipe.backends.postgres import PostgresBackend

backend = PostgresBackend("postgresql://user:password@localhost/lightpipe")
await backend.initialize()
```

`initialize()` opens the pool and verifies that the schema is current; it never applies migrations.

Workers are started with all pipeline definitions they are allowed to execute:

```console
lightpipe --backend postgresql://user:password@localhost/lightpipe worker \
  examples.scrape_and_predict:scrape_and_predict
```

For a durable split-process demo, run `docker compose up --build`, then open
`http://127.0.0.1:8000`. This starts PostgreSQL 16, runs migrations once, launches a control-only
API, and launches a separate worker. Use `docker compose down` to stop it; add `--volumes` only
when you intentionally want to delete its database.

Postgres notifications are only wake-up hints. Runnable task rows remain authoritative, so lost
notifications cannot lose work. Task outputs must be JSON-compatible; larger data should be placed
in an `ArtifactStore` and represented by an `ArtifactRef`.

Third-party adapters are exposed through the `lightpipe.backends` entry-point group. A conforming
adapter must pass the behavioral suite represented by `tests/test_backend_contract.py`.

## Delivery guarantees

Workers claim tasks with expiring leases and fencing tokens. A crashed worker's task becomes
runnable again. A stale worker cannot commit after another worker acquires the task. Therefore stage
execution is **at least once**: external side effects must use the stable run/task identity or their
own transactional idempotency key.

Caching is deliberately opt-in because cached stages are expected to be pure. Cache expiration
controls reuse; artifact retention is a separate concern.

## Current boundaries

This initial implementation targets a single trusted operator. It does not provide multi-tenancy,
RBAC, cyclic graphs, arbitrary topology mutation from stage code, or continuous record streaming.
Cron-expression parsing and artifact reference garbage collection are follow-on operational work.
Interval schedules and stateful pollers already keep
their ownership and cursor state in the selected orchestration backend.
