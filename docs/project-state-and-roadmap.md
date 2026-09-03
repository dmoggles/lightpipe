# Project state and roadmap

Last updated: 2026-09-04

## Executive summary

Lightpipe now has a working operational local service and the foundation of a distributed
orchestrator. Pipeline authoring, dynamic fan-out, retries, caching, artifacts, leases, triggers,
the backend abstraction, and the service lifecycle are implemented and tested through the
in-memory backend. The package builds, the example pipeline runs from the CLI, and `lightpipe
serve` launches the API, dashboard, reconciler, trigger scheduler, and local workers together.

It is not yet production-ready. In particular, the Postgres adapter has not been exercised against
a live database, authentication is absent, and several operational and observability features
remain incomplete.

The next milestone is durable Postgres deployment. The same pipeline should run unchanged with the
API and workers split across processes, backed by a live database and exercised under concurrency
and worker-failure scenarios.

## Maturity definitions

- **Verified:** implemented and covered by automated tests or an end-to-end smoke test.
- **Implemented:** code exists, but integration or failure-mode testing is incomplete.
- **Foundation:** public model or interface exists, but the operational behavior is incomplete.
- **Planned:** no supported implementation yet.

## Current state

| Subsystem | Maturity | Current capability | Important gaps |
| --- | --- | --- | --- |
| Astral toolchain | Verified | `uv`, `uv_build`, Ruff, and `ty` are configured; the lockfile is committed-ready | CI workflow is not yet defined |
| Pipeline DSL | Verified | `@stage`, `@pipeline`, deterministic graph compilation, parameters, and typed node results | More definition validation and richer complex-output typing |
| Dynamic execution | Verified | Runtime `map`, terminal maps, `collect`, empty maps, and independent branch completion | Fan-out quotas, paging, and high-cardinality load tests |
| Local orchestration | Verified | In-memory runs, task state, reconciliation, retries, cancellation, events, and cache | State intentionally does not survive restart |
| Worker execution | Verified | Local draining, supervised Linux child-process execution, graceful service shutdown, and safe active-task release | Cross-process shutdown and fault-injection testing |
| Backend abstraction | Verified foundation | Domain-level adapter contract, capabilities, entry-point loading, and conformance tests | Must be validated with a non-relational adapter |
| Postgres backend | Implemented | Durable records, task claiming, leases, fencing, events, cache, and trigger cursors | No live Postgres integration, concurrency, migration, or fault-injection tests |
| Result caching | Verified | Opt-in cache policy, deterministic keys, TTL expiry, and cross-run reuse | Manual invalidation controls and artifact-aware garbage collection |
| Artifact storage | Verified locally | Content-addressed filesystem store and S3-compatible adapter | S3 integration tests, reference accounting, retention jobs, and pinning |
| Triggers | Verified locally | Running interval schedules and stateful pollers with cursor leases and idempotent run requests | Cron expressions, webhook registry, overlap policies, and trigger history UI |
| CLI | Verified | Local `run`, durable `worker`, `inspect`, and operational `serve` commands | Retry, trigger-management, and garbage-collection commands |
| Control API | Verified locally | Managed lifespan, pipeline/run endpoints, cancellation, event streaming, health, and worker status | Authentication, pagination, and live Postgres deployment testing |
| Dashboard | Verified locally | Launchable run submission, run list/detail, and service status | Graph, logs, attempts, richer filters, and controls |
| Observability | Foundation | Append-only orchestration events and SSE transport | Structured stage logs, metrics, traces, and OpenTelemetry export |
| Production controls | Planned | Basic lease and retry primitives exist | Priorities, quotas, backpressure, backfills, retention workers, and recovery tooling |

## Verified baseline

The following checks currently pass:

```console
uv lock --check
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv run pytest -q
uv build
```

The automated suite contains 21 passing tests covering:

- linear DAG execution;
- dynamic maps, collection, terminal maps, and empty maps;
- retry and failure behavior;
- cache reuse across runs;
- idempotent run submission;
- task lease fencing and expired-lease recovery;
- continued processing of successful items after a mapped sibling fails;
- content-addressed filesystem artifacts;
- backend task identity, exclusive claims, and cancellation;
- poller cursors and schedule idempotency.
- service startup/readiness, API submission, worker status, immediate triggers, and graceful task
  release during shutdown.

The packaged CLI also passes an end-to-end run of
`examples.scrape_and_predict:scrape_and_predict` using the in-memory backend.

## Roadmap

### Milestone 1: Operational local service — complete (2026-09-04)

Goal: make the existing API and dashboard usable through one supported command.

- Add `lightpipe serve MODULE:PIPELINE [...]` with host, port, and backend options.
- Manage backend initialization and shutdown through the FastAPI lifespan.
- Start a reconciler, in-process worker pool, and trigger scheduler for `memory://`.
- Register all supplied pipeline definitions before accepting run requests.
- Add readiness, liveness, and worker-status endpoints.
- Add tests that launch the service, submit a run, observe events, and reach a terminal state.

Acceptance criteria:

```console
uv run lightpipe serve examples.scrape_and_predict:scrape_and_predict
```

starts a usable service at `http://127.0.0.1:8000`; a run submitted from the API or dashboard
finishes without starting another process.

### Milestone 2: Durable Postgres deployment

Goal: verify the backend contract under real multi-process execution.

- Add a disposable Postgres integration environment and run the backend conformance suite against
  both memory and Postgres.
- Replace schema bootstrap as the production mechanism with versioned migrations.
- Run the API/control service and workers as separate processes.
- Test concurrent claims, duplicate completion, lease expiry, stale fencing tokens, and restart at
  every task transition boundary.
- Confirm idle reconciliation repairs completion-to-downstream crash windows.
- Document backup, restore, connection-pool, indexing, and retention requirements.

Acceptance criteria: the example pipeline runs unchanged through separate API and worker processes;
intentional worker termination may repeat a task but cannot lose it or accept a stale completion.

### Milestone 3: Monitoring and controls

Goal: make run behavior understandable and recoverable without querying storage directly.

- Display the compiled DAG, dynamically mapped instances, attempts, timings, errors, cache hits,
  and artifact metadata.
- Persist and stream stdout, stderr, and structured stage logs.
- Add cancel, rerun, and retry-failed controls to the API, CLI, and dashboard.
- Add pagination and filters for pipelines, versions, runs, states, and time ranges.
- Export correlated OpenTelemetry traces, metrics, and logs.

Acceptance criteria: an operator can identify a failed mapped item, inspect its attempts and logs,
retry it, and observe the downstream graph complete from the dashboard.

### Milestone 4: Trigger automation

Goal: operate scheduled scraping and event-driven prediction workflows without external glue.

- Run registered interval schedules and pollers inside a leased scheduler service.
- Add cron expressions with explicit time zones and daylight-saving behavior.
- Add managed webhook triggers and request authentication.
- Implement overlap policies (`skip`, `queue`, and `allow`), missed-run handling, pause/resume, and
  trigger history.
- Preserve cursor updates and run creation through idempotent recovery tests.

Acceptance criteria: a poller detects a new scrape target exactly once logically, advances its
cursor safely, and launches a monitored pipeline run after scheduler or worker restarts.

### Milestone 5: Storage and execution hardening

Goal: bound resource use and support sustained operation.

- Add artifact reference accounting, pinning, expiry, and garbage collection.
- Add independent retention policies for cache entries, run history, events, and logs.
- Add global and per-pipeline concurrency, priorities, maximum fan-out, rate limits, and
  backpressure.
- Add backfills, timeout escalation, graceful worker draining, and orphan recovery tools.
- Load-test short tasks, large maps, long-running model jobs, and mixed scraping/training workloads.

Acceptance criteria: configured storage and concurrency bounds are enforced under load, and cleanup
cannot delete artifacts referenced by retained runs or live cache entries.

### Milestone 6: Backend portability proof

Goal: demonstrate that Postgres details have not leaked into the orchestration core.

- Implement a limited Hazelcast adapter covering runs, leased tasks, events, triggers, and cache.
- Run the shared conformance suite and representative pipeline tests unchanged.
- Refine capability negotiation for backend features that cannot be implemented equivalently.
- Publish an adapter template and external-plugin test instructions.

Acceptance criteria: switching the configured backend does not require changes to pipeline,
runtime, worker, trigger, CLI, or API code.

## Recommended next step

Begin Milestone 2 with a disposable live Postgres environment and run the shared backend
conformance suite against it. Then exercise separate API and worker processes, concurrent claims,
lease expiry, fencing, and deliberate worker termination. This validates the durable architecture
before investing further in the dashboard.

## Explicit non-goals for the initial release

- Multi-tenant isolation, quotas, and RBAC.
- Arbitrary cyclic execution graphs.
- Imperative topology mutation from running stage code.
- Continuous record-stream processing.
- Exactly-once external side effects.

External writes remain the responsibility of stage code and must be idempotent because task
execution is at least once.
