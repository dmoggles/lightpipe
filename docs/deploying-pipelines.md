# Deploying pipelines with workers

This guide deploys Lightpipe with a control service and one or more separate workers sharing a
Postgres backend. The control service accepts run requests, reconciles graphs, and operates
triggers. Workers claim and execute runnable stages.

## 1. Package the pipeline

Put pipeline definitions and every dependency used by their stages in the same application package
or container image as Lightpipe. A definition is referenced as `module:object`:

```python
# my_project/pipelines.py
from lightpipe import pipeline, stage


@stage(retries=2)
def transform(value: int) -> int:
    return value * 2


@pipeline
def derive(value: int):
    return transform(value)
```

The deployable reference is `my_project.pipelines:derive`. Verify that reference before deployment:

```console
lightpipe run my_project.pipelines:derive --parameters '{"value":21}'
```

The API and workers must run the same application revision and register the same definition. Each
compiled graph has a deterministic definition hash; a worker cannot execute a persisted run whose
hash it has not registered.

Stage inputs and outputs must be JSON-compatible. Store large values in a shared `ArtifactStore`
and pass `ArtifactRef` values through the graph. A local filesystem artifact store is unsuitable
unless the same path is mounted at every worker; use shared object storage for multi-host workers.

## 2. Prepare Postgres

Install the downstream project and Lightpipe integrations into the same Python environment, then
set the backend URL through the deployment's secret manager:

```console
pip install "lightpipe[api,postgres]" my-project
export DATABASE_URL='postgresql://lightpipe:password@database.example/lightpipe'
```

Installing Lightpipe creates the `lightpipe` executable in the environment's `bin` directory.
Neither the server nor workers require `uv`; `python -m lightpipe` is an equivalent fallback when
an environment does not place console scripts on `PATH`. A downstream project may use `uv`, pip,
or another standards-compatible installer to build that environment.

Use a dedicated database. Apply migrations once as a deployment job, before starting application
processes:

```console
lightpipe --backend "$DATABASE_URL" db status
lightpipe --backend "$DATABASE_URL" db upgrade
lightpipe --backend "$DATABASE_URL" db status
```

The final status must report `"ready": true`. API and worker startup validates the revision but
never changes the schema. See [Postgres operations](postgres-operations.md) for backup, restore,
pool sizing, and retention guidance.

## 3. Start the control service

Run the API without embedded workers:

```console
lightpipe --backend "$DATABASE_URL" serve \
  my_project.pipelines:derive \
  --workers 0 \
  --host 0.0.0.0 \
  --port 8000
```

Add any `module:object` poller and schedule definitions to the same command. The control process
runs their leased scheduler loops as well as graph reconciliation.

For production, add `--no-scheduler` to the API command and run the same pipeline and trigger
definitions through `lightpipe scheduler` in one or more independent replicas. Postgres trigger
leases fence competing replicas; see [Trigger automation](trigger-automation.md).

Route traffic only after `GET /health/ready` returns HTTP 200. `GET /health/live` confirms the
process is alive. The dashboard is served at `/`; in the current release `/api/workers` reports
only workers embedded in that control process, so it is empty in control-only mode.

## 4. Start and scale workers

Start a worker with every pipeline definition it is permitted to execute:

```console
lightpipe --backend "$DATABASE_URL" worker \
  my_project.pipelines:derive \
  --worker-id derive-worker-1 \
  --poll-interval 1 \
  --lease-seconds 300
```

One worker process executes one stage at a time. Scale concurrency by running more processes with
unique `--worker-id` values. All workers may register the same pipelines: Postgres uses row locking
and fencing tokens so only one active lease owns a task.

Workers can be specialized by supplying different definition lists, but at least one live worker
must register every definition accepted by the API. A worker presented with a run whose definition
hash is unknown cannot execute it.

Choose a lease longer than normal pauses in the worker host. Supervised stage subprocesses renew
their lease automatically. After an ungraceful worker loss, reassignment begins only after the
lease expires; shorter leases recover faster but generate more heartbeats and tolerate shorter
infrastructure pauses.

## 5. Submit and inspect a run

Submit through the dashboard or API:

```console
curl --fail --silent \
  -H 'Content-Type: application/json' \
  -d '{"parameters":{"value":21},"idempotency_key":"example-21"}' \
  http://127.0.0.1:8000/api/pipelines/derive/runs
```

Use the returned run ID with `GET /api/v1/runs/{run_id}`,
`GET /api/v1/runs/{run_id}/events`, or:

```console
lightpipe --backend "$DATABASE_URL" inspect RUN_ID
```

Use `lightpipe retry-failed RUN_ID`, `lightpipe rerun RUN_ID`, or the matching dashboard controls
for recovery. A failed-task retry reopens the original run and preserves successful mapped items;
a rerun creates a new run linked to the original.

An idempotency key is optional but recommended for externally retried submissions. Reusing the
same key for the same pipeline returns the original run.

## Docker Compose example

The repository includes a complete development deployment:

```console
docker compose up --build
```

It starts PostgreSQL 16, runs the one-shot migration service, then starts a control-only API and a
separate worker using `examples.scrape_and_predict:scrape_and_predict`. Open
`http://127.0.0.1:8000` and submit `{"target":"example"}`.

To deploy another pipeline, copy the Compose file, build an image containing that project, and
replace the definition reference in both the `api` and `worker` commands. Production credentials
must come from secrets rather than the development values in `compose.yaml`.

Stop the example without deleting its database:

```console
docker compose down
```

Use `docker compose down --volumes` only when intentionally discarding all persisted runs.

## Safe upgrades and shutdown

For code-only changes that do not alter a pipeline's definition hash, roll workers normally. When
a pipeline definition changes, use this sequence:

1. Stop accepting new runs for the old definition.
2. Keep old API/worker processes available until existing runs reach terminal states.
3. Run any required Lightpipe database migration as a separate deployment job.
4. Start workers and the API from the new application image.
5. Verify readiness and complete a smoke-test run before removing the old deployment.

If old runs must overlap the rollout, keep workers with the old code registered until those runs
finish. Do not assume a worker with the new definition can execute old persisted runs.

SIGTERM asks a standalone worker to stop claiming work and finish its current stage before closing.
If the platform sends SIGKILL after its shutdown deadline, the task is recovered after lease
expiry. Task delivery is at least once, so stage code that writes to external systems must use an
idempotency key, an upsert, or another transactional deduplication mechanism.

## Deployment checklist

- The API and workers use the same image and pipeline references.
- Postgres `db status` reports `ready: true`.
- Every accepted pipeline has at least one worker pool.
- Worker IDs are unique and connection-pool totals fit the database limit.
- Artifact storage is reachable from every worker.
- Readiness is checked before traffic is enabled.
- External stage side effects are idempotent.
- Backup, restore, retention, and worker-loss procedures have been tested.
