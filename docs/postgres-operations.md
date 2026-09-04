# Postgres operations

Lightpipe supports PostgreSQL 16 and later. Use a dedicated database and run migrations before any
API or worker process starts.

## Deployment

```console
lightpipe --backend "$DATABASE_URL" db status
lightpipe --backend "$DATABASE_URL" db upgrade
lightpipe --backend "$DATABASE_URL" serve my_project:pipeline --workers 0
lightpipe --backend "$DATABASE_URL" worker my_project:pipeline --worker-id worker-1
```

Migrations are forward-only and serialized with a Postgres advisory lock. Application startup
checks the `alembic_version` revision and exits if it is absent or behind. Run `db upgrade` as a
one-shot deployment step before rolling out API and worker processes. The repository's
`compose.yaml` demonstrates this ordering; its credentials are development-only.

## Connections and indexes

Each backend uses a pool of 1–10 connections by default. Budget total connections as process count
times pool maximum, leaving capacity for migrations and operations. Programmatic deployments can
set `min_size` and `max_size` on `PostgresBackend`.

Task claims are indexed by state, availability, and creation time. Event lookup is indexed by run
and sequence, while run idempotency and task identity use unique indexes. Inspect query plans and
vacuum health under the production workload before changing these indexes.

## Backup and restore

Use platform snapshots or `pg_dump`, and retain WAL according to the required recovery point:

```console
pg_dump --format=custom --file=lightpipe.dump "$DATABASE_URL"
createdb lightpipe_restored
pg_restore --clean --if-exists --no-owner --dbname=lightpipe_restored lightpipe.dump
lightpipe --backend postgresql://localhost/lightpipe_restored db status
```

Test restores regularly. Stop writers or use a coordinated database snapshot when an exact
application-level recovery point is required.

## Recovery and retention

Worker execution is at least once. A killed worker retains its lease until expiry; a reconciler
returns the task to runnable state. Fencing prevents the former worker from committing after a
replacement claims the task. External stage writes must therefore be idempotent.

Run, task, attempt, stage-log, event, cache, and trigger rows are currently retained indefinitely,
except that expired
cache entries are removed on lookup. Until retention jobs arrive in Milestone 5, monitor database
growth and perform only operator-reviewed archival. Do not delete records belonging to retained
runs.
