# Storage and execution hardening

Milestone 5 adds opt-in bounds without changing existing pipeline behavior. A pipeline can declare
its operational policy alongside its definition:

```python
from datetime import timedelta

from lightpipe import PipelinePolicy, RateLimit, RetentionPolicy, pipeline


@pipeline(
    policy=PipelinePolicy(
        max_concurrency=8,
        max_active_runs=4,
        max_fanout=10_000,
        max_materialized_tasks=500,
        rate_limit=RateLimit(100, timedelta(minutes=1), burst=20),
        retention=RetentionPolicy(
            runs_for=timedelta(days=30),
            events_for=timedelta(days=7),
            logs_for=timedelta(days=14),
            cache_for=timedelta(days=7),
        ),
    )
)
def ingest(...): ...
```

Pending runs are accepted durably and admitted as capacity becomes available. Higher integer
priorities run first; caller priorities are capped by `max_priority`. Mapped work is materialized
incrementally, and a collection over `max_fanout` fails before that mapped node creates tasks.

## Maintenance and artifacts

`serve` runs leased retention maintenance by default. Configure physical artifact collection with
`--artifact-store file:///shared/artifacts` or `--artifact-store s3://bucket/prefix`. Use
`--no-maintenance` when a separate control replica owns maintenance. Cleanup is batched and uses a
two-pass grace period; active task execution suppresses discovery of new deletion candidates.

Useful one-shot controls include:

```console
lightpipe --backend "$DATABASE_URL" retention run --dry-run
lightpipe --backend "$DATABASE_URL" artifact gc --store s3://bucket/lightpipe --dry-run
lightpipe --backend "$DATABASE_URL" artifact pin URI --label release --expires-at 2026-12-01T00:00:00Z
lightpipe --backend "$DATABASE_URL" artifact pins
lightpipe --backend "$DATABASE_URL" artifact unpin PIN_ID
```

## Backfills and recovery

Schedule ranges are inclusive and do not move the live schedule cursor:

```console
lightpipe --backend "$DATABASE_URL" backfill schedule project:daily \
  --from 2026-08-01T00:00:00Z --to 2026-08-31T23:59:59Z
```

Direct pipeline backfills stream JSONL. Each line contains `parameters` and may include
`idempotency_key` and `priority`. `--batch-id` supplies stable per-line keys when omitted.

```console
lightpipe --backend "$DATABASE_URL" backfill pipeline project:flow \
  --input requests.jsonl --batch-id august-rebuild
```

Workers stop claiming on SIGTERM/SIGINT, drain for `--shutdown-grace`, then release unfinished work
with a new fencing boundary. Inspect or repair abandoned work with `recover stale-workers`,
`recover reap-leases`, `recover reconcile`, and the explicit `recover release-worker --force`.

Run correctness-oriented load scenarios with:

```console
PYTHONPATH=src python benchmarks/load.py --scenario mixed --size 1000
```
