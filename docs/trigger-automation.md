# Trigger automation

Lightpipe supports interval pollers, interval and cron schedules, and authenticated webhooks. Code
defines what a trigger launches; the backend stores its enabled state, cursor, due times, leases,
and occurrence history.

## Runnable example

The repository includes `examples.trigger_automation` with a ten-second interval, a weekday cron
schedule, and a signed webhook. Start all three with one local worker:

```bash
export LIGHTPIPE_DEMO_WEBHOOK_SECRET=development-secret
uv run lightpipe serve \
  examples.trigger_automation:notification \
  examples.trigger_automation:heartbeat \
  examples.trigger_automation:weekday_morning \
  examples.trigger_automation:incoming_notification
```

Open `http://127.0.0.1:8000` to inspect occurrences and linked runs. Send a webhook from another
terminal:

```bash
body='{"message":"hello from webhook"}'
timestamp=$(date +%s)
delivery="test-$timestamp"
signature=$(printf '%s.%s.%s' "$timestamp" "$delivery" "$body" \
  | openssl dgst -sha256 -hmac "$LIGHTPIPE_DEMO_WEBHOOK_SECRET" -hex \
  | sed 's/^.* /sha256=/')

curl -i http://127.0.0.1:8000/api/v1/webhooks/incoming_notification \
  -H 'Content-Type: application/json' \
  -H "X-Lightpipe-Timestamp: $timestamp" \
  -H "X-Lightpipe-Delivery: $delivery" \
  -H "X-Lightpipe-Signature: $signature" \
  --data-binary "$body"
```

## Schedules and policies

Cron schedules use standard five-field expressions and require an IANA timezone:

```python
from lightpipe import MissedRunPolicy, OverlapPolicy, schedule


@schedule(
    cron="0 9 * * 1-5",
    timezone="Europe/London",
    missed=MissedRunPolicy.COALESCE,
    overlap=OverlapPolicy.SKIP,
)
def weekday_prediction(occurrence):
    return prediction_pipeline(as_of=occurrence.scheduled_for.isoformat())
```

Nonexistent local times during a spring-forward transition are skipped. A repeated local minute
during a fall-back transition runs once. `coalesce` launches the latest missed occurrence,
`catch_up` launches chronologically up to `catch_up_limit`, and `skip` waits for the next future
occurrence. Overlap policies are `skip`, `queue` (one deferred occurrence), and `allow`.

## Production scheduler

Keep API and trigger execution independently scalable by disabling the embedded scheduler and
running one or more scheduler replicas against the same Postgres backend:

```bash
lightpipe --backend "$DATABASE_URL" serve \
  my_project:pipeline my_project:daily my_project:incoming --no-scheduler --workers 0

lightpipe --backend "$DATABASE_URL" scheduler \
  my_project:pipeline my_project:daily my_project:incoming
```

Scheduler replicas coordinate through expiring fenced leases. Trigger callbacks are heartbeated;
after a crash, the next scheduler repeats the same occurrence and idempotency key. This provides
exactly-once logical run creation, but stage side effects must still be idempotent.

## Webhooks

```python
from lightpipe import WebhookEvent, webhook


@webhook(secret_env="PREDICTION_WEBHOOK_SECRET")
def incoming(event: WebhookEvent):
    return prediction_pipeline(**event.payload)
```

Send JSON to `POST /api/v1/webhooks/{name}` with:

- `X-Lightpipe-Timestamp`: current Unix timestamp in seconds.
- `X-Lightpipe-Delivery`: a unique delivery identifier.
- `X-Lightpipe-Signature`: `sha256=<hex HMAC>`.

The signed bytes are `<timestamp>.<delivery>.<raw request body>`. Lightpipe rejects signatures
outside a five-minute window, duplicate delivery IDs are idempotent, and bodies are limited to
1 MiB. Secrets are read from the trigger's environment variable and are never stored or returned.

## Operations

Use `lightpipe trigger list`, `show`, `history`, `pause`, and `resume`, or the equivalent
`/api/v1/triggers` endpoints. The dashboard shows current configuration, next and previous due
times, lease ownership, occurrence history, and linked runs. Trigger history also streams from
`GET /api/v1/triggers/{name}/events`.
