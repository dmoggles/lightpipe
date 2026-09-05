"""Runnable Milestone 4 trigger example.

Start it with:

    export LIGHTPIPE_DEMO_WEBHOOK_SECRET=development-secret
    uv run lightpipe serve \
      examples.trigger_automation:notification \
      examples.trigger_automation:heartbeat \
      examples.trigger_automation:weekday_morning \
      examples.trigger_automation:incoming_notification
"""

from __future__ import annotations

from datetime import timedelta

from lightpipe import ScheduledOccurrence, WebhookEvent, pipeline, schedule, stage, webhook


@stage
def deliver(message: str, source: str) -> dict[str, str]:
    print(f"deliver from {source}: {message}")
    return {"message": message, "source": source}


@pipeline
def notification(message: str, source: str):
    return deliver(message, source)


@schedule(every=timedelta(seconds=10), name="demo_heartbeat")
def heartbeat(occurrence: ScheduledOccurrence):
    return notification(
        message=f"heartbeat due at {occurrence.scheduled_for.isoformat()}",
        source="interval",
    )


@schedule(cron="0 9 * * 1-5", timezone="Europe/London", name="weekday_morning")
def weekday_morning(occurrence: ScheduledOccurrence):
    return notification(
        message=f"weekday run due at {occurrence.scheduled_for.isoformat()}",
        source="cron",
    )


@webhook(name="incoming_notification", secret_env="LIGHTPIPE_DEMO_WEBHOOK_SECRET")
def incoming_notification(event: WebhookEvent):
    if not isinstance(event.payload, dict) or not isinstance(event.payload.get("message"), str):
        raise ValueError("payload must contain a string 'message'")
    return notification(message=event.payload["message"], source="webhook")
