from __future__ import annotations

import time
from pathlib import Path

from lightpipe import pipeline, stage


@stage
def interruptible(value: int, marker: str) -> int:
    marker_path = Path(marker)
    if not marker_path.exists():
        marker_path.write_text("started")
        time.sleep(5)
    return value * 2


@pipeline
def failure_recovery(value: int, marker: str):
    return interruptible(value, marker)
