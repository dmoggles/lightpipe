"""Correctness-oriented Milestone 5 load scenarios.

Run with: PYTHONPATH=src python benchmarks/load.py --scenario mixed
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time

from lightpipe import MemoryBackend, PipelinePolicy, Runtime, pipeline, stage


@stage
def short(value: int) -> int:
    return value + 1


@stage
async def long(value: int) -> int:
    await asyncio.sleep(0.02)
    return value * 2


@pipeline(policy=PipelinePolicy(max_concurrency=8, max_materialized_tasks=64))
def short_flow(values: list[int]):
    return short.map(values)


@pipeline(policy=PipelinePolicy(max_concurrency=4, max_materialized_tasks=32))
def long_flow(values: list[int]):
    return long.map(values)


async def run(scenario: str, size: int) -> dict[str, object]:
    backend = MemoryBackend()
    runtime = Runtime(backend)
    invocations = []
    if scenario in {"short", "large-map", "mixed"}:
        invocations.append(short_flow(list(range(size))))
    if scenario in {"long", "mixed"}:
        invocations.append(long_flow(list(range(max(1, size // 10)))))
    started = time.monotonic()
    runs = [await runtime.submit(invocation) for invocation in invocations]
    workers = [asyncio.create_task(runtime.run_until_complete(run.id)) for run in runs]
    completed = await asyncio.gather(*workers)
    elapsed = time.monotonic() - started
    if any(run.state.value != "succeeded" for run in completed):
        raise RuntimeError("a load scenario did not complete successfully")
    task_count = 0
    for completed_run in completed:
        task_count += len(await backend.tasks_for_run(completed_run.id))
    return {
        "scenario": scenario,
        "items": task_count,
        "runs": len(completed),
        "elapsed_seconds": elapsed,
        "tasks_per_second": task_count / elapsed,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario", choices=("short", "large-map", "long", "mixed"), default="mixed"
    )
    parser.add_argument("--size", type=int, default=1000)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.scenario, args.size)), indent=2))


if __name__ == "__main__":
    main()
