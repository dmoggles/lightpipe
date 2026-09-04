from __future__ import annotations

import dataclasses
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Any

from lightpipe.runtime import Runtime
from lightpipe.service import ServiceSupervisor, TriggerDefinition


def _json_default(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if isinstance(value, (datetime, Enum)):
        return value.isoformat() if isinstance(value, datetime) else value.value
    raise TypeError(type(value).__name__)


def create_app(
    runtime: Runtime,
    pipelines: dict[str, Any],
    *,
    supervisor: ServiceSupervisor | None = None,
    triggers: tuple[TriggerDefinition, ...] = (),
    worker_count: int = 1,
    process_isolation: bool = True,
    owns_backend: bool = False,
) -> Any:
    try:
        from fastapi import FastAPI, HTTPException
        from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
    except ImportError as error:
        raise RuntimeError("install lightpipe[api] to create the control API") from error

    service = supervisor or ServiceSupervisor(
        runtime.backend,
        pipelines,
        triggers=triggers,
        worker_count=worker_count,
        process_isolation=process_isolation,
        owns_backend=owns_backend,
    )
    if supervisor is not None and supervisor.backend is not runtime.backend:
        raise ValueError("supervisor and API runtime must share a backend")
    runtime = service.runtime

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await service.start()
        try:
            yield
        finally:
            await service.stop()

    app = FastAPI(title="lightpipe", version="0.2.0", lifespan=lifespan)
    app.state.lightpipe_service = service

    @app.get("/health/live")
    async def liveness() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/health/ready")
    async def readiness() -> Any:
        ready = await service.ready()
        body = {"status": "ready" if ready else "not_ready"}
        return body if ready else JSONResponse(body, status_code=503)

    @app.get("/api/workers")
    async def worker_status() -> dict[str, Any]:
        return service.snapshot()

    @app.get("/api/pipelines")
    async def list_pipelines() -> list[dict[str, Any]]:
        return [
            {
                "name": name,
                "definition_hash": item.compile().definition_hash,
                "parameters": item.compile().parameters,
            }
            for name, item in pipelines.items()
        ]

    @app.post("/api/pipelines/{name}/runs", status_code=202)
    async def create_run(name: str, body: dict[str, Any]) -> Any:
        if name not in pipelines:
            raise HTTPException(404, f"unknown pipeline {name}")
        parameters = body.get("parameters", {})
        if not isinstance(parameters, dict):
            raise HTTPException(422, "parameters must be a JSON object")
        try:
            invocation = pipelines[name](**parameters)
            return await runtime.submit(invocation, idempotency_key=body.get("idempotency_key"))
        except (TypeError, ValueError) as error:
            raise HTTPException(422, str(error)) from error

    @app.get("/api/runs")
    async def list_runs(limit: int = 100) -> Any:
        if limit < 1 or limit > 1000:
            raise HTTPException(422, "limit must be between 1 and 1000")
        return await runtime.backend.list_runs(limit=limit)

    @app.get("/api/runs/{run_id}")
    async def get_run(run_id: str) -> Any:
        try:
            run = await runtime.backend.get_run(run_id)
        except KeyError as error:
            raise HTTPException(404, f"unknown run {run_id}") from error
        return {"run": run, "tasks": await runtime.backend.tasks_for_run(run_id)}

    @app.post("/api/runs/{run_id}/cancel", status_code=202)
    async def cancel_run(run_id: str) -> dict[str, str]:
        try:
            await runtime.backend.cancel_run(run_id)
        except KeyError as error:
            raise HTTPException(404, f"unknown run {run_id}") from error
        return {"status": "cancelled"}

    @app.get("/api/runs/{run_id}/events")
    async def stream_events(run_id: str) -> StreamingResponse:
        try:
            await runtime.backend.get_run(run_id)
        except KeyError as error:
            raise HTTPException(404, f"unknown run {run_id}") from error

        async def generate() -> AsyncIterator[str]:
            async for event in runtime.backend.subscribe(run_id):
                yield f"data: {json.dumps(event, default=_json_default)}\n\n"

        return StreamingResponse(generate(), media_type="text/event-stream")

    @app.get("/", response_class=HTMLResponse)
    async def dashboard() -> str:
        return _DASHBOARD

    return app


_DASHBOARD = """<!doctype html>
<html lang="en">
<head>
  <title>lightpipe</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    :root { color-scheme: light dark; font-family: system-ui, sans-serif; }
    body { margin: 0; background: #10151d; color: #e7edf5; }
    main { max-width: 1100px; margin: auto; padding: 2rem; }
    h1 { color: #8bd5ca; } h2 { margin-top: 2rem; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
    .card { background: #18202b; border: 1px solid #344052; border-radius: 8px; padding: 1rem; }
    input, select, textarea, button { box-sizing: border-box; padding: .6rem; margin: .25rem 0; }
    textarea { width: 100%; min-height: 6rem; font-family: monospace; }
    button { cursor: pointer; background: #8bd5ca; color: #10151d; border: 0; border-radius: 4px; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: .5rem; border-bottom: 1px solid #344052; }
    a { color: #91d7e3; cursor: pointer; } pre { white-space: pre-wrap; overflow-wrap: anywhere; }
    .error, .failed { color: #ed8796; } .succeeded, .cached { color: #a6da95; }
    @media (max-width: 760px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body><main>
  <h1>lightpipe</h1>
  <div class="grid">
    <section class="card">
      <h2>Start a run</h2>
      <select id="pipeline"></select>
      <textarea id="parameters">{}</textarea>
      <button id="submit">Run pipeline</button>
      <div id="message"></div>
    </section>
    <section class="card"><h2>Service</h2><pre id="service">Loading…</pre></section>
  </div>
  <section class="card"><h2>Runs</h2><div id="runs">Loading…</div></section>
  <section class="card"><h2>Run detail</h2><pre id="detail">Select a run.</pre></section>
</main>
<script>
const replacements = {'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'};
const esc = value => String(value).replace(/[&<>"']/g, c => replacements[c]);
async function json(url, options) {
  const response = await fetch(url, options); const body = await response.json();
  if (!response.ok) throw new Error(body.detail || response.statusText); return body;
}
async function loadPipelines() {
  const pipelines = await json('/api/pipelines');
  const options = pipelines.map(p => `<option>${esc(p.name)}</option>`).join('');
  document.querySelector('#pipeline').innerHTML = options;
}
async function loadService() {
  const status = await json('/api/workers');
  document.querySelector('#service').textContent = JSON.stringify(status, null, 2);
}
async function loadRuns() {
  const runs = await json('/api/runs');
  const rows = runs.map(r => `<tr><td>${esc(r.pipeline_name)}</td>` +
    `<td><a data-run="${esc(r.id)}">${esc(r.id)}</a></td>` +
    `<td class="${esc(r.state)}">${esc(r.state)}</td></tr>`).join('');
  const header = '<table><tr><th>Pipeline</th><th>Run</th><th>State</th></tr>';
  document.querySelector('#runs').innerHTML = header + rows + '</table>';
  document.querySelectorAll('[data-run]').forEach(a => a.onclick = () => loadRun(a.dataset.run));
}
async function loadRun(id) {
  const detail = await json(`/api/runs/${id}`);
  document.querySelector('#detail').textContent = JSON.stringify(detail, null, 2);
}
document.querySelector('#submit').onclick = async () => {
  const message = document.querySelector('#message');
  try {
    const parameters = JSON.parse(document.querySelector('#parameters').value);
    const name = document.querySelector('#pipeline').value;
    const options = {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({parameters})
    };
    const run = await json(`/api/pipelines/${encodeURIComponent(name)}/runs`, options);
    message.className = ''; message.textContent = `Started ${run.id}`;
    await loadRuns(); await loadRun(run.id);
  } catch (error) { message.className = 'error'; message.textContent = error.message; }
};
async function refresh() {
  try { await Promise.all([loadRuns(), loadService()]); } catch (error) { console.error(error); }
}
loadPipelines(); refresh(); setInterval(refresh, 1000);
</script></body></html>"""
