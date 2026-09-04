import React, { useCallback, useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import "./style.css";

type State = "pending" | "running" | "runnable" | "leased" | "succeeded" | "failed" | "cancelled" | "cached" | "skipped";
type Run = { id: string; pipeline_name: string; definition_hash: string; state: State; created_at: string; updated_at: string; rerun_of?: string };
type Attempt = { id: string; attempt: number; worker_id: string; state: string; leased_at: string; started_at?: string; finished_at?: string; error?: string; cache_hit: boolean };
type Task = { id: string; node_id: string; map_index: number | null; state: State; attempt: number; error?: string; created_at: string; updated_at: string; attempts: Attempt[]; artifacts: Artifact[] };
type NodeSpec = { id: string; stage: string; dependencies: string[]; mapped: boolean; retry: { attempts: number }; timeout?: number; cache?: { ttl_seconds: number } };
type Graph = { name: string; definition_hash: string; parameters: string[]; nodes: NodeSpec[] };
type Artifact = { path: string; $artifact: string; media_type?: string; digest?: string; size?: number };
type Detail = { run: Run & { parameters: unknown; output: unknown }; graph: Graph | null; graph_available: boolean; tasks: Task[]; artifacts: Artifact[] };
type Log = { id: string; occurred_at: string; attempt: number; stream: string; level: string; logger?: string; message: string; fields: Record<string, unknown> };
type Page<T> = { items: T[]; next_cursor: string | null };

async function api<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, options);
  const body = await response.json();
  if (!response.ok) throw new Error(body.detail || response.statusText);
  return body as T;
}

const fmt = (value?: string) => value ? new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "medium" }).format(new Date(value)) : "—";
const duration = (start?: string, finish?: string) => start ? `${Math.max(0, ((finish ? new Date(finish) : new Date()).getTime() - new Date(start).getTime()) / 1000).toFixed(2)}s` : "—";

function StateBadge({ state }: { state: string }) {
  return <span className={`badge state-${state}`}><span aria-hidden="true" className="dot" />{state}</span>;
}

function GraphView({ graph, tasks, selected, onSelect }: { graph: Graph; tasks: Task[]; selected?: string; onSelect: (task: Task) => void }) {
  const layout = useMemo(() => {
    const levels = new Map<string, number>();
    const byId = new Map(graph.nodes.map(node => [node.id, node]));
    const level = (id: string): number => {
      if (levels.has(id)) return levels.get(id)!;
      const node = byId.get(id)!;
      const value = node.dependencies.length ? Math.max(...node.dependencies.map(level)) + 1 : 0;
      levels.set(id, value); return value;
    };
    graph.nodes.forEach(node => level(node.id));
    const rows = new Map<number, number>();
    return graph.nodes.map(node => {
      const x = level(node.id) * 270 + 30;
      const row = rows.get(level(node.id)) || 0; rows.set(level(node.id), row + 1);
      return { node, x, y: row * 170 + 30 };
    });
  }, [graph]);
  const positions = new Map(layout.map(item => [item.node.id, item]));
  const width = Math.max(600, ...layout.map(item => item.x + 230));
  const height = Math.max(230, ...layout.map(item => item.y + 140));
  return <div className="graph-scroll" role="region" aria-label="Pipeline graph" tabIndex={0}>
    <svg className="graph" viewBox={`0 0 ${width} ${height}`} style={{ minWidth: width, height }}>
      <defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 z" /></marker></defs>
      {layout.flatMap(({ node, x, y }) => node.dependencies.map(dep => {
        const source = positions.get(dep)!;
        return <path className="edge" key={`${dep}-${node.id}`} d={`M${source.x + 210},${source.y + 50} C${source.x + 240},${source.y + 50} ${x - 30},${y + 50} ${x},${y + 50}`} />;
      }))}
      {layout.map(({ node, x, y }) => {
        const instances = tasks.filter(task => task.node_id === node.id);
        const state = instances.some(t => t.state === "failed") ? "failed" : instances.some(t => ["running", "leased"].includes(t.state)) ? "running" : instances.length && instances.every(t => ["succeeded", "cached"].includes(t.state)) ? "succeeded" : instances[0]?.state || "pending";
        return <g key={node.id} transform={`translate(${x} ${y})`} className="graph-node">
          <rect width="210" height="108" rx="12" className={`node-box node-${state}`} />
          <text x="16" y="28" className="node-title">{node.stage}</text>
          <text x="16" y="49" className="node-id">{node.id}</text>
          <text x="16" y="70" className="node-meta">{node.mapped ? `mapped · ${instances.length} items` : state}</text>
          {instances.slice(0, 7).map((task, index) => <circle key={task.id} tabIndex={0} role="button" aria-label={`Select ${task.id}, ${task.state}`} onClick={() => onSelect(task)} onKeyDown={event => event.key === "Enter" && onSelect(task)} cx={18 + index * 25} cy="91" r="7" className={`instance state-fill-${task.state} ${selected === task.id ? "selected" : ""}`} />)}
          {instances.length > 7 && <text x="192" y="96" textAnchor="end" className="node-meta">+{instances.length - 7}</text>}
        </g>;
      })}
    </svg>
  </div>;
}

function App() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [next, setNext] = useState<string | null>(null);
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [page, setPage] = useState(0);
  const [pipeline, setPipeline] = useState("");
  const [definition, setDefinition] = useState("");
  const [state, setState] = useState("");
  const [createdAfter, setCreatedAfter] = useState("");
  const [createdBefore, setCreatedBefore] = useState("");
  const [selectedRun, setSelectedRun] = useState<string | null>(null);
  const [detail, setDetail] = useState<Detail | null>(null);
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);
  const [logs, setLogs] = useState<Log[]>([]);
  const [tab, setTab] = useState("graph");
  const [error, setError] = useState("");
  const [pipelines, setPipelines] = useState<{name: string; definition_hash: string; parameters: string[]}[]>([]);
  const [newPipeline, setNewPipeline] = useState("");
  const [params, setParams] = useState("{}");
  const [streamVersion, setStreamVersion] = useState(0);
  const [service, setService] = useState<{started: boolean; workers: {state: string}[]; triggers: unknown[]} | null>(null);

  const loadRuns = useCallback(async (cursor: string | null = cursors[page]) => {
    const query = new URLSearchParams({ limit: "25" });
    if (cursor) query.set("cursor", cursor);
    if (pipeline) query.set("pipeline", pipeline);
    if (definition) query.set("definition_hash", definition);
    if (state) query.set("state", state);
    if (createdAfter) query.set("created_after", new Date(createdAfter).toISOString());
    if (createdBefore) query.set("created_before", new Date(createdBefore).toISOString());
    const result = await api<Page<Run>>(`/api/v1/runs?${query}`);
    setRuns(result.items); setNext(result.next_cursor);
  }, [cursors, page, pipeline, definition, state, createdAfter, createdBefore]);

  const loadDetail = useCallback(async (id: string) => {
    const value = await api<Detail>(`/api/v1/runs/${id}`);
    setDetail(value);
    setSelectedTask(current => value.tasks.find(task => task.id === current?.id) || value.tasks[0] || null);
  }, []);

  useEffect(() => { api<{name: string; definition_hash: string; parameters: string[]}[]>("/api/pipelines").then(value => { setPipelines(value); setNewPipeline(value[0]?.name || ""); }).catch(e => setError(e.message)); }, []);
  useEffect(() => { loadRuns().catch(e => setError(e.message)); }, [loadRuns]);
  useEffect(() => {
    const refresh = () => { loadRuns().catch(e => setError(e.message)); api<{started: boolean; workers: {state: string}[]; triggers: unknown[]}>("/api/workers").then(setService).catch(e => setError(e.message)); };
    const timer = window.setInterval(refresh, 5000); refresh();
    return () => window.clearInterval(timer);
  }, [loadRuns]);
  useEffect(() => {
    if (!selectedRun) return;
    loadDetail(selectedRun).catch(e => setError(e.message));
    const stream = new EventSource(`/api/v1/runs/${selectedRun}/events`);
    stream.onmessage = event => {
      const value = JSON.parse(event.data);
      loadDetail(selectedRun); loadRuns();
      if (["run.succeeded", "run.failed", "run.cancelled"].includes(value.kind)) stream.close();
    };
    return () => stream.close();
  }, [selectedRun, loadDetail, loadRuns, streamVersion]);
  useEffect(() => {
    if (!selectedTask) { setLogs([]); return; }
    const load = () => api<Page<Log>>(`/api/v1/tasks/${selectedTask.id}/logs?limit=1000`).then(value => setLogs(value.items));
    load().catch(e => setError(e.message));
    const stream = new EventSource(`/api/v1/tasks/${selectedTask.id}/logs/stream`);
    stream.onmessage = event => setLogs(current => [...current, JSON.parse(event.data)]);
    return () => stream.close();
  }, [selectedTask?.id]);

  async function control(action: "cancel" | "rerun" | "retry-failed", taskIds?: string[]) {
    if (!selectedRun) return;
    if (!window.confirm(`${action.replace("-", " ")} this run?`)) return;
    try {
      const body = action === "retry-failed" ? JSON.stringify({ task_ids: taskIds || [] }) : "{}";
      const result = await api<Run>(`/api/v1/runs/${selectedRun}/${action}`, { method: "POST", headers: { "Content-Type": "application/json" }, body });
      if (action === "rerun") setSelectedRun(result.id);
      else { setStreamVersion(value => value + 1); await loadDetail(selectedRun); }
      await loadRuns();
    } catch (e) { setError((e as Error).message); }
  }

  async function submit() {
    try {
      const target = newPipeline;
      if (!target) return;
      const run = await api<Run>(`/api/pipelines/${encodeURIComponent(target)}/runs`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ parameters: JSON.parse(params) }) });
      setSelectedRun(run.id); await loadRuns();
    } catch (e) { setError((e as Error).message); }
  }

  const allArtifacts = [...(detail?.artifacts || []), ...(detail?.tasks.flatMap(task => task.artifacts) || [])];
  return <div className="shell">
    <aside>
      <div className="brand"><span className="mark">L</span><div><strong>Lightpipe</strong><small>Operations</small></div></div>
      <nav aria-label="Primary"><a className="active">Runs</a><a href="/docs">API docs</a></nav>
      <div className="service"><span className={service?.started ? "pulse" : "pulse offline"} /> {service ? `${service.workers.length} workers · ${service.triggers.length} triggers` : "Connecting…"}</div>
    </aside>
    <main>
      <header><div><p className="eyebrow">ORCHESTRATION</p><h1>Pipeline runs</h1></div><button className="primary" onClick={submit}>New run</button></header>
      {error && <div className="alert" role="alert"><span>{error}</span><button onClick={() => setError("")}>Dismiss</button></div>}
      <section className="toolbar" aria-label="Run filters">
        <label>Pipeline<input value={pipeline} onChange={e => { setPipeline(e.target.value); setPage(0); setCursors([null]); }} placeholder="All pipelines" /></label>
        <label>Version<input value={definition} onChange={e => { setDefinition(e.target.value); setPage(0); setCursors([null]); }} placeholder="Definition hash" /></label>
        <label>State<select value={state} onChange={e => { setState(e.target.value); setPage(0); setCursors([null]); }}><option value="">All states</option>{["pending","running","succeeded","failed","cancelled"].map(value => <option key={value}>{value}</option>)}</select></label>
        <label>After<input type="datetime-local" value={createdAfter} onChange={e => { setCreatedAfter(e.target.value); setPage(0); setCursors([null]); }} /></label>
        <label>Before<input type="datetime-local" value={createdBefore} onChange={e => { setCreatedBefore(e.target.value); setPage(0); setCursors([null]); }} /></label>
        <label className="parameters">New run<select value={newPipeline} onChange={e => setNewPipeline(e.target.value)}>{pipelines.map(value => <option key={value.definition_hash} value={value.name}>{value.name}</option>)}</select><textarea value={params} onChange={e => setParams(e.target.value)} aria-label="New run JSON parameters" /></label>
      </section>
      <section className="panel runs-panel">
        <table><thead><tr><th>Pipeline</th><th>Run</th><th>State</th><th>Created</th><th>Duration</th></tr></thead>
          <tbody>{runs.map(run => <tr key={run.id} className={selectedRun === run.id ? "active-row" : ""} onClick={() => setSelectedRun(run.id)}><td><strong>{run.pipeline_name}</strong><small>{run.definition_hash.slice(0, 8)}</small></td><td><button className="link">{run.id}</button></td><td><StateBadge state={run.state} /></td><td>{fmt(run.created_at)}</td><td>{duration(run.created_at, run.updated_at)}</td></tr>)}</tbody></table>
        {!runs.length && <div className="empty">No runs match these filters.</div>}
        <div className="pager"><button disabled={page === 0} onClick={() => setPage(value => value - 1)}>Previous</button><span>Page {page + 1}</span><button disabled={!next} onClick={() => { setCursors(value => [...value.slice(0, page + 1), next]); setPage(value => value + 1); }}>Next</button></div>
      </section>
      {detail && <section className="workspace">
        <div className="run-heading"><div><p className="eyebrow">{detail.run.pipeline_name}</p><h2>{detail.run.id}</h2><p className="muted">Definition {detail.run.definition_hash.slice(0, 12)} · created {fmt(detail.run.created_at)}</p></div><StateBadge state={detail.run.state} /><div className="actions"><button disabled={!detail.tasks.some(t => t.state === "failed")} onClick={() => control("retry-failed")}>Retry failed</button><button onClick={() => control("rerun")}>Rerun</button><button className="danger" disabled={!(["pending","running"] as string[]).includes(detail.run.state)} onClick={() => control("cancel")}>Cancel</button></div></div>
        <div className="tabs" role="tablist">{["graph","attempts","logs","artifacts"].map(value => <button role="tab" aria-selected={tab === value} className={tab === value ? "selected-tab" : ""} onClick={() => setTab(value)} key={value}>{value}</button>)}</div>
        {tab === "graph" && (detail.graph ? <GraphView graph={detail.graph} tasks={detail.tasks} selected={selectedTask?.id} onSelect={setSelectedTask} /> : <div className="empty">Graph metadata is unavailable for this historical definition.</div>)}
        {tab === "attempts" && <div className="split"><div className="task-list">{detail.tasks.map(task => <button key={task.id} className={selectedTask?.id === task.id ? "selected-task" : ""} onClick={() => setSelectedTask(task)}><span>{task.node_id}{task.map_index !== null ? ` [${task.map_index}]` : ""}</span><StateBadge state={task.state} /></button>)}</div><div className="attempts">{selectedTask?.attempts.map(attempt => <article key={attempt.id}><div><strong>Attempt {attempt.attempt}</strong><StateBadge state={attempt.state} /></div><dl><dt>Worker</dt><dd>{attempt.worker_id}</dd><dt>Started</dt><dd>{fmt(attempt.started_at || attempt.leased_at)}</dd><dt>Duration</dt><dd>{duration(attempt.started_at || attempt.leased_at, attempt.finished_at)}</dd></dl>{attempt.error && <pre className="error-text">{attempt.error}</pre>}</article>)}</div></div>}
        {tab === "logs" && <div className="logs"><div className="log-head"><strong>{selectedTask ? `${selectedTask.node_id}${selectedTask.map_index !== null ? ` [${selectedTask.map_index}]` : ""}` : "Select a task"}</strong><span>{logs.length} records</span></div>{logs.map(log => <div className={`log-line log-${log.level}`} key={log.id}><time>{new Date(log.occurred_at).toLocaleTimeString()}</time><span>{log.stream}</span><code>{log.message}</code>{Object.keys(log.fields).length > 0 && <pre>{JSON.stringify(log.fields)}</pre>}</div>)}</div>}
        {tab === "artifacts" && <div className="artifact-grid">{allArtifacts.map((artifact, index) => <article key={`${artifact.$artifact}-${index}`}><strong>{artifact.media_type || "artifact"}</strong><code>{artifact.$artifact}</code><dl><dt>Location</dt><dd>{artifact.path}</dd><dt>Size</dt><dd>{artifact.size ?? "—"}</dd><dt>Digest</dt><dd>{artifact.digest || "—"}</dd></dl></article>)}{!allArtifacts.length && <div className="empty">No artifact references in this run.</div>}</div>}
      </section>}
    </main>
  </div>;
}

createRoot(document.getElementById("root")!).render(<React.StrictMode><App /></React.StrictMode>);
