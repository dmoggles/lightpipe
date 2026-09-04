# Backend adapter contract

Backends implement `lightpipe.backends.base.OrchestrationBackend`. The interface is intentionally
expressed in orchestration operations rather than database primitives. Pipeline definitions,
workers, triggers, and the control API must not import a concrete adapter.

## Required semantics

An adapter must provide the following guarantees:

1. `(run_id, node_id, map_index)` is an idempotent task identity.
2. A runnable task can be leased by only one worker at a time.
3. Every lease has an opaque fencing token and expiry time.
4. Only the current, unexpired fencing token can start, heartbeat, complete, or fail its task.
5. Completion stores the output and corresponding event atomically.
6. Expired leases become runnable without losing their attempt history.
7. Run submission with the same pipeline name and idempotency key returns the existing run.
8. Dynamic expansion markers are idempotent. The core creates task identities before writing the
   marker, allowing a crash to cause harmless repeated creation instead of lost work.
9. Trigger leases provide the same fencing behavior as task leases and persist their cursor only on
   successful completion.
10. Cache publication is safe under concurrent writers, and expired entries are never returned.

The core deliberately reconciles downstream scheduling after task completion. Reconciliation is
idempotent and workers run it when idle, closing the crash window between committing a result and
creating its downstream tasks without requiring a distributed transaction across backend types.

## Plugin registration

Publish the adapter factory as a Python entry point whose name matches its URL scheme:

```toml
[project.entry-points."lightpipe.backends"]
hazelcast = "lightpipe_hazelcast:HazelcastBackend"
```

```python
backend = await load_backend("hazelcast://cluster.example/pipelines")
```

The constructor receives the complete URL. If it exposes an asynchronous `initialize()` method,
the loader invokes it before returning the backend.

Backend initialization must not mutate a production schema. The Postgres adapter verifies its
Alembic revision during initialization, while `lightpipe db upgrade` is the explicit schema-change
boundary.

## Capability declaration

Each adapter exposes `BackendCapabilities`:

- `durable`: state survives complete backend restarts;
- `event_subscription`: the adapter supplies an efficient event subscription implementation;
- `atomic_completion`: result state and its event are committed together;
- `max_inline_bytes`: maximum serialized task value stored directly by the backend.

The base `subscribe()` implementation reads existing events once. Durable adapters without native
subscriptions override it with safe polling; notification systems always treat a notification as
a hint and recover from the durable event cursor. The Postgres adapter currently supplies cursor
polling, while its writes also publish notifications for a future lower-latency listener.

## Conformance

Run the behavioral cases in `tests/test_backend_contract.py` against the new adapter, then add
adapter-specific tests for cluster loss and persistence. A production adapter must additionally be
fault-injected at each claim/start/heartbeat/complete boundary. Passing ordinary happy-path CRUD
tests is not sufficient for at-least-once execution.

Hazelcast implementations should normally use a fenced `IMap` task record as the source of truth
and a queue or reliable topic only as a wake-up hint. Removing an item from `IQueue` alone does not
satisfy the lease-recovery contract.
