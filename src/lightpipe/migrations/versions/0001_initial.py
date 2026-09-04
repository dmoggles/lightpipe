"""Create the initial Lightpipe orchestration schema.

Revision ID: 0001
Revises: None
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


SCHEMA = """
CREATE TABLE IF NOT EXISTS lp_runs (
  id text PRIMARY KEY,
  pipeline_name text NOT NULL,
  definition_hash text NOT NULL,
  parameters jsonb NOT NULL,
  state text NOT NULL,
  output jsonb,
  idempotency_key text,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS lp_run_idempotency
  ON lp_runs (pipeline_name, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS lp_tasks (
  id text PRIMARY KEY,
  run_id text NOT NULL REFERENCES lp_runs(id) ON DELETE CASCADE,
  node_id text NOT NULL,
  map_index integer,
  state text NOT NULL,
  attempt integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL,
  lease_owner text,
  lease_token text,
  lease_expires_at timestamptz,
  output jsonb,
  error text,
  cache_key text,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS lp_task_identity
  ON lp_tasks (run_id, node_id, COALESCE(map_index, -1));
CREATE INDEX IF NOT EXISTS lp_task_claim
  ON lp_tasks (state, available_at, created_at);
CREATE TABLE IF NOT EXISTS lp_expansions (
  run_id text NOT NULL REFERENCES lp_runs(id) ON DELETE CASCADE,
  node_id text NOT NULL,
  item_count integer NOT NULL,
  PRIMARY KEY (run_id, node_id)
);
CREATE TABLE IF NOT EXISTS lp_events (
  sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id text UNIQUE NOT NULL,
  run_id text NOT NULL REFERENCES lp_runs(id) ON DELETE CASCADE,
  task_id text,
  kind text NOT NULL,
  payload jsonb NOT NULL,
  occurred_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS lp_event_run_sequence ON lp_events (run_id, sequence);
CREATE TABLE IF NOT EXISTS lp_cache (
  key text PRIMARY KEY,
  output jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS lp_triggers (
  name text PRIMARY KEY,
  cursor jsonb,
  lease_owner text,
  lease_token text,
  lease_expires_at timestamptz,
  updated_at timestamptz NOT NULL
);
"""


def upgrade() -> None:
    op.execute(SCHEMA)


def downgrade() -> None:
    raise NotImplementedError("Lightpipe migrations are forward-only")
