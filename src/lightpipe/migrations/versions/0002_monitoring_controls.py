"""Add durable definitions, attempts, logs, and rerun lineage.

Revision ID: 0002
Revises: 0001
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


SCHEMA = """
ALTER TABLE lp_runs ADD COLUMN IF NOT EXISTS rerun_of text REFERENCES lp_runs(id);
ALTER TABLE lp_runs ADD COLUMN IF NOT EXISTS trace_context jsonb;

CREATE TABLE IF NOT EXISTS lp_pipeline_definitions (
  definition_hash text PRIMARY KEY,
  pipeline_name text NOT NULL,
  graph jsonb NOT NULL,
  created_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS lp_definition_name_created
  ON lp_pipeline_definitions (pipeline_name, created_at DESC, definition_hash DESC);

CREATE TABLE IF NOT EXISTS lp_task_attempts (
  id text PRIMARY KEY,
  task_id text NOT NULL REFERENCES lp_tasks(id) ON DELETE CASCADE,
  run_id text NOT NULL REFERENCES lp_runs(id) ON DELETE CASCADE,
  attempt integer NOT NULL,
  worker_id text NOT NULL,
  state text NOT NULL,
  leased_at timestamptz NOT NULL,
  started_at timestamptz,
  finished_at timestamptz,
  error text,
  cache_hit boolean NOT NULL DEFAULT false,
  UNIQUE (task_id, attempt)
);
CREATE INDEX IF NOT EXISTS lp_attempt_run_task
  ON lp_task_attempts (run_id, task_id, attempt);

CREATE TABLE IF NOT EXISTS lp_stage_logs (
  sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id text UNIQUE NOT NULL,
  run_id text NOT NULL REFERENCES lp_runs(id) ON DELETE CASCADE,
  task_id text NOT NULL REFERENCES lp_tasks(id) ON DELETE CASCADE,
  attempt integer NOT NULL,
  occurred_at timestamptz NOT NULL,
  stream text NOT NULL,
  level text NOT NULL,
  logger text,
  message text NOT NULL,
  fields jsonb NOT NULL,
  trace_id text,
  span_id text
);
CREATE INDEX IF NOT EXISTS lp_log_task_sequence ON lp_stage_logs (task_id, sequence);
CREATE INDEX IF NOT EXISTS lp_log_run_sequence ON lp_stage_logs (run_id, sequence);
"""


def upgrade() -> None:
    op.execute(SCHEMA)


def downgrade() -> None:
    raise NotImplementedError("Lightpipe migrations are forward-only")
