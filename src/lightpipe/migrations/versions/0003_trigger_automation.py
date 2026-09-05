"""Add managed trigger definitions and occurrence history.

Revision ID: 0003
Revises: 0002
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


SCHEMA = """
ALTER TABLE lp_runs ADD COLUMN IF NOT EXISTS trigger_name text;
ALTER TABLE lp_runs ADD COLUMN IF NOT EXISTS trigger_occurrence_id text;

ALTER TABLE lp_triggers ADD COLUMN IF NOT EXISTS kind text;
ALTER TABLE lp_triggers ADD COLUMN IF NOT EXISTS definition_hash text;
ALTER TABLE lp_triggers ADD COLUMN IF NOT EXISTS config jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE lp_triggers ADD COLUMN IF NOT EXISTS enabled boolean NOT NULL DEFAULT true;
ALTER TABLE lp_triggers ADD COLUMN IF NOT EXISTS last_due_at timestamptz;
ALTER TABLE lp_triggers ADD COLUMN IF NOT EXISTS next_due_at timestamptz;
ALTER TABLE lp_triggers ADD COLUMN IF NOT EXISTS created_at timestamptz
  NOT NULL DEFAULT clock_timestamp();

CREATE TABLE IF NOT EXISTS lp_trigger_occurrences (
  sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id text UNIQUE NOT NULL,
  trigger_name text NOT NULL REFERENCES lp_triggers(name) ON DELETE CASCADE,
  state text NOT NULL,
  occurred_at timestamptz NOT NULL,
  scheduled_for timestamptz,
  delivery_id text,
  requests jsonb NOT NULL DEFAULT '[]'::jsonb,
  run_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  detail text,
  updated_at timestamptz NOT NULL,
  UNIQUE (trigger_name, delivery_id)
);
CREATE INDEX IF NOT EXISTS lp_trigger_occurrence_history
  ON lp_trigger_occurrences (trigger_name, sequence DESC);
CREATE UNIQUE INDEX IF NOT EXISTS lp_trigger_scheduled_occurrence
  ON lp_trigger_occurrences (trigger_name, scheduled_for)
  WHERE scheduled_for IS NOT NULL;
CREATE INDEX IF NOT EXISTS lp_trigger_state_next_due
  ON lp_triggers (enabled, next_due_at);
CREATE INDEX IF NOT EXISTS lp_run_trigger_occurrence
  ON lp_runs (trigger_occurrence_id) WHERE trigger_occurrence_id IS NOT NULL;
"""


def upgrade() -> None:
    op.execute(SCHEMA)


def downgrade() -> None:
    raise NotImplementedError("Lightpipe migrations are forward-only")
