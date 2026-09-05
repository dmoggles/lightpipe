"""Add execution policy and maintenance metadata.

Revision ID: 0004
Revises: 0003
"""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


SCHEMA = """
ALTER TABLE lp_runs ADD COLUMN IF NOT EXISTS priority integer NOT NULL DEFAULT 0;
ALTER TABLE lp_runs ADD COLUMN IF NOT EXISTS policy jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE lp_runs ADD COLUMN IF NOT EXISTS admitted_at timestamptz;
ALTER TABLE lp_runs DROP CONSTRAINT IF EXISTS lp_runs_rerun_of_fkey;
ALTER TABLE lp_runs ADD CONSTRAINT lp_runs_rerun_of_fkey
  FOREIGN KEY (rerun_of) REFERENCES lp_runs(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS lp_run_admission
  ON lp_runs (state, priority DESC, created_at) WHERE state='pending';

ALTER TABLE lp_cache ADD COLUMN IF NOT EXISTS pipeline_name text;
ALTER TABLE lp_cache ADD COLUMN IF NOT EXISTS last_used_at timestamptz
  NOT NULL DEFAULT clock_timestamp();
CREATE INDEX IF NOT EXISTS lp_cache_expiry ON lp_cache (expires_at);

CREATE TABLE IF NOT EXISTS lp_artifacts (
  uri text PRIMARY KEY,
  digest text,
  size bigint,
  discovered_at timestamptz NOT NULL,
  candidate_since timestamptz
);
CREATE TABLE IF NOT EXISTS lp_artifact_references (
  uri text NOT NULL REFERENCES lp_artifacts(uri) ON DELETE CASCADE,
  source_kind text NOT NULL,
  source_id text NOT NULL,
  PRIMARY KEY (uri, source_kind, source_id)
);
CREATE INDEX IF NOT EXISTS lp_artifact_reference_source
  ON lp_artifact_references (source_kind, source_id);
CREATE TABLE IF NOT EXISTS lp_artifact_pins (
  id text PRIMARY KEY,
  uri text NOT NULL,
  label text NOT NULL,
  created_at timestamptz NOT NULL,
  expires_at timestamptz
);
CREATE INDEX IF NOT EXISTS lp_artifact_pin_uri ON lp_artifact_pins (uri);

CREATE TABLE IF NOT EXISTS lp_maintenance_leases (
  name text PRIMARY KEY,
  owner text NOT NULL,
  token text NOT NULL,
  expires_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS lp_workers (
  id text PRIMARY KEY,
  state text NOT NULL,
  current_task_id text,
  last_seen_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS lp_rate_limits (
  pipeline_name text PRIMARY KEY,
  tokens double precision NOT NULL,
  updated_at timestamptz NOT NULL
);
"""


def upgrade() -> None:
    op.execute(SCHEMA)


def downgrade() -> None:
    raise NotImplementedError("Lightpipe migrations are forward-only")
