-- Enable pgcrypto extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. Workflow Definitions
CREATE TABLE IF NOT EXISTS workflows (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Workflow Runs
CREATE TABLE IF NOT EXISTS workflow_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id     UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    status          VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    current_step    VARCHAR(255),
    error_summary   TEXT,
    workflow_version INTEGER NOT NULL DEFAULT 1,
    schema_version   VARCHAR(50) NOT NULL DEFAULT 'v1',
    version          INTEGER NOT NULL DEFAULT 1,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at    TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT check_workflow_run_status 
        CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'PAUSED'))
);

-- 3. Immutable Step Executions (Event Ledger)
CREATE TABLE IF NOT EXISTS step_executions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_run_id     UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    step_name           VARCHAR(255) NOT NULL,
    status              VARCHAR(50) NOT NULL,
    input_payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_payload      JSONB,
    error_message       TEXT,
    execution_time_ms   INTEGER,
    attempt_number      INTEGER NOT NULL DEFAULT 1,
    idempotency_key     VARCHAR(255),
    workflow_version    INTEGER NOT NULL DEFAULT 1,
    schema_version      VARCHAR(50) NOT NULL DEFAULT 'v1',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT check_step_execution_status 
        CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED', 'INVALIDATED')),

    CONSTRAINT uq_step_attempt 
        UNIQUE (workflow_run_id, step_name, attempt_number)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_step_executions_run_step 
    ON step_executions (workflow_run_id, step_name);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_status 
    ON workflow_runs (status);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_workflow_id 
    ON workflow_runs (workflow_id);

CREATE INDEX IF NOT EXISTS idx_runs_status_heartbeat
    ON workflow_runs (status, heartbeat_at);

CREATE INDEX IF NOT EXISTS idx_checkpoints_run_active
    ON step_checkpoints (workflow_run_id, is_active);

