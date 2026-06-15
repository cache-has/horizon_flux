// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! PostgreSQL schema creation and migration.

use deadpool_postgres::Pool;

/// The current schema version. Bump when adding migrations.
const SCHEMA_VERSION: i32 = 1;

/// SQL for the initial schema (version 1).
const V1_SCHEMA: &str = r#"
-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pipeline metadata (definition JSON stored inline as JSONB)
CREATE TABLE IF NOT EXISTS pipelines (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    definition  JSONB NOT NULL,
    created_at  BIGINT NOT NULL,
    updated_at  BIGINT NOT NULL,
    last_run_at BIGINT,
    run_count   INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pipelines_name ON pipelines (name);

-- Pipeline version history
CREATE TABLE IF NOT EXISTS pipeline_versions (
    pipeline_id TEXT NOT NULL REFERENCES pipelines (id) ON DELETE CASCADE,
    version     INTEGER NOT NULL,
    saved_at    BIGINT NOT NULL,
    snapshot    JSONB NOT NULL,
    PRIMARY KEY (pipeline_id, version)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_versions_id
    ON pipeline_versions (pipeline_id, version DESC);

-- Pipeline execution history
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id            TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    environment   TEXT NOT NULL,
    status        TEXT NOT NULL,
    start_time_ms BIGINT,
    end_time_ms   BIGINT,
    error         TEXT,
    test_results  TEXT,
    triggered_by  TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_pipeline
    ON pipeline_runs (pipeline_name, start_time_ms DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status ON pipeline_runs (status);

-- Per-node execution statistics
CREATE TABLE IF NOT EXISTS node_run_stats (
    run_id        TEXT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    node_id       TEXT NOT NULL,
    start_time_ms BIGINT NOT NULL,
    end_time_ms   BIGINT NOT NULL,
    rows_in       BIGINT NOT NULL,
    rows_out      BIGINT NOT NULL,
    error         TEXT,
    materialization_receipt TEXT,
    PRIMARY KEY (run_id, node_id)
);

-- Doc 27 migration: older databases created before the materialization
-- receipt landed don't have this column. Postgres supports the idempotent
-- form so we can run it unconditionally.
ALTER TABLE node_run_stats ADD COLUMN IF NOT EXISTS materialization_receipt TEXT;

-- Doc 30 migration: test results on runs.
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS test_results TEXT;

-- Doc 37 migration: trigger attribution on runs.
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS triggered_by TEXT;

-- Incremental sink materialization state (planning doc 27).
-- One row per (pipeline_id, node_id, environment); cascades on pipeline delete.
CREATE TABLE IF NOT EXISTS incremental_state (
    pipeline_id        TEXT    NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    node_id            TEXT    NOT NULL,
    environment        TEXT    NOT NULL,
    watermark_column   TEXT    NOT NULL,
    watermark_value    TEXT    NOT NULL,
    watermark_type     TEXT    NOT NULL
        CHECK (watermark_type IN ('timestamp','int64','string')),
    last_run_at        BIGINT  NOT NULL,
    last_run_id        TEXT    NOT NULL,
    rows_processed     BIGINT  NOT NULL,
    schema_fingerprint TEXT,
    PRIMARY KEY (pipeline_id, node_id, environment)
);

CREATE INDEX IF NOT EXISTS idx_incremental_state_env
    ON incremental_state (environment);

-- Append-only history of Arrow schemas observed per run.
CREATE TABLE IF NOT EXISTS incremental_schema_history (
    pipeline_id  TEXT   NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    node_id      TEXT   NOT NULL,
    environment  TEXT   NOT NULL,
    run_id       TEXT   NOT NULL,
    schema_json  TEXT   NOT NULL,
    fingerprint  TEXT   NOT NULL,
    recorded_at  BIGINT NOT NULL,
    PRIMARY KEY (pipeline_id, node_id, environment, run_id)
);

CREATE INDEX IF NOT EXISTS idx_incremental_schema_history_node
    ON incremental_schema_history (pipeline_id, node_id, environment, recorded_at DESC);

-- Environment definitions
CREATE TABLE IF NOT EXISTS environments (
    name     TEXT PRIMARY KEY,
    fallback TEXT REFERENCES environments(name)
);

-- Table overrides per environment
CREATE TABLE IF NOT EXISTS table_overrides (
    environment TEXT NOT NULL REFERENCES environments(name) ON DELETE CASCADE,
    schema_name TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    PRIMARY KEY (environment, schema_name, table_name)
);

-- Cross-pipeline lineage: static resource bindings (planning doc 31).
CREATE TABLE IF NOT EXISTS pipeline_resource_bindings (
    pipeline_id          TEXT   NOT NULL,
    node_id              TEXT   NOT NULL,
    direction            TEXT   NOT NULL,
    resource_fingerprint TEXT   NOT NULL,
    environment          TEXT   NOT NULL,
    updated_at           BIGINT NOT NULL,
    PRIMARY KEY (pipeline_id, node_id, environment)
);

CREATE INDEX IF NOT EXISTS idx_prb_fingerprint
    ON pipeline_resource_bindings (resource_fingerprint, environment);

-- Cross-pipeline lineage: runtime-observed resource accesses (planning doc 31).
CREATE TABLE IF NOT EXISTS lineage_observations (
    pipeline_id          TEXT   NOT NULL,
    node_id              TEXT   NOT NULL,
    run_id               TEXT   NOT NULL,
    direction            TEXT   NOT NULL,
    resource_fingerprint TEXT   NOT NULL,
    environment          TEXT   NOT NULL,
    observed_at          BIGINT NOT NULL,
    PRIMARY KEY (run_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_lo_fingerprint
    ON lineage_observations (resource_fingerprint, environment, observed_at);

-- Column-level lineage edges (planning doc 35).
CREATE TABLE IF NOT EXISTS column_lineage_edges (
    id                             BIGSERIAL PRIMARY KEY,
    pipeline_id                    TEXT NOT NULL,
    environment                    TEXT NOT NULL,
    downstream_node_id             TEXT NOT NULL,
    downstream_column              TEXT NOT NULL,
    downstream_is_external         INTEGER NOT NULL,
    downstream_resource_fingerprint TEXT,
    upstream_node_id               TEXT,
    upstream_column                TEXT NOT NULL,
    upstream_is_external           INTEGER NOT NULL,
    upstream_resource_fingerprint  TEXT,
    relationship                   TEXT NOT NULL,
    expression_text                TEXT,
    confidence                     TEXT NOT NULL,
    derived_at                     TEXT NOT NULL,
    source_run_id                  TEXT
);

CREATE INDEX IF NOT EXISTS idx_cle_downstream
    ON column_lineage_edges (pipeline_id, environment, downstream_node_id, downstream_column);
CREATE INDEX IF NOT EXISTS idx_cle_upstream
    ON column_lineage_edges (pipeline_id, environment, upstream_node_id, upstream_column);
CREATE INDEX IF NOT EXISTS idx_cle_downstream_resource
    ON column_lineage_edges (downstream_resource_fingerprint, downstream_column);
CREATE INDEX IF NOT EXISTS idx_cle_upstream_resource
    ON column_lineage_edges (upstream_resource_fingerprint, upstream_column);

-- Failure reports captured when a node fails (planning doc 37).
CREATE TABLE IF NOT EXISTS failure_reports (
    run_id      TEXT   NOT NULL,
    node_id     TEXT   NOT NULL,
    report_json TEXT   NOT NULL,
    captured_at BIGINT NOT NULL,
    PRIMARY KEY (run_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_failure_reports_run ON failure_reports (run_id);

-- SLA evaluations (planning doc 37, sub-feature 3).
CREATE TABLE IF NOT EXISTS sla_evaluations (
    fingerprint       TEXT   NOT NULL,
    evaluated_at      TEXT   NOT NULL,
    status            TEXT   NOT NULL,
    age               TEXT,
    max_age           TEXT   NOT NULL,
    warn_at           TEXT,
    producer_pipeline TEXT,
    last_success_at   TEXT,
    PRIMARY KEY (fingerprint, evaluated_at)
);

CREATE INDEX IF NOT EXISTS idx_sla_evaluations_fp
    ON sla_evaluations (fingerprint, evaluated_at DESC);
"#;

/// Ensure the database schema is up to date.
///
/// Creates all tables on first run. Future versions will add incremental
/// migrations keyed by `schema_version`.
pub(crate) async fn ensure_schema(pool: &Pool) -> Result<(), String> {
    let client = pool
        .get()
        .await
        .map_err(|e| format!("failed to get connection for schema init: {e}"))?;

    // Check if schema_version table exists.
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'schema_version'
            )",
            &[],
        )
        .await
        .map_err(|e| format!("failed to check schema_version table: {e}"))?
        .get(0);

    if exists {
        let row = client
            .query_opt(
                "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1",
                &[],
            )
            .await
            .map_err(|e| format!("failed to read schema version: {e}"))?;

        if let Some(row) = row {
            let current: i32 = row.get(0);
            if current >= SCHEMA_VERSION {
                return Ok(());
            }
            // Future: apply incremental migrations from current+1..=SCHEMA_VERSION
        }
    }

    // Apply full schema (idempotent thanks to IF NOT EXISTS).
    client
        .batch_execute(V1_SCHEMA)
        .await
        .map_err(|e| format!("failed to create schema: {e}"))?;

    // Record the schema version.
    client
        .execute(
            "INSERT INTO schema_version (version) VALUES ($1)",
            &[&SCHEMA_VERSION],
        )
        .await
        .map_err(|e| format!("failed to record schema version: {e}"))?;

    Ok(())
}
