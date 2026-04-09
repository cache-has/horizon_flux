// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite-backed storage for pipeline execution history.

use crate::error::{IncrementalStateError, LineageStoreError, RunStoreError};
use crate::incremental_state::{IncrementalSchemaRecord, IncrementalState};
use crate::run::{NodeRunStats, PipelineRun, RunId, RunStatus, TestResultSummary};
use crate::storage::{
    IncrementalStateStorage, LineageObservation, LineageStorage, RunStorage, StoredResourceBinding,
};
use flux_engine::NodeId;
use flux_engine::lineage::BindingDirection;
use flux_engine::lineage::ResourceFingerprint;
use rusqlite::{Connection, params};
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Persists pipeline run history and per-node statistics in embedded SQLite.
pub struct SqliteRunStore {
    conn: Mutex<Connection>,
}

impl SqliteRunStore {
    /// Open (or create) a run store at the given file path.
    pub fn open(path: &Path) -> Result<Self, RunStoreError> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        Ok(store)
    }

    /// Open an in-memory run store (useful for tests).
    pub fn open_in_memory() -> Result<Self, RunStoreError> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<(), RunStoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS pipeline_runs (
                id            TEXT PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                environment   TEXT NOT NULL,
                status        TEXT NOT NULL,
                start_time_ms INTEGER,
                end_time_ms   INTEGER,
                error         TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_runs_pipeline
                ON pipeline_runs (pipeline_name, start_time_ms DESC);

            CREATE TABLE IF NOT EXISTS node_run_stats (
                run_id        TEXT NOT NULL REFERENCES pipeline_runs(id),
                node_id       TEXT NOT NULL,
                start_time_ms INTEGER NOT NULL,
                end_time_ms   INTEGER NOT NULL,
                rows_in       INTEGER NOT NULL,
                rows_out      INTEGER NOT NULL,
                error         TEXT,
                materialization_receipt TEXT,
                PRIMARY KEY (run_id, node_id)
            );

            -- Incremental sink materialization state (planning doc 27).
            -- One row per (pipeline_id, node_id, environment); the latest run
            -- upserts it. No FK to pipelines(id) because the pipeline store
            -- lives in a separate SQLite database file in this backend.
            CREATE TABLE IF NOT EXISTS incremental_state (
                pipeline_id        TEXT NOT NULL,
                node_id            TEXT NOT NULL,
                environment        TEXT NOT NULL,
                watermark_column   TEXT NOT NULL,
                watermark_value    TEXT NOT NULL,
                watermark_type     TEXT NOT NULL
                    CHECK (watermark_type IN ('timestamp','int64','string')),
                last_run_at        INTEGER NOT NULL,
                last_run_id        TEXT NOT NULL,
                rows_processed     INTEGER NOT NULL,
                schema_fingerprint TEXT,
                PRIMARY KEY (pipeline_id, node_id, environment)
            );

            CREATE INDEX IF NOT EXISTS idx_incremental_state_env
                ON incremental_state (environment);

            -- Append-only history of Arrow schemas observed per run, used by
            -- the schema-change detector and the run-history UI.
            CREATE TABLE IF NOT EXISTS incremental_schema_history (
                pipeline_id  TEXT    NOT NULL,
                node_id      TEXT    NOT NULL,
                environment  TEXT    NOT NULL,
                run_id       TEXT    NOT NULL,
                schema_json  TEXT    NOT NULL,
                fingerprint  TEXT    NOT NULL,
                recorded_at  INTEGER NOT NULL,
                PRIMARY KEY (pipeline_id, node_id, environment, run_id)
            );

            CREATE INDEX IF NOT EXISTS idx_incremental_schema_history_node
                ON incremental_schema_history
                   (pipeline_id, node_id, environment, recorded_at DESC);

            -- Cross-pipeline lineage: static resource bindings (planning doc 31).
            CREATE TABLE IF NOT EXISTS pipeline_resource_bindings (
                pipeline_id          TEXT NOT NULL,
                node_id              TEXT NOT NULL,
                direction            TEXT NOT NULL,
                resource_fingerprint TEXT NOT NULL,
                environment          TEXT NOT NULL,
                updated_at           INTEGER NOT NULL,
                PRIMARY KEY (pipeline_id, node_id, environment)
            );

            CREATE INDEX IF NOT EXISTS idx_prb_fingerprint
                ON pipeline_resource_bindings (resource_fingerprint, environment);

            -- Cross-pipeline lineage: runtime-observed resource accesses (planning doc 31).
            CREATE TABLE IF NOT EXISTS lineage_observations (
                pipeline_id          TEXT NOT NULL,
                node_id              TEXT NOT NULL,
                run_id               TEXT NOT NULL,
                direction            TEXT NOT NULL,
                resource_fingerprint TEXT NOT NULL,
                environment          TEXT NOT NULL,
                observed_at          INTEGER NOT NULL,
                PRIMARY KEY (run_id, node_id)
            );

            CREATE INDEX IF NOT EXISTS idx_lo_fingerprint
                ON lineage_observations (resource_fingerprint, environment, observed_at);",
        )?;
        // Idempotent migration: older databases created before doc 27 don't
        // have the receipt column. SQLite has no `ADD COLUMN IF NOT EXISTS`,
        // so attempt the ALTER and swallow the duplicate-column error.
        let alter = conn.execute(
            "ALTER TABLE node_run_stats ADD COLUMN materialization_receipt TEXT",
            [],
        );
        if let Err(rusqlite::Error::SqliteFailure(_, Some(msg))) = &alter {
            if !msg.contains("duplicate column") {
                alter.map(|_| ())?;
            }
        } else if let Err(e) = alter {
            // Other errors are real — bubble up.
            return Err(e.into());
        }

        // Idempotent migration: add test_results JSON column (doc 30).
        let alter2 = conn.execute("ALTER TABLE pipeline_runs ADD COLUMN test_results TEXT", []);
        if let Err(rusqlite::Error::SqliteFailure(_, Some(msg))) = &alter2 {
            if !msg.contains("duplicate column") {
                alter2.map(|_| ())?;
            }
        } else if let Err(e) = alter2 {
            return Err(e.into());
        }

        Ok(())
    }

    fn load_node_stats_locked(
        &self,
        conn: &Connection,
        run_id: &RunId,
    ) -> Result<Vec<NodeRunStats>, RunStoreError> {
        let mut stmt = conn.prepare(
            "SELECT node_id, start_time_ms, end_time_ms, rows_in, rows_out, error,
                    materialization_receipt
             FROM node_run_stats WHERE run_id = ?1
             ORDER BY start_time_ms ASC",
        )?;
        let mut rows = stmt.query(params![run_id.0.to_string()])?;
        let mut stats = Vec::new();
        while let Some(row) = rows.next()? {
            let receipt_json: Option<String> = row.get(6)?;
            let materialization_receipt = receipt_json
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok());
            stats.push(NodeRunStats {
                node_id: NodeId::new(row.get::<_, String>(0)?),
                start_time: ms_to_system_time(row.get::<_, i64>(1)?),
                end_time: ms_to_system_time(row.get::<_, i64>(2)?),
                rows_in: row.get::<_, i64>(3)? as u64,
                rows_out: row.get::<_, i64>(4)? as u64,
                error: row.get(5)?,
                materialization_receipt,
            });
        }
        Ok(stats)
    }

    /// Import a pipeline run preserving its original ID, timestamps, status, and node stats.
    ///
    /// Used by `flux metadata import` to copy data from a remote store.
    /// Skips the run if one with the same ID already exists.
    pub fn import_run(&self, run: &PipelineRun) -> Result<(), RunStoreError> {
        let conn = self.conn.lock().unwrap();
        let start_ms = run.start_time.map(system_time_to_ms);
        let end_ms = run.end_time.map(system_time_to_ms);
        let test_results_json: Option<String> = if run.test_results.is_empty() {
            None
        } else {
            serde_json::to_string(&run.test_results).ok()
        };

        conn.execute(
            "INSERT OR IGNORE INTO pipeline_runs (id, pipeline_name, environment, status, start_time_ms, end_time_ms, error, test_results)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                run.id.0.to_string(),
                run.pipeline_name,
                run.environment,
                run.status.as_str(),
                start_ms,
                end_ms,
                run.error,
                test_results_json,
            ],
        )?;

        for stats in &run.node_stats {
            let receipt_json = stats
                .materialization_receipt
                .as_ref()
                .and_then(|r| serde_json::to_string(r).ok());
            conn.execute(
                "INSERT OR IGNORE INTO node_run_stats (run_id, node_id, start_time_ms, end_time_ms, rows_in, rows_out, error, materialization_receipt)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    run.id.0.to_string(),
                    stats.node_id.0,
                    system_time_to_ms(stats.start_time),
                    system_time_to_ms(stats.end_time),
                    stats.rows_in as i64,
                    stats.rows_out as i64,
                    stats.error,
                    receipt_json,
                ],
            )?;
        }

        Ok(())
    }
}

impl RunStorage for SqliteRunStore {
    fn create_run(
        &self,
        pipeline_name: &str,
        environment: &str,
    ) -> Result<PipelineRun, RunStoreError> {
        let run = PipelineRun::new(pipeline_name, environment);
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO pipeline_runs (id, pipeline_name, environment, status)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                run.id.0.to_string(),
                run.pipeline_name,
                run.environment,
                run.status.as_str(),
            ],
        )?;
        Ok(run)
    }

    fn set_running(&self, run_id: &RunId, start_time: SystemTime) -> Result<(), RunStoreError> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "UPDATE pipeline_runs SET status = ?1, start_time_ms = ?2 WHERE id = ?3",
            params![
                RunStatus::Running.as_str(),
                system_time_to_ms(start_time),
                run_id.0.to_string(),
            ],
        )?;
        if rows == 0 {
            return Err(RunStoreError::NotFound(run_id.to_string()));
        }
        Ok(())
    }

    fn finish_run(
        &self,
        run_id: &RunId,
        status: RunStatus,
        end_time: SystemTime,
        error: Option<&str>,
    ) -> Result<(), RunStoreError> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "UPDATE pipeline_runs SET status = ?1, end_time_ms = ?2, error = ?3 WHERE id = ?4",
            params![
                status.as_str(),
                system_time_to_ms(end_time),
                error,
                run_id.0.to_string(),
            ],
        )?;
        if rows == 0 {
            return Err(RunStoreError::NotFound(run_id.to_string()));
        }
        Ok(())
    }

    fn save_test_results(
        &self,
        run_id: &RunId,
        results: &[TestResultSummary],
    ) -> Result<(), RunStoreError> {
        let conn = self.conn.lock().unwrap();
        let json = serde_json::to_string(results).map_err(|e| {
            RunStoreError::Database(format!("failed to serialize test results: {e}"))
        })?;
        conn.execute(
            "UPDATE pipeline_runs SET test_results = ?1 WHERE id = ?2",
            params![json, run_id.0.to_string()],
        )?;
        Ok(())
    }

    fn save_node_stats(&self, run_id: &RunId, stats: &NodeRunStats) -> Result<(), RunStoreError> {
        let conn = self.conn.lock().unwrap();
        let receipt_json = stats
            .materialization_receipt
            .as_ref()
            .and_then(|r| serde_json::to_string(r).ok());
        conn.execute(
            "INSERT OR REPLACE INTO node_run_stats
                (run_id, node_id, start_time_ms, end_time_ms, rows_in, rows_out, error, materialization_receipt)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                run_id.0.to_string(),
                stats.node_id.0,
                system_time_to_ms(stats.start_time),
                system_time_to_ms(stats.end_time),
                stats.rows_in as i64,
                stats.rows_out as i64,
                stats.error,
                receipt_json,
            ],
        )?;
        Ok(())
    }

    fn get_run(&self, run_id: &RunId) -> Result<Option<PipelineRun>, RunStoreError> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare(
            "SELECT id, pipeline_name, environment, status, start_time_ms, end_time_ms, error,
                    test_results
             FROM pipeline_runs WHERE id = ?1",
        )?;

        let mut rows = stmt.query(params![run_id.0.to_string()])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };

        let mut run = row_to_pipeline_run(row)?;
        run.node_stats = self.load_node_stats_locked(&conn, run_id)?;
        Ok(Some(run))
    }

    fn list_runs(
        &self,
        pipeline_name: Option<&str>,
        limit: u32,
    ) -> Result<Vec<PipelineRun>, RunStoreError> {
        let conn = self.conn.lock().unwrap();

        let mut runs = Vec::new();

        match pipeline_name {
            Some(name) => {
                let mut stmt = conn.prepare(
                    "SELECT id, pipeline_name, environment, status, start_time_ms, end_time_ms, error,
                            test_results
                     FROM pipeline_runs
                     WHERE pipeline_name = ?1
                     ORDER BY start_time_ms DESC
                     LIMIT ?2",
                )?;
                let mut rows = stmt.query(params![name, limit])?;
                while let Some(row) = rows.next()? {
                    let mut run = row_to_pipeline_run(row)?;
                    let run_id = run.id.clone();
                    run.node_stats = self.load_node_stats_locked(&conn, &run_id)?;
                    runs.push(run);
                }
            }
            None => {
                let mut stmt = conn.prepare(
                    "SELECT id, pipeline_name, environment, status, start_time_ms, end_time_ms, error,
                            test_results
                     FROM pipeline_runs
                     ORDER BY start_time_ms DESC
                     LIMIT ?1",
                )?;
                let mut rows = stmt.query(params![limit])?;
                while let Some(row) = rows.next()? {
                    let mut run = row_to_pipeline_run(row)?;
                    let run_id = run.id.clone();
                    run.node_stats = self.load_node_stats_locked(&conn, &run_id)?;
                    runs.push(run);
                }
            }
        }

        Ok(runs)
    }
}

impl IncrementalStateStorage for SqliteRunStore {
    fn load_state(
        &self,
        pipeline_id: &str,
        node_id: &str,
        environment: &str,
    ) -> Result<Option<IncrementalState>, IncrementalStateError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT pipeline_id, node_id, environment, watermark_column, watermark_value,
                    watermark_type, last_run_at, last_run_id, rows_processed, schema_fingerprint
             FROM incremental_state
             WHERE pipeline_id = ?1 AND node_id = ?2 AND environment = ?3",
        )?;
        let mut rows = stmt.query(params![pipeline_id, node_id, environment])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_incremental_state(row)?)),
            None => Ok(None),
        }
    }

    fn save_state(&self, state: &IncrementalState) -> Result<(), IncrementalStateError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO incremental_state
                (pipeline_id, node_id, environment, watermark_column, watermark_value,
                 watermark_type, last_run_at, last_run_id, rows_processed, schema_fingerprint)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT (pipeline_id, node_id, environment) DO UPDATE SET
                watermark_column   = excluded.watermark_column,
                watermark_value    = excluded.watermark_value,
                watermark_type     = excluded.watermark_type,
                last_run_at        = excluded.last_run_at,
                last_run_id        = excluded.last_run_id,
                rows_processed     = excluded.rows_processed,
                schema_fingerprint = excluded.schema_fingerprint",
            params![
                state.pipeline_id,
                state.node_id,
                state.environment,
                state.watermark_column,
                state.watermark_value,
                state.watermark_type,
                state.last_run_at_ms,
                state.last_run_id,
                state.rows_processed as i64,
                state.schema_fingerprint,
            ],
        )?;
        Ok(())
    }

    fn reset_state(
        &self,
        pipeline_id: &str,
        node_id: &str,
        environment: &str,
    ) -> Result<bool, IncrementalStateError> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "DELETE FROM incremental_state
             WHERE pipeline_id = ?1 AND node_id = ?2 AND environment = ?3",
            params![pipeline_id, node_id, environment],
        )?;
        Ok(rows > 0)
    }

    fn list_states(
        &self,
        environment: Option<&str>,
    ) -> Result<Vec<IncrementalState>, IncrementalStateError> {
        let conn = self.conn.lock().unwrap();
        let mut out = Vec::new();
        match environment {
            Some(env) => {
                let mut stmt = conn.prepare(
                    "SELECT pipeline_id, node_id, environment, watermark_column, watermark_value,
                            watermark_type, last_run_at, last_run_id, rows_processed,
                            schema_fingerprint
                     FROM incremental_state
                     WHERE environment = ?1
                     ORDER BY pipeline_id, node_id",
                )?;
                let mut rows = stmt.query(params![env])?;
                while let Some(row) = rows.next()? {
                    out.push(row_to_incremental_state(row)?);
                }
            }
            None => {
                let mut stmt = conn.prepare(
                    "SELECT pipeline_id, node_id, environment, watermark_column, watermark_value,
                            watermark_type, last_run_at, last_run_id, rows_processed,
                            schema_fingerprint
                     FROM incremental_state
                     ORDER BY environment, pipeline_id, node_id",
                )?;
                let mut rows = stmt.query([])?;
                while let Some(row) = rows.next()? {
                    out.push(row_to_incremental_state(row)?);
                }
            }
        }
        Ok(out)
    }

    fn record_schema(&self, record: &IncrementalSchemaRecord) -> Result<(), IncrementalStateError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO incremental_schema_history
                (pipeline_id, node_id, environment, run_id, schema_json, fingerprint, recorded_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT (pipeline_id, node_id, environment, run_id) DO UPDATE SET
                schema_json = excluded.schema_json,
                fingerprint = excluded.fingerprint,
                recorded_at = excluded.recorded_at",
            params![
                record.pipeline_id,
                record.node_id,
                record.environment,
                record.run_id,
                record.schema_json,
                record.fingerprint,
                record.recorded_at_ms,
            ],
        )?;
        Ok(())
    }

    fn latest_schema(
        &self,
        pipeline_id: &str,
        node_id: &str,
        environment: &str,
    ) -> Result<Option<IncrementalSchemaRecord>, IncrementalStateError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT pipeline_id, node_id, environment, run_id, schema_json, fingerprint,
                    recorded_at
             FROM incremental_schema_history
             WHERE pipeline_id = ?1 AND node_id = ?2 AND environment = ?3
             ORDER BY recorded_at DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![pipeline_id, node_id, environment])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_schema_record(row)?)),
            None => Ok(None),
        }
    }

    fn import_state(&self, state: &IncrementalState) -> Result<(), IncrementalStateError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO incremental_state
                (pipeline_id, node_id, environment, watermark_column, watermark_value,
                 watermark_type, last_run_at, last_run_id, rows_processed, schema_fingerprint)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                state.pipeline_id,
                state.node_id,
                state.environment,
                state.watermark_column,
                state.watermark_value,
                state.watermark_type,
                state.last_run_at_ms,
                state.last_run_id,
                state.rows_processed as i64,
                state.schema_fingerprint,
            ],
        )?;
        Ok(())
    }

    fn import_schema_record(
        &self,
        record: &IncrementalSchemaRecord,
    ) -> Result<(), IncrementalStateError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO incremental_schema_history
                (pipeline_id, node_id, environment, run_id, schema_json, fingerprint, recorded_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                record.pipeline_id,
                record.node_id,
                record.environment,
                record.run_id,
                record.schema_json,
                record.fingerprint,
                record.recorded_at_ms,
            ],
        )?;
        Ok(())
    }
}

impl LineageStorage for SqliteRunStore {
    fn save_bindings(
        &self,
        pipeline_id: &str,
        environment: &str,
        bindings: &[StoredResourceBinding],
    ) -> Result<(), LineageStoreError> {
        let conn = self.conn.lock().unwrap();
        // Atomic replace: delete existing, then insert new.
        conn.execute(
            "DELETE FROM pipeline_resource_bindings
             WHERE pipeline_id = ?1 AND environment = ?2",
            params![pipeline_id, environment],
        )?;
        for b in bindings {
            conn.execute(
                "INSERT INTO pipeline_resource_bindings
                    (pipeline_id, node_id, direction, resource_fingerprint, environment, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    b.pipeline_id,
                    b.node_id,
                    b.direction.to_string(),
                    b.resource_fingerprint.0,
                    b.environment,
                    b.updated_at_ms,
                ],
            )?;
        }
        Ok(())
    }

    fn load_bindings(
        &self,
        pipeline_id: &str,
        environment: &str,
    ) -> Result<Vec<StoredResourceBinding>, LineageStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT pipeline_id, node_id, direction, resource_fingerprint, environment, updated_at
             FROM pipeline_resource_bindings
             WHERE pipeline_id = ?1 AND environment = ?2
             ORDER BY node_id",
        )?;
        let mut rows = stmt.query(params![pipeline_id, environment])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(row_to_binding(row)?);
        }
        Ok(out)
    }

    fn all_bindings(
        &self,
        environment: &str,
    ) -> Result<Vec<StoredResourceBinding>, LineageStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT pipeline_id, node_id, direction, resource_fingerprint, environment, updated_at
             FROM pipeline_resource_bindings
             WHERE environment = ?1
             ORDER BY pipeline_id, node_id",
        )?;
        let mut rows = stmt.query(params![environment])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(row_to_binding(row)?);
        }
        Ok(out)
    }

    fn delete_bindings(&self, pipeline_id: &str) -> Result<(), LineageStoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM pipeline_resource_bindings WHERE pipeline_id = ?1",
            params![pipeline_id],
        )?;
        Ok(())
    }

    fn record_observation(
        &self,
        observation: &LineageObservation,
    ) -> Result<(), LineageStoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO lineage_observations
                (pipeline_id, node_id, run_id, direction, resource_fingerprint, environment,
                 observed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                observation.pipeline_id,
                observation.node_id,
                observation.run_id,
                observation.direction.to_string(),
                observation.resource_fingerprint.0,
                observation.environment,
                observation.observed_at_ms,
            ],
        )?;
        Ok(())
    }

    fn query_observations(
        &self,
        environment: &str,
        since_ms: i64,
    ) -> Result<Vec<LineageObservation>, LineageStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT pipeline_id, node_id, run_id, direction, resource_fingerprint, environment,
                    observed_at
             FROM lineage_observations
             WHERE environment = ?1 AND observed_at >= ?2
             ORDER BY observed_at DESC",
        )?;
        let mut rows = stmt.query(params![environment, since_ms])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(row_to_observation(row)?);
        }
        Ok(out)
    }

    fn enforce_retention(&self, older_than_ms: i64) -> Result<u64, LineageStoreError> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM lineage_observations WHERE observed_at < ?1",
            params![older_than_ms],
        )?;
        Ok(deleted as u64)
    }
}

fn row_to_binding(row: &rusqlite::Row<'_>) -> Result<StoredResourceBinding, LineageStoreError> {
    let direction_str: String = row.get(2)?;
    let direction = match direction_str.as_str() {
        "source" => BindingDirection::Source,
        _ => BindingDirection::Sink,
    };
    Ok(StoredResourceBinding {
        pipeline_id: row.get(0)?,
        node_id: row.get(1)?,
        direction,
        resource_fingerprint: ResourceFingerprint::new(row.get::<_, String>(3)?),
        environment: row.get(4)?,
        updated_at_ms: row.get(5)?,
    })
}

fn row_to_observation(row: &rusqlite::Row<'_>) -> Result<LineageObservation, LineageStoreError> {
    let direction_str: String = row.get(3)?;
    let direction = match direction_str.as_str() {
        "source" => BindingDirection::Source,
        _ => BindingDirection::Sink,
    };
    Ok(LineageObservation {
        pipeline_id: row.get(0)?,
        node_id: row.get(1)?,
        run_id: row.get(2)?,
        direction,
        resource_fingerprint: ResourceFingerprint::new(row.get::<_, String>(4)?),
        environment: row.get(5)?,
        observed_at_ms: row.get(6)?,
    })
}

fn row_to_incremental_state(
    row: &rusqlite::Row<'_>,
) -> Result<IncrementalState, IncrementalStateError> {
    Ok(IncrementalState {
        pipeline_id: row.get(0)?,
        node_id: row.get(1)?,
        environment: row.get(2)?,
        watermark_column: row.get(3)?,
        watermark_value: row.get(4)?,
        watermark_type: row.get(5)?,
        last_run_at_ms: row.get(6)?,
        last_run_id: row.get(7)?,
        rows_processed: row.get::<_, i64>(8)? as u64,
        schema_fingerprint: row.get(9)?,
    })
}

fn row_to_schema_record(
    row: &rusqlite::Row<'_>,
) -> Result<IncrementalSchemaRecord, IncrementalStateError> {
    Ok(IncrementalSchemaRecord {
        pipeline_id: row.get(0)?,
        node_id: row.get(1)?,
        environment: row.get(2)?,
        run_id: row.get(3)?,
        schema_json: row.get(4)?,
        fingerprint: row.get(5)?,
        recorded_at_ms: row.get(6)?,
    })
}

fn row_to_pipeline_run(row: &rusqlite::Row<'_>) -> Result<PipelineRun, RunStoreError> {
    let id_str: String = row.get(0)?;
    let status_str: String = row.get(3)?;
    let start_ms: Option<i64> = row.get(4)?;
    let end_ms: Option<i64> = row.get(5)?;
    let test_results_json: Option<String> = row.get(7)?;
    let test_results = test_results_json
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();

    Ok(PipelineRun {
        id: RunId(
            Uuid::parse_str(&id_str)
                .map_err(|e| RunStoreError::InvalidStatus(format!("bad UUID: {e}")))?,
        ),
        pipeline_name: row.get(1)?,
        environment: row.get(2)?,
        status: RunStatus::parse(&status_str)
            .ok_or_else(|| RunStoreError::InvalidStatus(status_str))?,
        start_time: start_ms.map(ms_to_system_time),
        end_time: end_ms.map(ms_to_system_time),
        node_stats: Vec::new(), // populated by caller
        error: row.get(6)?,
        test_results,
    })
}

fn system_time_to_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn ms_to_system_time(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RunStorage;

    #[test]
    fn import_run_preserves_fields() {
        let store1 = SqliteRunStore::open_in_memory().unwrap();
        let run = store1.create_run("test-pipe", "dev").unwrap();
        let start = SystemTime::now();
        store1.set_running(&run.id, start).unwrap();
        store1
            .finish_run(&run.id, RunStatus::Success, SystemTime::now(), None)
            .unwrap();
        let original = store1.get_run(&run.id).unwrap().unwrap();

        // Import into a second store.
        let store2 = SqliteRunStore::open_in_memory().unwrap();
        store2.import_run(&original).unwrap();

        let fetched = store2.get_run(&run.id).unwrap().unwrap();
        assert_eq!(fetched.id, original.id);
        assert_eq!(fetched.pipeline_name, "test-pipe");
        assert_eq!(fetched.environment, "dev");
        assert!(matches!(fetched.status, RunStatus::Success));
    }

    #[test]
    fn incremental_state_round_trip() {
        let store = SqliteRunStore::open_in_memory().unwrap();

        // load on empty store -> None
        let none = store.load_state("p1", "sink", "dev").unwrap();
        assert!(none.is_none());

        // save and re-load
        let s1 = IncrementalState {
            pipeline_id: "p1".into(),
            node_id: "sink".into(),
            environment: "dev".into(),
            watermark_column: "updated_at".into(),
            watermark_value: "2026-04-08T00:00:00.000000000Z".into(),
            watermark_type: "timestamp".into(),
            last_run_at_ms: 1_700_000_000_000,
            last_run_id: "run-1".into(),
            rows_processed: 42,
            schema_fingerprint: Some("abc123".into()),
        };
        store.save_state(&s1).unwrap();
        let loaded = store.load_state("p1", "sink", "dev").unwrap().unwrap();
        assert_eq!(loaded, s1);

        // upsert advances the watermark
        let s2 = IncrementalState {
            watermark_value: "2026-04-09T00:00:00.000000000Z".into(),
            last_run_at_ms: 1_700_000_100_000,
            last_run_id: "run-2".into(),
            rows_processed: 7,
            ..s1.clone()
        };
        store.save_state(&s2).unwrap();
        assert_eq!(store.load_state("p1", "sink", "dev").unwrap().unwrap(), s2);

        // env isolation: prod has no state of its own
        assert!(store.load_state("p1", "sink", "prod").unwrap().is_none());

        // list filters by env
        let mut prod = s1.clone();
        prod.environment = "prod".into();
        store.save_state(&prod).unwrap();
        assert_eq!(store.list_states(Some("dev")).unwrap().len(), 1);
        assert_eq!(store.list_states(Some("prod")).unwrap().len(), 1);
        assert_eq!(store.list_states(None).unwrap().len(), 2);

        // schema history append + latest
        let r1 = IncrementalSchemaRecord {
            pipeline_id: "p1".into(),
            node_id: "sink".into(),
            environment: "dev".into(),
            run_id: "run-1".into(),
            schema_json: "{\"fields\":[]}".into(),
            fingerprint: "f1".into(),
            recorded_at_ms: 1_700_000_000_000,
        };
        let r2 = IncrementalSchemaRecord {
            run_id: "run-2".into(),
            fingerprint: "f2".into(),
            recorded_at_ms: 1_700_000_100_000,
            ..r1.clone()
        };
        store.record_schema(&r1).unwrap();
        store.record_schema(&r2).unwrap();
        let latest = store.latest_schema("p1", "sink", "dev").unwrap().unwrap();
        assert_eq!(latest.fingerprint, "f2");

        // reset removes the row, returns true once
        assert!(store.reset_state("p1", "sink", "dev").unwrap());
        assert!(!store.reset_state("p1", "sink", "dev").unwrap());
        assert!(store.load_state("p1", "sink", "dev").unwrap().is_none());

        // import is idempotent
        store.import_state(&s1).unwrap();
        store.import_state(&s1).unwrap();
        assert_eq!(store.list_states(Some("dev")).unwrap().len(), 1);
        store.import_schema_record(&r1).unwrap();
        store.import_schema_record(&r1).unwrap();
    }

    #[test]
    fn import_run_skips_duplicate() {
        let store = SqliteRunStore::open_in_memory().unwrap();
        let run = store.create_run("pipe", "prod").unwrap();

        // Importing the same run again should not error.
        let full_run = store.get_run(&run.id).unwrap().unwrap();
        store.import_run(&full_run).unwrap();

        let runs = store.list_runs(None, 100).unwrap();
        assert_eq!(runs.len(), 1);
    }

    #[test]
    fn test_results_round_trip() {
        use crate::run::{AssertionResultSummary, TestResultSummary};
        use flux_engine::NodeId;
        use flux_engine::node::TestSeverity;

        let store = SqliteRunStore::open_in_memory().unwrap();
        let run = store.create_run("test-pipe", "dev").unwrap();
        store.set_running(&run.id, SystemTime::now()).unwrap();

        let results = vec![TestResultSummary {
            node_id: NodeId::new("validate_orders"),
            passed: false,
            severity: TestSeverity::Error,
            assertions: vec![
                AssertionResultSummary {
                    name: "not_null(order_id)".into(),
                    passed: true,
                    violation_count: 0,
                    violating_rows: Vec::new(),
                    message: None,
                },
                AssertionResultSummary {
                    name: "unique(order_id)".into(),
                    passed: false,
                    violation_count: 3,
                    violating_rows: vec![serde_json::json!({"order_id": 42})],
                    message: Some("3 duplicate(s) found".into()),
                },
            ],
        }];

        store.save_test_results(&run.id, &results).unwrap();
        store
            .finish_run(&run.id, RunStatus::Failed, SystemTime::now(), None)
            .unwrap();

        let loaded = store.get_run(&run.id).unwrap().unwrap();
        assert_eq!(loaded.test_results.len(), 1);
        let tr = &loaded.test_results[0];
        assert_eq!(tr.node_id, NodeId::new("validate_orders"));
        assert!(!tr.passed);
        assert_eq!(tr.assertions.len(), 2);
        assert!(tr.assertions[0].passed);
        assert!(!tr.assertions[1].passed);
        assert_eq!(tr.assertions[1].violation_count, 3);
        assert_eq!(tr.assertions[1].violating_rows.len(), 1);
    }

    // -- Lineage storage tests (planning doc 31) ---------------------------

    #[test]
    fn lineage_bindings_round_trip() {
        let store = SqliteRunStore::open_in_memory().unwrap();

        // Empty store returns empty vec.
        let bindings = store.load_bindings("p1", "dev").unwrap();
        assert!(bindings.is_empty());

        // Save bindings for pipeline p1, env dev.
        let b1 = StoredResourceBinding {
            pipeline_id: "p1".into(),
            node_id: "src".into(),
            direction: BindingDirection::Source,
            resource_fingerprint: ResourceFingerprint::new("postgres://host:5432/db/public.orders"),
            environment: "dev".into(),
            updated_at_ms: 1_700_000_000_000,
        };
        let b2 = StoredResourceBinding {
            pipeline_id: "p1".into(),
            node_id: "sink".into(),
            direction: BindingDirection::Sink,
            resource_fingerprint: ResourceFingerprint::new("file:///data/output.csv"),
            environment: "dev".into(),
            updated_at_ms: 1_700_000_000_000,
        };
        store
            .save_bindings("p1", "dev", &[b1.clone(), b2.clone()])
            .unwrap();

        let loaded = store.load_bindings("p1", "dev").unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].node_id, "sink"); // sorted by node_id
        assert_eq!(loaded[1].node_id, "src");

        // Environment isolation.
        assert!(store.load_bindings("p1", "prod").is_ok());
        assert!(store.load_bindings("p1", "prod").unwrap().is_empty());

        // all_bindings returns everything in the environment.
        let all = store.all_bindings("dev").unwrap();
        assert_eq!(all.len(), 2);

        // Atomic replace: save new set, old ones disappear.
        let b3 = StoredResourceBinding {
            pipeline_id: "p1".into(),
            node_id: "new_sink".into(),
            direction: BindingDirection::Sink,
            resource_fingerprint: ResourceFingerprint::new("s3://bucket/path/"),
            environment: "dev".into(),
            updated_at_ms: 1_700_000_100_000,
        };
        store.save_bindings("p1", "dev", &[b3]).unwrap();
        let loaded = store.load_bindings("p1", "dev").unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].node_id, "new_sink");

        // Delete.
        store.delete_bindings("p1").unwrap();
        assert!(store.load_bindings("p1", "dev").unwrap().is_empty());
    }

    #[test]
    fn lineage_observations_round_trip() {
        let store = SqliteRunStore::open_in_memory().unwrap();

        let obs1 = LineageObservation {
            pipeline_id: "p1".into(),
            node_id: "src".into(),
            run_id: "run-1".into(),
            direction: BindingDirection::Source,
            resource_fingerprint: ResourceFingerprint::new("postgres://host:5432/db/public.orders"),
            environment: "dev".into(),
            observed_at_ms: 1_700_000_000_000,
        };
        let obs2 = LineageObservation {
            pipeline_id: "p2".into(),
            node_id: "sink".into(),
            run_id: "run-2".into(),
            direction: BindingDirection::Sink,
            resource_fingerprint: ResourceFingerprint::new("postgres://host:5432/db/public.orders"),
            environment: "dev".into(),
            observed_at_ms: 1_700_000_100_000,
        };

        store.record_observation(&obs1).unwrap();
        store.record_observation(&obs2).unwrap();

        // Query with since_ms before both observations.
        let all = store.query_observations("dev", 0).unwrap();
        assert_eq!(all.len(), 2);
        // Ordered by observed_at DESC.
        assert_eq!(all[0].run_id, "run-2");
        assert_eq!(all[1].run_id, "run-1");

        // Query with since_ms between observations.
        let recent = store.query_observations("dev", 1_700_000_050_000).unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].run_id, "run-2");

        // Environment isolation.
        let prod = store.query_observations("prod", 0).unwrap();
        assert!(prod.is_empty());
    }

    #[test]
    fn lineage_retention_enforcement() {
        let store = SqliteRunStore::open_in_memory().unwrap();

        let old_obs = LineageObservation {
            pipeline_id: "p1".into(),
            node_id: "src".into(),
            run_id: "old-run".into(),
            direction: BindingDirection::Source,
            resource_fingerprint: ResourceFingerprint::new("file:///data/old.csv"),
            environment: "dev".into(),
            observed_at_ms: 1_000_000_000_000, // old
        };
        let new_obs = LineageObservation {
            pipeline_id: "p1".into(),
            node_id: "src".into(),
            run_id: "new-run".into(),
            direction: BindingDirection::Source,
            resource_fingerprint: ResourceFingerprint::new("file:///data/new.csv"),
            environment: "dev".into(),
            observed_at_ms: 1_700_000_000_000, // recent
        };

        store.record_observation(&old_obs).unwrap();
        store.record_observation(&new_obs).unwrap();

        // Enforce retention: delete observations older than 1_500_000_000_000.
        let deleted = store.enforce_retention(1_500_000_000_000).unwrap();
        assert_eq!(deleted, 1);

        let remaining = store.query_observations("dev", 0).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].run_id, "new-run");
    }
}
