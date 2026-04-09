// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite-backed storage for pipeline execution history.

use crate::error::{IncrementalStateError, RunStoreError};
use crate::incremental_state::{IncrementalSchemaRecord, IncrementalState};
use crate::run::{NodeRunStats, PipelineRun, RunId, RunStatus};
use crate::storage::{IncrementalStateStorage, RunStorage};
use flux_engine::NodeId;
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
                   (pipeline_id, node_id, environment, recorded_at DESC);",
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

        conn.execute(
            "INSERT OR IGNORE INTO pipeline_runs (id, pipeline_name, environment, status, start_time_ms, end_time_ms, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                run.id.0.to_string(),
                run.pipeline_name,
                run.environment,
                run.status.as_str(),
                start_ms,
                end_ms,
                run.error,
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
            "SELECT id, pipeline_name, environment, status, start_time_ms, end_time_ms, error
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
                    "SELECT id, pipeline_name, environment, status, start_time_ms, end_time_ms, error
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
                    "SELECT id, pipeline_name, environment, status, start_time_ms, end_time_ms, error
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
}
