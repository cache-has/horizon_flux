// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite-backed storage for pipeline execution history.

use crate::error::RunStoreError;
use crate::run::{NodeRunStats, PipelineRun, RunId, RunStatus};
use crate::storage::RunStorage;
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
                PRIMARY KEY (run_id, node_id)
            );",
        )?;
        Ok(())
    }

    fn load_node_stats_locked(
        &self,
        conn: &Connection,
        run_id: &RunId,
    ) -> Result<Vec<NodeRunStats>, RunStoreError> {
        let mut stmt = conn.prepare(
            "SELECT node_id, start_time_ms, end_time_ms, rows_in, rows_out, error
             FROM node_run_stats WHERE run_id = ?1
             ORDER BY start_time_ms ASC",
        )?;
        let mut rows = stmt.query(params![run_id.0.to_string()])?;
        let mut stats = Vec::new();
        while let Some(row) = rows.next()? {
            stats.push(NodeRunStats {
                node_id: NodeId::new(row.get::<_, String>(0)?),
                start_time: ms_to_system_time(row.get::<_, i64>(1)?),
                end_time: ms_to_system_time(row.get::<_, i64>(2)?),
                rows_in: row.get::<_, i64>(3)? as u64,
                rows_out: row.get::<_, i64>(4)? as u64,
                error: row.get(5)?,
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
            conn.execute(
                "INSERT OR IGNORE INTO node_run_stats (run_id, node_id, start_time_ms, end_time_ms, rows_in, rows_out, error)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    run.id.0.to_string(),
                    stats.node_id.0,
                    system_time_to_ms(stats.start_time),
                    system_time_to_ms(stats.end_time),
                    stats.rows_in as i64,
                    stats.rows_out as i64,
                    stats.error,
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
        conn.execute(
            "INSERT OR REPLACE INTO node_run_stats
                (run_id, node_id, start_time_ms, end_time_ms, rows_in, rows_out, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                run_id.0.to_string(),
                stats.node_id.0,
                system_time_to_ms(stats.start_time),
                system_time_to_ms(stats.end_time),
                stats.rows_in as i64,
                stats.rows_out as i64,
                stats.error,
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
