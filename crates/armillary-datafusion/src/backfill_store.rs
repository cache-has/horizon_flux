// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQLite-backed storage for backfill metadata (planning doc 33).

use crate::error::BackfillStoreError;
use crate::storage::BackfillStorage;
use armillary_engine::backfill::{
    Backfill, BackfillId, BackfillIteration, BackfillProgress, BackfillStatus, IterationStatus,
    RangeDefinition,
};
use rusqlite::{Connection, params};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

/// Persists backfill metadata in embedded SQLite.
pub struct SqliteBackfillStore {
    conn: Mutex<Connection>,
}

impl SqliteBackfillStore {
    /// Open (or create) a backfill store at the given file path.
    pub fn open(path: &Path) -> Result<Self, BackfillStoreError> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        Ok(store)
    }

    /// Open an in-memory store (useful for tests).
    pub fn open_in_memory() -> Result<Self, BackfillStoreError> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<(), BackfillStoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS backfills (
                id                    TEXT PRIMARY KEY,
                pipeline_id           TEXT NOT NULL,
                environment           TEXT NOT NULL,
                range_definition_json TEXT NOT NULL,
                concurrency           INTEGER NOT NULL DEFAULT 1,
                fail_fast             INTEGER NOT NULL DEFAULT 0,
                full_refresh          INTEGER NOT NULL DEFAULT 1,
                status                TEXT NOT NULL,
                created_at            TEXT NOT NULL,
                started_at            TEXT,
                completed_at          TEXT,
                created_by            TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_backfills_pipeline
                ON backfills (pipeline_id, created_at DESC);

            CREATE INDEX IF NOT EXISTS idx_backfills_status
                ON backfills (status);

            CREATE TABLE IF NOT EXISTS backfill_iterations (
                backfill_id     TEXT NOT NULL REFERENCES backfills(id) ON DELETE CASCADE,
                iteration_index INTEGER NOT NULL,
                iteration_key   TEXT NOT NULL,
                variables_json  TEXT NOT NULL,
                status          TEXT NOT NULL,
                run_id          TEXT,
                error           TEXT,
                started_at      TEXT,
                completed_at    TEXT,
                PRIMARY KEY (backfill_id, iteration_index)
            );

            CREATE INDEX IF NOT EXISTS idx_bi_key
                ON backfill_iterations (backfill_id, iteration_key);

            CREATE INDEX IF NOT EXISTS idx_bi_status
                ON backfill_iterations (backfill_id, status);",
        )?;
        Ok(())
    }
}

impl BackfillStorage for SqliteBackfillStore {
    fn create_backfill(&self, backfill: &Backfill) -> Result<(), BackfillStoreError> {
        let conn = self.conn.lock().unwrap();
        let range_json = serde_json::to_string(&backfill.range_definition)
            .map_err(|e| BackfillStoreError::Database(e.to_string()))?;
        conn.execute(
            "INSERT INTO backfills (id, pipeline_id, environment, range_definition_json,
                concurrency, fail_fast, full_refresh, status, created_at, started_at,
                completed_at, created_by)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                backfill.id.0,
                backfill.pipeline_id,
                backfill.environment,
                range_json,
                backfill.concurrency,
                backfill.fail_fast as i32,
                backfill.full_refresh as i32,
                backfill.status.as_str(),
                backfill.created_at,
                backfill.started_at,
                backfill.completed_at,
                backfill.created_by,
            ],
        )?;
        Ok(())
    }

    fn create_iterations(
        &self,
        iterations: &[BackfillIteration],
    ) -> Result<(), BackfillStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "INSERT INTO backfill_iterations (backfill_id, iteration_index, iteration_key,
                variables_json, status, run_id, error, started_at, completed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )?;
        for iter in iterations {
            let vars_json = serde_json::to_string(&iter.variables)
                .map_err(|e| BackfillStoreError::Database(e.to_string()))?;
            stmt.execute(params![
                iter.backfill_id.0,
                iter.iteration_index,
                iter.iteration_key,
                vars_json,
                iter.status.as_str(),
                iter.run_id,
                iter.error,
                iter.started_at,
                iter.completed_at,
            ])?;
        }
        Ok(())
    }

    fn get_backfill(&self, id: &BackfillId) -> Result<Option<Backfill>, BackfillStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, pipeline_id, environment, range_definition_json,
                    concurrency, fail_fast, full_refresh, status,
                    created_at, started_at, completed_at, created_by
             FROM backfills WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id.0])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_backfill(row)?)),
            None => Ok(None),
        }
    }

    fn list_backfills(
        &self,
        pipeline_id: Option<&str>,
        status: Option<BackfillStatus>,
        limit: u32,
    ) -> Result<Vec<Backfill>, BackfillStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut sql = String::from(
            "SELECT id, pipeline_id, environment, range_definition_json,
                    concurrency, fail_fast, full_refresh, status,
                    created_at, started_at, completed_at, created_by
             FROM backfills WHERE 1=1",
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(pid) = pipeline_id {
            sql.push_str(" AND pipeline_id = ?");
            param_values.push(Box::new(pid.to_string()));
        }
        if let Some(st) = status {
            sql.push_str(" AND status = ?");
            param_values.push(Box::new(st.as_str().to_string()));
        }
        sql.push_str(" ORDER BY created_at DESC LIMIT ?");
        param_values.push(Box::new(limit));

        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();
        let mut rows = stmt.query(params.as_slice())?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            result.push(row_to_backfill(row)?);
        }
        Ok(result)
    }

    fn list_iterations(
        &self,
        backfill_id: &BackfillId,
    ) -> Result<Vec<BackfillIteration>, BackfillStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT backfill_id, iteration_index, iteration_key, variables_json,
                    status, run_id, error, started_at, completed_at
             FROM backfill_iterations
             WHERE backfill_id = ?1
             ORDER BY iteration_index ASC",
        )?;
        let mut rows = stmt.query(params![backfill_id.0])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            result.push(row_to_iteration(row)?);
        }
        Ok(result)
    }

    fn update_backfill_status(
        &self,
        id: &BackfillId,
        status: BackfillStatus,
        started_at: Option<&str>,
        completed_at: Option<&str>,
    ) -> Result<(), BackfillStoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE backfills SET status = ?1,
                started_at = COALESCE(?2, started_at),
                completed_at = COALESCE(?3, completed_at)
             WHERE id = ?4",
            params![status.as_str(), started_at, completed_at, id.0],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn update_iteration(
        &self,
        backfill_id: &BackfillId,
        iteration_index: u32,
        status: IterationStatus,
        run_id: Option<&str>,
        error: Option<&str>,
        started_at: Option<&str>,
        completed_at: Option<&str>,
    ) -> Result<(), BackfillStoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE backfill_iterations SET
                status = ?1,
                run_id = COALESCE(?2, run_id),
                error = ?3,
                started_at = COALESCE(?4, started_at),
                completed_at = COALESCE(?5, completed_at)
             WHERE backfill_id = ?6 AND iteration_index = ?7",
            params![
                status.as_str(),
                run_id,
                error,
                started_at,
                completed_at,
                backfill_id.0,
                iteration_index,
            ],
        )?;
        Ok(())
    }

    fn get_progress(
        &self,
        backfill_id: &BackfillId,
    ) -> Result<BackfillProgress, BackfillStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT status, COUNT(*) as cnt
             FROM backfill_iterations
             WHERE backfill_id = ?1
             GROUP BY status",
        )?;
        let mut rows = stmt.query(params![backfill_id.0])?;
        let mut progress = BackfillProgress {
            total: 0,
            succeeded: 0,
            failed: 0,
            running: 0,
            pending: 0,
            skipped: 0,
        };
        while let Some(row) = rows.next()? {
            let status_str: String = row.get(0)?;
            let count: u32 = row.get(1)?;
            progress.total += count;
            match status_str.as_str() {
                "succeeded" => progress.succeeded = count,
                "failed" => progress.failed = count,
                "running" => progress.running = count,
                "pending" => progress.pending = count,
                "skipped" => progress.skipped = count,
                _ => {}
            }
        }
        Ok(progress)
    }

    fn delete_backfill(&self, id: &BackfillId) -> Result<bool, BackfillStoreError> {
        let conn = self.conn.lock().unwrap();
        // Iterations deleted by CASCADE.
        let rows_affected = conn.execute("DELETE FROM backfills WHERE id = ?1", params![id.0])?;
        Ok(rows_affected > 0)
    }
}

fn row_to_backfill(row: &rusqlite::Row<'_>) -> Result<Backfill, BackfillStoreError> {
    let range_json: String = row.get(3)?;
    let range_definition: RangeDefinition = serde_json::from_str(&range_json)
        .map_err(|e| BackfillStoreError::Database(format!("invalid range JSON: {e}")))?;
    let status_str: String = row.get(7)?;
    let status = BackfillStatus::parse(&status_str)
        .ok_or_else(|| BackfillStoreError::InvalidStatus(status_str))?;
    let fail_fast_int: i32 = row.get(5)?;
    let full_refresh_int: i32 = row.get(6)?;

    Ok(Backfill {
        id: BackfillId(row.get(0)?),
        pipeline_id: row.get(1)?,
        environment: row.get(2)?,
        range_definition,
        concurrency: row.get(4)?,
        fail_fast: fail_fast_int != 0,
        full_refresh: full_refresh_int != 0,
        status,
        created_at: row.get(8)?,
        started_at: row.get(9)?,
        completed_at: row.get(10)?,
        created_by: row.get(11)?,
    })
}

fn row_to_iteration(row: &rusqlite::Row<'_>) -> Result<BackfillIteration, BackfillStoreError> {
    let vars_json: String = row.get(3)?;
    let variables: HashMap<String, Value> = serde_json::from_str(&vars_json)
        .map_err(|e| BackfillStoreError::Database(format!("invalid variables JSON: {e}")))?;
    let status_str: String = row.get(4)?;
    let status = IterationStatus::parse(&status_str)
        .ok_or_else(|| BackfillStoreError::InvalidStatus(status_str))?;

    Ok(BackfillIteration {
        backfill_id: BackfillId(row.get(0)?),
        iteration_index: row.get(1)?,
        iteration_key: row.get(2)?,
        variables,
        status,
        run_id: row.get(5)?,
        error: row.get(6)?,
        started_at: row.get(7)?,
        completed_at: row.get(8)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_backfill(id: &str) -> Backfill {
        Backfill {
            id: BackfillId(id.into()),
            pipeline_id: "test-pipe".into(),
            environment: "dev".into(),
            range_definition: RangeDefinition::List {
                values: vec!["a".into(), "b".into()],
                variable_mapping: HashMap::from([("v".into(), "$iteration.value".into())]),
            },
            concurrency: 2,
            fail_fast: false,
            full_refresh: true,
            status: BackfillStatus::Pending,
            created_at: "2024-01-01T00:00:00Z".into(),
            started_at: None,
            completed_at: None,
            created_by: Some("test".into()),
        }
    }

    fn make_iterations(backfill_id: &str) -> Vec<BackfillIteration> {
        vec![
            BackfillIteration {
                backfill_id: BackfillId(backfill_id.into()),
                iteration_index: 0,
                iteration_key: "a".into(),
                variables: HashMap::from([("v".into(), Value::String("a".into()))]),
                status: IterationStatus::Pending,
                run_id: None,
                error: None,
                started_at: None,
                completed_at: None,
            },
            BackfillIteration {
                backfill_id: BackfillId(backfill_id.into()),
                iteration_index: 1,
                iteration_key: "b".into(),
                variables: HashMap::from([("v".into(), Value::String("b".into()))]),
                status: IterationStatus::Pending,
                run_id: None,
                error: None,
                started_at: None,
                completed_at: None,
            },
        ]
    }

    #[test]
    fn create_and_get_backfill() {
        let store = SqliteBackfillStore::open_in_memory().unwrap();
        let bf = make_backfill("bf1");
        store.create_backfill(&bf).unwrap();
        let loaded = store
            .get_backfill(&BackfillId("bf1".into()))
            .unwrap()
            .unwrap();
        assert_eq!(loaded.pipeline_id, "test-pipe");
        assert_eq!(loaded.concurrency, 2);
        assert!(loaded.full_refresh);
    }

    #[test]
    fn create_and_list_iterations() {
        let store = SqliteBackfillStore::open_in_memory().unwrap();
        let bf = make_backfill("bf2");
        store.create_backfill(&bf).unwrap();
        store.create_iterations(&make_iterations("bf2")).unwrap();
        let iters = store.list_iterations(&BackfillId("bf2".into())).unwrap();
        assert_eq!(iters.len(), 2);
        assert_eq!(iters[0].iteration_key, "a");
        assert_eq!(iters[1].iteration_key, "b");
    }

    #[test]
    fn update_iteration_status() {
        let store = SqliteBackfillStore::open_in_memory().unwrap();
        store.create_backfill(&make_backfill("bf3")).unwrap();
        store.create_iterations(&make_iterations("bf3")).unwrap();
        store
            .update_iteration(
                &BackfillId("bf3".into()),
                0,
                IterationStatus::Succeeded,
                Some("run-abc"),
                None,
                Some("2024-01-01T01:00:00Z"),
                Some("2024-01-01T01:05:00Z"),
            )
            .unwrap();
        let iters = store.list_iterations(&BackfillId("bf3".into())).unwrap();
        assert_eq!(iters[0].status, IterationStatus::Succeeded);
        assert_eq!(iters[0].run_id, Some("run-abc".into()));
    }

    #[test]
    fn progress_aggregation() {
        let store = SqliteBackfillStore::open_in_memory().unwrap();
        store.create_backfill(&make_backfill("bf4")).unwrap();
        store.create_iterations(&make_iterations("bf4")).unwrap();
        store
            .update_iteration(
                &BackfillId("bf4".into()),
                0,
                IterationStatus::Succeeded,
                None,
                None,
                None,
                None,
            )
            .unwrap();
        let progress = store.get_progress(&BackfillId("bf4".into())).unwrap();
        assert_eq!(progress.total, 2);
        assert_eq!(progress.succeeded, 1);
        assert_eq!(progress.pending, 1);
    }

    #[test]
    fn delete_backfill_cascades() {
        let store = SqliteBackfillStore::open_in_memory().unwrap();
        store.create_backfill(&make_backfill("bf5")).unwrap();
        store.create_iterations(&make_iterations("bf5")).unwrap();
        assert!(store.delete_backfill(&BackfillId("bf5".into())).unwrap());
        assert!(
            store
                .get_backfill(&BackfillId("bf5".into()))
                .unwrap()
                .is_none()
        );
        let iters = store.list_iterations(&BackfillId("bf5".into())).unwrap();
        assert!(iters.is_empty());
    }

    #[test]
    fn list_backfills_with_filters() {
        let store = SqliteBackfillStore::open_in_memory().unwrap();
        store.create_backfill(&make_backfill("bf6")).unwrap();
        let mut bf2 = make_backfill("bf7");
        bf2.pipeline_id = "other-pipe".into();
        store.create_backfill(&bf2).unwrap();

        let all = store.list_backfills(None, None, 100).unwrap();
        assert_eq!(all.len(), 2);

        let filtered = store.list_backfills(Some("test-pipe"), None, 100).unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id.0, "bf6");
    }
}
