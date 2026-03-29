// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Hybrid SQLite + filesystem storage for pipeline definitions.
//!
//! Pipeline metadata (ID, name, timestamps, run stats) lives in SQLite.
//! Pipeline definition JSON lives on the filesystem as `{id}.json` files
//! inside a configurable pipelines directory.

use crate::pipeline::Pipeline;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Unique identifier for a stored pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PipelineId(pub Uuid);

impl PipelineId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for PipelineId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for PipelineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for PipelineId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

/// A pipeline definition with storage metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRecord {
    pub id: PipelineId,
    pub pipeline: Pipeline,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub last_run_at: Option<SystemTime>,
    pub run_count: u32,
}

/// Errors from pipeline storage operations.
#[derive(Debug, thiserror::Error)]
pub enum PipelineStoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),

    #[error("pipeline not found: {0}")]
    NotFound(String),

    #[error("pipeline name `{0}` already exists")]
    NameConflict(String),

    #[error("invalid UUID: {0}")]
    InvalidId(String),
}

/// Persists pipeline definitions using SQLite (metadata) and the filesystem
/// (JSON definition files in a configurable directory).
pub struct PipelineStore {
    conn: Mutex<Connection>,
    pipelines_dir: PathBuf,
}

impl PipelineStore {
    /// Open (or create) a pipeline store.
    ///
    /// `db_path` — SQLite database file for metadata.
    /// `pipelines_dir` — directory where `{pipeline_id}.json` files are written.
    pub fn open(db_path: &Path, pipelines_dir: &Path) -> Result<Self, PipelineStoreError> {
        std::fs::create_dir_all(pipelines_dir)?;
        let conn = Connection::open(db_path)?;
        let store = Self {
            conn: Mutex::new(conn),
            pipelines_dir: pipelines_dir.to_path_buf(),
        };
        store.init_schema()?;
        Ok(store)
    }

    /// Open an in-memory pipeline store backed by a temporary directory.
    /// Useful for tests — caller must keep the `pipelines_dir` alive for the
    /// duration of the test.
    pub fn open_in_memory(pipelines_dir: &Path) -> Result<Self, PipelineStoreError> {
        std::fs::create_dir_all(pipelines_dir)?;
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn: Mutex::new(conn),
            pipelines_dir: pipelines_dir.to_path_buf(),
        };
        store.init_schema()?;
        Ok(store)
    }

    /// Return the directory where pipeline JSON files are stored.
    pub fn pipelines_dir(&self) -> &Path {
        &self.pipelines_dir
    }

    fn init_schema(&self) -> Result<(), PipelineStoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS pipelines (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL UNIQUE,
                created_at  INTEGER NOT NULL,
                updated_at  INTEGER NOT NULL,
                last_run_at INTEGER,
                run_count   INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_pipelines_name
                ON pipelines (name);",
        )?;
        Ok(())
    }

    /// Create a new pipeline. Returns the created record.
    pub fn create(&self, pipeline: Pipeline) -> Result<PipelineRecord, PipelineStoreError> {
        let conn = self.conn.lock().unwrap();

        // Check for name conflict.
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pipelines WHERE name = ?1)",
            params![pipeline.name],
            |row| row.get(0),
        )?;
        if exists {
            return Err(PipelineStoreError::NameConflict(pipeline.name.clone()));
        }

        let now = SystemTime::now();
        let id = PipelineId::new();

        // Write definition JSON to filesystem.
        let json = serde_json::to_string_pretty(&pipeline)?;
        std::fs::write(self.json_path(&id), &json)?;

        conn.execute(
            "INSERT INTO pipelines (id, name, created_at, updated_at, run_count)
             VALUES (?1, ?2, ?3, ?4, 0)",
            params![
                id.0.to_string(),
                pipeline.name,
                system_time_to_ms(now),
                system_time_to_ms(now),
            ],
        )?;

        Ok(PipelineRecord {
            id,
            pipeline,
            created_at: now,
            updated_at: now,
            last_run_at: None,
            run_count: 0,
        })
    }

    /// Get a pipeline by name.
    pub fn get_by_name(&self, name: &str) -> Result<Option<PipelineRecord>, PipelineStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, created_at, updated_at, last_run_at, run_count
             FROM pipelines WHERE name = ?1",
        )?;
        let mut rows = stmt.query(params![name])?;
        match rows.next()? {
            Some(row) => {
                let meta = row_to_metadata(row)?;
                drop(rows);
                drop(stmt);
                drop(conn);
                let pipeline = self.read_definition(&meta.id)?;
                Ok(Some(PipelineRecord {
                    id: meta.id,
                    pipeline,
                    created_at: meta.created_at,
                    updated_at: meta.updated_at,
                    last_run_at: meta.last_run_at,
                    run_count: meta.run_count,
                }))
            }
            None => Ok(None),
        }
    }

    /// Get a pipeline by ID.
    pub fn get(&self, id: &PipelineId) -> Result<Option<PipelineRecord>, PipelineStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, created_at, updated_at, last_run_at, run_count
             FROM pipelines WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id.0.to_string()])?;
        match rows.next()? {
            Some(row) => {
                let meta = row_to_metadata(row)?;
                let pipeline = self.read_definition(&meta.id)?;
                Ok(Some(PipelineRecord {
                    id: meta.id,
                    pipeline,
                    created_at: meta.created_at,
                    updated_at: meta.updated_at,
                    last_run_at: meta.last_run_at,
                    run_count: meta.run_count,
                }))
            }
            None => Ok(None),
        }
    }

    /// List all pipelines, ordered by name.
    pub fn list(&self, limit: u32, offset: u32) -> Result<Vec<PipelineRecord>, PipelineStoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, created_at, updated_at, last_run_at, run_count
             FROM pipelines
             ORDER BY name ASC
             LIMIT ?1 OFFSET ?2",
        )?;
        let mut rows = stmt.query(params![limit, offset])?;
        let mut metas = Vec::new();
        while let Some(row) = rows.next()? {
            metas.push(row_to_metadata(row)?);
        }
        drop(rows);
        drop(stmt);
        drop(conn);

        let mut records = Vec::with_capacity(metas.len());
        for meta in metas {
            let pipeline = self.read_definition(&meta.id)?;
            records.push(PipelineRecord {
                id: meta.id,
                pipeline,
                created_at: meta.created_at,
                updated_at: meta.updated_at,
                last_run_at: meta.last_run_at,
                run_count: meta.run_count,
            });
        }
        Ok(records)
    }

    /// Get the total count of pipelines.
    pub fn count(&self) -> Result<u32, PipelineStoreError> {
        let conn = self.conn.lock().unwrap();
        let count: u32 = conn.query_row("SELECT COUNT(*) FROM pipelines", [], |row| row.get(0))?;
        Ok(count)
    }

    /// Update an existing pipeline. Returns the updated record.
    pub fn update(
        &self,
        id: &PipelineId,
        pipeline: Pipeline,
    ) -> Result<PipelineRecord, PipelineStoreError> {
        let conn = self.conn.lock().unwrap();

        // Check if a different pipeline already has this name.
        let conflict: Option<String> = conn
            .query_row(
                "SELECT id FROM pipelines WHERE name = ?1 AND id != ?2",
                params![pipeline.name, id.0.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if conflict.is_some() {
            return Err(PipelineStoreError::NameConflict(pipeline.name.clone()));
        }

        let now = SystemTime::now();

        // Write updated definition to filesystem.
        let json = serde_json::to_string_pretty(&pipeline)?;
        std::fs::write(self.json_path(id), &json)?;

        let rows = conn.execute(
            "UPDATE pipelines SET name = ?1, updated_at = ?2 WHERE id = ?3",
            params![pipeline.name, system_time_to_ms(now), id.0.to_string()],
        )?;
        if rows == 0 {
            return Err(PipelineStoreError::NotFound(id.to_string()));
        }

        // Re-read to get full metadata.
        drop(conn);
        self.get(id)?
            .ok_or_else(|| PipelineStoreError::NotFound(id.to_string()))
    }

    /// Delete a pipeline by ID. Removes both the metadata row and the JSON file.
    pub fn delete(&self, id: &PipelineId) -> Result<(), PipelineStoreError> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "DELETE FROM pipelines WHERE id = ?1",
            params![id.0.to_string()],
        )?;
        if rows == 0 {
            return Err(PipelineStoreError::NotFound(id.to_string()));
        }
        drop(conn);

        // Remove the JSON file (ignore error if already missing).
        let path = self.json_path(id);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Record that a pipeline was executed. Updates `last_run_at` to now and
    /// increments `run_count`.
    pub fn record_run(&self, id: &PipelineId) -> Result<(), PipelineStoreError> {
        let conn = self.conn.lock().unwrap();
        let now = system_time_to_ms(SystemTime::now());
        let rows = conn.execute(
            "UPDATE pipelines SET last_run_at = ?1, run_count = run_count + 1 WHERE id = ?2",
            params![now, id.0.to_string()],
        )?;
        if rows == 0 {
            return Err(PipelineStoreError::NotFound(id.to_string()));
        }
        Ok(())
    }

    /// Filesystem path for a pipeline's JSON definition file.
    fn json_path(&self, id: &PipelineId) -> PathBuf {
        self.pipelines_dir.join(format!("{}.json", id.0))
    }

    /// Read a pipeline definition from its JSON file on disk.
    fn read_definition(&self, id: &PipelineId) -> Result<Pipeline, PipelineStoreError> {
        let path = self.json_path(id);
        let json = std::fs::read_to_string(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                PipelineStoreError::NotFound(format!(
                    "definition file missing for pipeline {id}"
                ))
            } else {
                PipelineStoreError::Io(e)
            }
        })?;
        let pipeline: Pipeline = serde_json::from_str(&json)?;
        Ok(pipeline)
    }
}

/// Intermediate metadata from a SQLite row (before reading the JSON file).
struct PipelineMetadata {
    id: PipelineId,
    created_at: SystemTime,
    updated_at: SystemTime,
    last_run_at: Option<SystemTime>,
    run_count: u32,
}

fn row_to_metadata(row: &rusqlite::Row<'_>) -> Result<PipelineMetadata, PipelineStoreError> {
    let id_str: String = row.get(0)?;
    let created_ms: i64 = row.get(2)?;
    let updated_ms: i64 = row.get(3)?;
    let last_run_ms: Option<i64> = row.get(4)?;
    let run_count: u32 = row.get(5)?;

    let id = Uuid::parse_str(&id_str).map_err(|e| PipelineStoreError::InvalidId(format!("{e}")))?;

    Ok(PipelineMetadata {
        id: PipelineId(id),
        created_at: ms_to_system_time(created_ms),
        updated_at: ms_to_system_time(updated_ms),
        last_run_at: last_run_ms.map(ms_to_system_time),
        run_count,
    })
}

fn system_time_to_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn ms_to_system_time(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms as u64)
}

/// Extension trait to add `optional()` to `rusqlite::Result`.
trait OptionalExt<T> {
    fn optional(self) -> Result<Option<T>, rusqlite::Error>;
}

impl<T> OptionalExt<T> for Result<T, rusqlite::Error> {
    fn optional(self) -> Result<Option<T>, rusqlite::Error> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Pipeline;

    fn test_pipeline(name: &str) -> Pipeline {
        Pipeline {
            name: name.to_string(),
            version: 1,
            default_environment: "dev".to_string(),
            variables: Default::default(),
            environment_overrides: Default::default(),
            sample_config: None,
            nodes: vec![],
            edges: vec![],
        }
    }

    #[test]
    fn create_and_get() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PipelineStore::open_in_memory(tmp.path()).unwrap();
        let record = store.create(test_pipeline("test")).unwrap();
        assert_eq!(record.pipeline.name, "test");
        assert_eq!(record.run_count, 0);
        assert!(record.last_run_at.is_none());

        let fetched = store.get(&record.id).unwrap().unwrap();
        assert_eq!(fetched.id, record.id);
        assert_eq!(fetched.pipeline.name, "test");

        // Verify JSON file exists on disk.
        let json_path = tmp.path().join(format!("{}.json", record.id.0));
        assert!(json_path.exists());
    }

    #[test]
    fn get_by_name_found() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PipelineStore::open_in_memory(tmp.path()).unwrap();
        let record = store.create(test_pipeline("lookup")).unwrap();

        let found = store.get_by_name("lookup").unwrap().unwrap();
        assert_eq!(found.id, record.id);
        assert_eq!(found.pipeline.name, "lookup");
    }

    #[test]
    fn get_by_name_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PipelineStore::open_in_memory(tmp.path()).unwrap();
        assert!(store.get_by_name("nope").unwrap().is_none());
    }

    #[test]
    fn name_conflict() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PipelineStore::open_in_memory(tmp.path()).unwrap();
        store.create(test_pipeline("dup")).unwrap();
        let err = store.create(test_pipeline("dup")).unwrap_err();
        assert!(matches!(err, PipelineStoreError::NameConflict(_)));
    }

    #[test]
    fn list_and_count() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PipelineStore::open_in_memory(tmp.path()).unwrap();
        store.create(test_pipeline("b")).unwrap();
        store.create(test_pipeline("a")).unwrap();

        let all = store.list(100, 0).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].pipeline.name, "a"); // sorted by name
        assert_eq!(all[1].pipeline.name, "b");

        assert_eq!(store.count().unwrap(), 2);
    }

    #[test]
    fn update_pipeline() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PipelineStore::open_in_memory(tmp.path()).unwrap();
        let record = store.create(test_pipeline("old")).unwrap();

        let updated = store.update(&record.id, test_pipeline("new")).unwrap();
        assert_eq!(updated.pipeline.name, "new");
        assert_eq!(updated.id, record.id);

        // Old name should no longer exist.
        let all = store.list(100, 0).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].pipeline.name, "new");
    }

    #[test]
    fn delete_pipeline() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PipelineStore::open_in_memory(tmp.path()).unwrap();
        let record = store.create(test_pipeline("doomed")).unwrap();
        let json_path = tmp.path().join(format!("{}.json", record.id.0));
        assert!(json_path.exists());

        store.delete(&record.id).unwrap();
        assert!(store.get(&record.id).unwrap().is_none());
        assert!(!json_path.exists());
    }

    #[test]
    fn delete_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PipelineStore::open_in_memory(tmp.path()).unwrap();
        let err = store.delete(&PipelineId::new()).unwrap_err();
        assert!(matches!(err, PipelineStoreError::NotFound(_)));
    }

    #[test]
    fn pagination() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PipelineStore::open_in_memory(tmp.path()).unwrap();
        for i in 0..5 {
            store.create(test_pipeline(&format!("p{i}"))).unwrap();
        }

        let page1 = store.list(2, 0).unwrap();
        assert_eq!(page1.len(), 2);
        let page2 = store.list(2, 2).unwrap();
        assert_eq!(page2.len(), 2);
        let page3 = store.list(2, 4).unwrap();
        assert_eq!(page3.len(), 1);
    }

    #[test]
    fn record_run_updates_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PipelineStore::open_in_memory(tmp.path()).unwrap();
        let record = store.create(test_pipeline("runner")).unwrap();

        store.record_run(&record.id).unwrap();
        let after_one = store.get(&record.id).unwrap().unwrap();
        assert_eq!(after_one.run_count, 1);
        assert!(after_one.last_run_at.is_some());

        store.record_run(&record.id).unwrap();
        let after_two = store.get(&record.id).unwrap().unwrap();
        assert_eq!(after_two.run_count, 2);
    }
}
