// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! PostgreSQL-backed storage for pipeline definitions.
//!
//! Unlike the SQLite implementation which stores pipeline JSON on the
//! filesystem, this stores the full definition as a `JSONB` column.

use crate::block_on;
use deadpool_postgres::Pool;
use flux_engine::pipeline::Pipeline;
use flux_engine::pipeline_store::{PipelineId, PipelineRecord, PipelineStoreError, PipelineVersion};
use flux_engine::storage::PipelineStorage;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// PostgreSQL-backed pipeline storage.
///
/// Pipeline definitions are stored as `JSONB` columns — no filesystem
/// dependency. Version history is stored as rows in `pipeline_versions`.
pub struct PostgresPipelineStore {
    pool: Pool,
}

impl PostgresPipelineStore {
    /// Create a new store backed by the given connection pool.
    ///
    /// Call [`crate::schema::ensure_schema`] before constructing this to
    /// guarantee the required tables exist.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl PipelineStorage for PostgresPipelineStore {
    fn create(&self, mut pipeline: Pipeline) -> Result<PipelineRecord, PipelineStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;

            // Check for name conflict.
            let row = client
                .query_opt(
                    "SELECT 1 FROM pipelines WHERE name = $1",
                    &[&pipeline.name],
                )
                .await
                .map_err(pg_err)?;
            if row.is_some() {
                return Err(PipelineStoreError::NameConflict(pipeline.name.clone()));
            }

            let now = SystemTime::now();
            let now_ms = system_time_to_ms(now);
            let id = PipelineId::new();

            pipeline.version = 1;

            let definition =
                serde_json::to_value(&pipeline).map_err(PipelineStoreError::Json)?;

            client
                .execute(
                    "INSERT INTO pipelines (id, name, definition, created_at, updated_at, run_count)
                     VALUES ($1, $2, $3, $4, $5, 0)",
                    &[
                        &id.0.to_string(),
                        &pipeline.name,
                        &definition,
                        &now_ms,
                        &now_ms,
                    ],
                )
                .await
                .map_err(pg_err)?;

            // Store initial version snapshot.
            client
                .execute(
                    "INSERT INTO pipeline_versions (pipeline_id, version, saved_at, snapshot)
                     VALUES ($1, $2, $3, $4)",
                    &[&id.0.to_string(), &1i32, &now_ms, &definition],
                )
                .await
                .map_err(pg_err)?;

            Ok(PipelineRecord {
                id,
                pipeline,
                created_at: now,
                updated_at: now,
                last_run_at: None,
                run_count: 0,
            })
        })
    }

    fn get(&self, id: &PipelineId) -> Result<Option<PipelineRecord>, PipelineStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let row = client
                .query_opt(
                    "SELECT id, name, definition, created_at, updated_at, last_run_at, run_count
                     FROM pipelines WHERE id = $1",
                    &[&id.0.to_string()],
                )
                .await
                .map_err(pg_err)?;

            match row {
                Some(row) => Ok(Some(row_to_record(&row)?)),
                None => Ok(None),
            }
        })
    }

    fn get_by_name(&self, name: &str) -> Result<Option<PipelineRecord>, PipelineStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let row = client
                .query_opt(
                    "SELECT id, name, definition, created_at, updated_at, last_run_at, run_count
                     FROM pipelines WHERE name = $1",
                    &[&name],
                )
                .await
                .map_err(pg_err)?;

            match row {
                Some(row) => Ok(Some(row_to_record(&row)?)),
                None => Ok(None),
            }
        })
    }

    fn list(&self, limit: u32, offset: u32) -> Result<Vec<PipelineRecord>, PipelineStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let rows = client
                .query(
                    "SELECT id, name, definition, created_at, updated_at, last_run_at, run_count
                     FROM pipelines
                     ORDER BY name ASC
                     LIMIT $1 OFFSET $2",
                    &[&(limit as i64), &(offset as i64)],
                )
                .await
                .map_err(pg_err)?;

            rows.iter().map(row_to_record).collect()
        })
    }

    fn count(&self) -> Result<u32, PipelineStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let row = client
                .query_one("SELECT COUNT(*)::INTEGER FROM pipelines", &[])
                .await
                .map_err(pg_err)?;
            let count: i32 = row.get(0);
            Ok(count as u32)
        })
    }

    fn update(
        &self,
        id: &PipelineId,
        mut pipeline: Pipeline,
    ) -> Result<PipelineRecord, PipelineStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;

            // Check for name conflict with a different pipeline.
            let conflict = client
                .query_opt(
                    "SELECT id FROM pipelines WHERE name = $1 AND id != $2",
                    &[&pipeline.name, &id.0.to_string()],
                )
                .await
                .map_err(pg_err)?;
            if conflict.is_some() {
                return Err(PipelineStoreError::NameConflict(pipeline.name.clone()));
            }

            // Auto-increment version from history.
            let max_row = client
                .query_one(
                    "SELECT COALESCE(MAX(version), 0) FROM pipeline_versions WHERE pipeline_id = $1",
                    &[&id.0.to_string()],
                )
                .await
                .map_err(pg_err)?;
            let current_max: i32 = max_row.get(0);
            let new_version = if current_max > 0 {
                current_max as u32 + 1
            } else {
                pipeline.version.max(1) + 1
            };
            pipeline.version = new_version;

            let now = SystemTime::now();
            let now_ms = system_time_to_ms(now);
            let definition =
                serde_json::to_value(&pipeline).map_err(PipelineStoreError::Json)?;

            let rows_affected = client
                .execute(
                    "UPDATE pipelines SET name = $1, definition = $2, updated_at = $3 WHERE id = $4",
                    &[&pipeline.name, &definition, &now_ms, &id.0.to_string()],
                )
                .await
                .map_err(pg_err)?;
            if rows_affected == 0 {
                return Err(PipelineStoreError::NotFound(id.to_string()));
            }

            // Store version snapshot.
            client
                .execute(
                    "INSERT INTO pipeline_versions (pipeline_id, version, saved_at, snapshot)
                     VALUES ($1, $2, $3, $4)",
                    &[
                        &id.0.to_string(),
                        &(new_version as i32),
                        &now_ms,
                        &definition,
                    ],
                )
                .await
                .map_err(pg_err)?;

            // Re-read full record.
            drop(client);
            self.get(id)?
                .ok_or_else(|| PipelineStoreError::NotFound(id.to_string()))
        })
    }

    fn delete(&self, id: &PipelineId) -> Result<(), PipelineStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;

            // Version history cascades via FK, but be explicit.
            client
                .execute(
                    "DELETE FROM pipeline_versions WHERE pipeline_id = $1",
                    &[&id.0.to_string()],
                )
                .await
                .map_err(pg_err)?;

            let rows_affected = client
                .execute("DELETE FROM pipelines WHERE id = $1", &[&id.0.to_string()])
                .await
                .map_err(pg_err)?;
            if rows_affected == 0 {
                return Err(PipelineStoreError::NotFound(id.to_string()));
            }
            Ok(())
        })
    }

    fn list_versions(
        &self,
        id: &PipelineId,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<PipelineVersion>, PipelineStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let rows = client
                .query(
                    "SELECT pipeline_id, version, saved_at, snapshot
                     FROM pipeline_versions
                     WHERE pipeline_id = $1
                     ORDER BY version DESC
                     LIMIT $2 OFFSET $3",
                    &[&id.0.to_string(), &(limit as i64), &(offset as i64)],
                )
                .await
                .map_err(pg_err)?;

            rows.iter().map(row_to_version).collect()
        })
    }

    fn get_version(
        &self,
        id: &PipelineId,
        version: u32,
    ) -> Result<Option<PipelineVersion>, PipelineStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let row = client
                .query_opt(
                    "SELECT pipeline_id, version, saved_at, snapshot
                     FROM pipeline_versions
                     WHERE pipeline_id = $1 AND version = $2",
                    &[&id.0.to_string(), &(version as i32)],
                )
                .await
                .map_err(pg_err)?;

            match row {
                Some(row) => Ok(Some(row_to_version(&row)?)),
                None => Ok(None),
            }
        })
    }

    fn count_versions(&self, id: &PipelineId) -> Result<u32, PipelineStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let row = client
                .query_one(
                    "SELECT COUNT(*)::INTEGER FROM pipeline_versions WHERE pipeline_id = $1",
                    &[&id.0.to_string()],
                )
                .await
                .map_err(pg_err)?;
            let count: i32 = row.get(0);
            Ok(count as u32)
        })
    }

    fn record_run(&self, id: &PipelineId) -> Result<(), PipelineStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let now_ms = system_time_to_ms(SystemTime::now());
            let rows_affected = client
                .execute(
                    "UPDATE pipelines SET last_run_at = $1, run_count = run_count + 1 WHERE id = $2",
                    &[&now_ms, &id.0.to_string()],
                )
                .await
                .map_err(pg_err)?;
            if rows_affected == 0 {
                return Err(PipelineStoreError::NotFound(id.to_string()));
            }
            Ok(())
        })
    }
}

fn row_to_record(row: &tokio_postgres::Row) -> Result<PipelineRecord, PipelineStoreError> {
    let id_str: String = row.get(0);
    let definition: serde_json::Value = row.get(2);
    let created_ms: i64 = row.get(3);
    let updated_ms: i64 = row.get(4);
    let last_run_ms: Option<i64> = row.get(5);
    let run_count: i32 = row.get(6);

    let id =
        Uuid::parse_str(&id_str).map_err(|e| PipelineStoreError::InvalidId(format!("{e}")))?;
    let pipeline: Pipeline =
        serde_json::from_value(definition).map_err(PipelineStoreError::Json)?;

    Ok(PipelineRecord {
        id: PipelineId(id),
        pipeline,
        created_at: ms_to_system_time(created_ms),
        updated_at: ms_to_system_time(updated_ms),
        last_run_at: last_run_ms.map(ms_to_system_time),
        run_count: run_count as u32,
    })
}

fn row_to_version(row: &tokio_postgres::Row) -> Result<PipelineVersion, PipelineStoreError> {
    let id_str: String = row.get(0);
    let version: i32 = row.get(1);
    let saved_ms: i64 = row.get(2);
    let snapshot_val: serde_json::Value = row.get(3);

    let id =
        Uuid::parse_str(&id_str).map_err(|e| PipelineStoreError::InvalidId(format!("{e}")))?;
    let snapshot: Pipeline =
        serde_json::from_value(snapshot_val).map_err(PipelineStoreError::Json)?;

    Ok(PipelineVersion {
        pipeline_id: PipelineId(id),
        version: version as u32,
        saved_at: ms_to_system_time(saved_ms),
        snapshot,
    })
}

fn pg_err(e: impl std::fmt::Display) -> PipelineStoreError {
    PipelineStoreError::Database(e.to_string())
}

fn system_time_to_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn ms_to_system_time(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms as u64)
}
