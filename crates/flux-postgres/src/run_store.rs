// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! PostgreSQL-backed storage for pipeline execution history.

use crate::block_on;
use deadpool_postgres::Pool;
use flux_datafusion::error::{IncrementalStateError, LineageStoreError, RunStoreError};
use flux_datafusion::incremental_state::{IncrementalSchemaRecord, IncrementalState};
use flux_datafusion::run::{NodeRunStats, PipelineRun, RunId, RunStatus, TestResultSummary};
use flux_datafusion::storage::{
    IncrementalStateStorage, LineageObservation, LineageStorage, RunStorage, StoredResourceBinding,
};
use flux_engine::NodeId;
use flux_engine::lineage::{BindingDirection, ResourceFingerprint};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// PostgreSQL-backed run history storage.
pub struct PostgresRunStore {
    pool: Pool,
}

impl PostgresRunStore {
    /// Create a new store backed by the given connection pool.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl RunStorage for PostgresRunStore {
    fn create_run(
        &self,
        pipeline_name: &str,
        environment: &str,
    ) -> Result<PipelineRun, RunStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let run = PipelineRun::new(pipeline_name, environment);
            client
                .execute(
                    "INSERT INTO pipeline_runs (id, pipeline_name, environment, status)
                     VALUES ($1, $2, $3, $4)",
                    &[
                        &run.id.0.to_string(),
                        &run.pipeline_name,
                        &run.environment,
                        &run.status.as_str(),
                    ],
                )
                .await
                .map_err(pg_err)?;
            Ok(run)
        })
    }

    fn set_running(&self, run_id: &RunId, start_time: SystemTime) -> Result<(), RunStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let rows_affected = client
                .execute(
                    "UPDATE pipeline_runs SET status = $1, start_time_ms = $2 WHERE id = $3",
                    &[
                        &RunStatus::Running.as_str(),
                        &system_time_to_ms(start_time),
                        &run_id.0.to_string(),
                    ],
                )
                .await
                .map_err(pg_err)?;
            if rows_affected == 0 {
                return Err(RunStoreError::NotFound(run_id.to_string()));
            }
            Ok(())
        })
    }

    fn finish_run(
        &self,
        run_id: &RunId,
        status: RunStatus,
        end_time: SystemTime,
        error: Option<&str>,
    ) -> Result<(), RunStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let error_owned = error.map(|s| s.to_string());
            let rows_affected = client
                .execute(
                    "UPDATE pipeline_runs SET status = $1, end_time_ms = $2, error = $3 WHERE id = $4",
                    &[
                        &status.as_str(),
                        &system_time_to_ms(end_time),
                        &error_owned as &(dyn tokio_postgres::types::ToSql + Sync),
                        &run_id.0.to_string(),
                    ],
                )
                .await
                .map_err(pg_err)?;
            if rows_affected == 0 {
                return Err(RunStoreError::NotFound(run_id.to_string()));
            }
            Ok(())
        })
    }

    fn save_test_results(
        &self,
        run_id: &RunId,
        results: &[TestResultSummary],
    ) -> Result<(), RunStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let json = serde_json::to_string(results).map_err(|e| {
                RunStoreError::Database(format!("failed to serialize test results: {e}"))
            })?;
            client
                .execute(
                    "UPDATE pipeline_runs SET test_results = $1 WHERE id = $2",
                    &[&json, &run_id.0.to_string()],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        })
    }

    fn save_node_stats(&self, run_id: &RunId, stats: &NodeRunStats) -> Result<(), RunStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let error_owned = stats.error.clone();
            let receipt_json: Option<String> = stats
                .materialization_receipt
                .as_ref()
                .and_then(|r| serde_json::to_string(r).ok());
            client
                .execute(
                    "INSERT INTO node_run_stats
                        (run_id, node_id, start_time_ms, end_time_ms, rows_in, rows_out, error, materialization_receipt)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                     ON CONFLICT (run_id, node_id) DO UPDATE SET
                        start_time_ms = EXCLUDED.start_time_ms,
                        end_time_ms = EXCLUDED.end_time_ms,
                        rows_in = EXCLUDED.rows_in,
                        rows_out = EXCLUDED.rows_out,
                        error = EXCLUDED.error,
                        materialization_receipt = EXCLUDED.materialization_receipt",
                    &[
                        &run_id.0.to_string(),
                        &stats.node_id.0,
                        &system_time_to_ms(stats.start_time),
                        &system_time_to_ms(stats.end_time),
                        &(stats.rows_in as i64),
                        &(stats.rows_out as i64),
                        &error_owned as &(dyn tokio_postgres::types::ToSql + Sync),
                        &receipt_json as &(dyn tokio_postgres::types::ToSql + Sync),
                    ],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        })
    }

    fn get_run(&self, run_id: &RunId) -> Result<Option<PipelineRun>, RunStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;

            let row = client
                .query_opt(
                    "SELECT id, pipeline_name, environment, status, start_time_ms, end_time_ms, error,
                            test_results
                     FROM pipeline_runs WHERE id = $1",
                    &[&run_id.0.to_string()],
                )
                .await
                .map_err(pg_err)?;

            let Some(row) = row else {
                return Ok(None);
            };

            let mut run = row_to_pipeline_run(&row)?;
            run.node_stats = load_node_stats(&client, run_id).await?;
            Ok(Some(run))
        })
    }

    fn list_runs(
        &self,
        pipeline_name: Option<&str>,
        limit: u32,
    ) -> Result<Vec<PipelineRun>, RunStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;

            let rows = match pipeline_name {
                Some(name) => {
                    client
                        .query(
                            "SELECT id, pipeline_name, environment, status, start_time_ms, end_time_ms, error,
                                    test_results
                             FROM pipeline_runs
                             WHERE pipeline_name = $1
                             ORDER BY start_time_ms DESC NULLS LAST
                             LIMIT $2",
                            &[&name, &(limit as i64)],
                        )
                        .await
                        .map_err(pg_err)?
                }
                None => {
                    client
                        .query(
                            "SELECT id, pipeline_name, environment, status, start_time_ms, end_time_ms, error,
                                    test_results
                             FROM pipeline_runs
                             ORDER BY start_time_ms DESC NULLS LAST
                             LIMIT $1",
                            &[&(limit as i64)],
                        )
                        .await
                        .map_err(pg_err)?
                }
            };

            let mut runs = Vec::with_capacity(rows.len());
            for row in &rows {
                let mut run = row_to_pipeline_run(row)?;
                let run_id = run.id.clone();
                run.node_stats = load_node_stats(&client, &run_id).await?;
                runs.push(run);
            }
            Ok(runs)
        })
    }
}

async fn load_node_stats(
    client: &deadpool_postgres::Client,
    run_id: &RunId,
) -> Result<Vec<NodeRunStats>, RunStoreError> {
    let rows = client
        .query(
            "SELECT node_id, start_time_ms, end_time_ms, rows_in, rows_out, error,
                    materialization_receipt
             FROM node_run_stats WHERE run_id = $1
             ORDER BY start_time_ms ASC",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(pg_err)?;

    let mut stats = Vec::with_capacity(rows.len());
    for row in &rows {
        let receipt_json: Option<String> = row.get(6);
        let materialization_receipt = receipt_json
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok());
        stats.push(NodeRunStats {
            node_id: NodeId::new(row.get::<_, String>(0)),
            start_time: ms_to_system_time(row.get::<_, i64>(1)),
            end_time: ms_to_system_time(row.get::<_, i64>(2)),
            rows_in: row.get::<_, i64>(3) as u64,
            rows_out: row.get::<_, i64>(4) as u64,
            error: row.get(5),
            materialization_receipt,
        });
    }
    Ok(stats)
}

fn row_to_pipeline_run(row: &tokio_postgres::Row) -> Result<PipelineRun, RunStoreError> {
    let id_str: String = row.get(0);
    let status_str: String = row.get(3);
    let start_ms: Option<i64> = row.get(4);
    let end_ms: Option<i64> = row.get(5);
    let test_results_json: Option<String> = row.get(7);
    let test_results = test_results_json
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();

    Ok(PipelineRun {
        id: RunId(
            Uuid::parse_str(&id_str)
                .map_err(|e| RunStoreError::InvalidStatus(format!("bad UUID: {e}")))?,
        ),
        pipeline_name: row.get(1),
        environment: row.get(2),
        status: RunStatus::parse(&status_str)
            .ok_or_else(|| RunStoreError::InvalidStatus(status_str))?,
        start_time: start_ms.map(ms_to_system_time),
        end_time: end_ms.map(ms_to_system_time),
        node_stats: Vec::new(),
        error: row.get(6),
        test_results,
    })
}

fn pg_err(e: impl std::fmt::Display) -> RunStoreError {
    RunStoreError::Database(e.to_string())
}

fn inc_err(e: impl std::fmt::Display) -> IncrementalStateError {
    IncrementalStateError::Database(e.to_string())
}

impl IncrementalStateStorage for PostgresRunStore {
    fn load_state(
        &self,
        pipeline_id: &str,
        node_id: &str,
        environment: &str,
    ) -> Result<Option<IncrementalState>, IncrementalStateError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(inc_err)?;
            let row = client
                .query_opt(
                    "SELECT pipeline_id, node_id, environment, watermark_column, watermark_value,
                            watermark_type, last_run_at, last_run_id, rows_processed,
                            schema_fingerprint
                     FROM incremental_state
                     WHERE pipeline_id = $1 AND node_id = $2 AND environment = $3",
                    &[&pipeline_id, &node_id, &environment],
                )
                .await
                .map_err(inc_err)?;
            Ok(row.as_ref().map(pg_row_to_state))
        })
    }

    fn save_state(&self, state: &IncrementalState) -> Result<(), IncrementalStateError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(inc_err)?;
            client
                .execute(
                    "INSERT INTO incremental_state
                        (pipeline_id, node_id, environment, watermark_column, watermark_value,
                         watermark_type, last_run_at, last_run_id, rows_processed,
                         schema_fingerprint)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                     ON CONFLICT (pipeline_id, node_id, environment) DO UPDATE SET
                        watermark_column   = EXCLUDED.watermark_column,
                        watermark_value    = EXCLUDED.watermark_value,
                        watermark_type     = EXCLUDED.watermark_type,
                        last_run_at        = EXCLUDED.last_run_at,
                        last_run_id        = EXCLUDED.last_run_id,
                        rows_processed     = EXCLUDED.rows_processed,
                        schema_fingerprint = EXCLUDED.schema_fingerprint",
                    &[
                        &state.pipeline_id,
                        &state.node_id,
                        &state.environment,
                        &state.watermark_column,
                        &state.watermark_value,
                        &state.watermark_type,
                        &state.last_run_at_ms,
                        &state.last_run_id,
                        &(state.rows_processed as i64),
                        &state.schema_fingerprint as &(dyn tokio_postgres::types::ToSql + Sync),
                    ],
                )
                .await
                .map_err(inc_err)?;
            Ok(())
        })
    }

    fn reset_state(
        &self,
        pipeline_id: &str,
        node_id: &str,
        environment: &str,
    ) -> Result<bool, IncrementalStateError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(inc_err)?;
            let n = client
                .execute(
                    "DELETE FROM incremental_state
                     WHERE pipeline_id = $1 AND node_id = $2 AND environment = $3",
                    &[&pipeline_id, &node_id, &environment],
                )
                .await
                .map_err(inc_err)?;
            Ok(n > 0)
        })
    }

    fn list_states(
        &self,
        environment: Option<&str>,
    ) -> Result<Vec<IncrementalState>, IncrementalStateError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(inc_err)?;
            let rows = match environment {
                Some(env) => client
                    .query(
                        "SELECT pipeline_id, node_id, environment, watermark_column,
                                    watermark_value, watermark_type, last_run_at, last_run_id,
                                    rows_processed, schema_fingerprint
                             FROM incremental_state
                             WHERE environment = $1
                             ORDER BY pipeline_id, node_id",
                        &[&env],
                    )
                    .await
                    .map_err(inc_err)?,
                None => client
                    .query(
                        "SELECT pipeline_id, node_id, environment, watermark_column,
                                    watermark_value, watermark_type, last_run_at, last_run_id,
                                    rows_processed, schema_fingerprint
                             FROM incremental_state
                             ORDER BY environment, pipeline_id, node_id",
                        &[],
                    )
                    .await
                    .map_err(inc_err)?,
            };
            Ok(rows.iter().map(pg_row_to_state).collect())
        })
    }

    fn record_schema(&self, record: &IncrementalSchemaRecord) -> Result<(), IncrementalStateError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(inc_err)?;
            client
                .execute(
                    "INSERT INTO incremental_schema_history
                        (pipeline_id, node_id, environment, run_id, schema_json, fingerprint,
                         recorded_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)
                     ON CONFLICT (pipeline_id, node_id, environment, run_id) DO UPDATE SET
                        schema_json = EXCLUDED.schema_json,
                        fingerprint = EXCLUDED.fingerprint,
                        recorded_at = EXCLUDED.recorded_at",
                    &[
                        &record.pipeline_id,
                        &record.node_id,
                        &record.environment,
                        &record.run_id,
                        &record.schema_json,
                        &record.fingerprint,
                        &record.recorded_at_ms,
                    ],
                )
                .await
                .map_err(inc_err)?;
            Ok(())
        })
    }

    fn latest_schema(
        &self,
        pipeline_id: &str,
        node_id: &str,
        environment: &str,
    ) -> Result<Option<IncrementalSchemaRecord>, IncrementalStateError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(inc_err)?;
            let row = client
                .query_opt(
                    "SELECT pipeline_id, node_id, environment, run_id, schema_json, fingerprint,
                            recorded_at
                     FROM incremental_schema_history
                     WHERE pipeline_id = $1 AND node_id = $2 AND environment = $3
                     ORDER BY recorded_at DESC
                     LIMIT 1",
                    &[&pipeline_id, &node_id, &environment],
                )
                .await
                .map_err(inc_err)?;
            Ok(row.as_ref().map(pg_row_to_schema_record))
        })
    }

    fn import_state(&self, state: &IncrementalState) -> Result<(), IncrementalStateError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(inc_err)?;
            client
                .execute(
                    "INSERT INTO incremental_state
                        (pipeline_id, node_id, environment, watermark_column, watermark_value,
                         watermark_type, last_run_at, last_run_id, rows_processed,
                         schema_fingerprint)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                     ON CONFLICT (pipeline_id, node_id, environment) DO NOTHING",
                    &[
                        &state.pipeline_id,
                        &state.node_id,
                        &state.environment,
                        &state.watermark_column,
                        &state.watermark_value,
                        &state.watermark_type,
                        &state.last_run_at_ms,
                        &state.last_run_id,
                        &(state.rows_processed as i64),
                        &state.schema_fingerprint as &(dyn tokio_postgres::types::ToSql + Sync),
                    ],
                )
                .await
                .map_err(inc_err)?;
            Ok(())
        })
    }

    fn import_schema_record(
        &self,
        record: &IncrementalSchemaRecord,
    ) -> Result<(), IncrementalStateError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(inc_err)?;
            client
                .execute(
                    "INSERT INTO incremental_schema_history
                        (pipeline_id, node_id, environment, run_id, schema_json, fingerprint,
                         recorded_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)
                     ON CONFLICT (pipeline_id, node_id, environment, run_id) DO NOTHING",
                    &[
                        &record.pipeline_id,
                        &record.node_id,
                        &record.environment,
                        &record.run_id,
                        &record.schema_json,
                        &record.fingerprint,
                        &record.recorded_at_ms,
                    ],
                )
                .await
                .map_err(inc_err)?;
            Ok(())
        })
    }
}

fn lineage_err(e: impl std::fmt::Display) -> LineageStoreError {
    LineageStoreError::Database(e.to_string())
}

impl LineageStorage for PostgresRunStore {
    fn save_bindings(
        &self,
        pipeline_id: &str,
        environment: &str,
        bindings: &[StoredResourceBinding],
    ) -> Result<(), LineageStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(lineage_err)?;
            client
                .execute(
                    "DELETE FROM pipeline_resource_bindings
                     WHERE pipeline_id = $1 AND environment = $2",
                    &[&pipeline_id, &environment],
                )
                .await
                .map_err(lineage_err)?;
            for b in bindings {
                client
                    .execute(
                        "INSERT INTO pipeline_resource_bindings
                            (pipeline_id, node_id, direction, resource_fingerprint, environment,
                             updated_at)
                         VALUES ($1, $2, $3, $4, $5, $6)",
                        &[
                            &b.pipeline_id,
                            &b.node_id,
                            &b.direction.to_string(),
                            &b.resource_fingerprint.0,
                            &b.environment,
                            &b.updated_at_ms,
                        ],
                    )
                    .await
                    .map_err(lineage_err)?;
            }
            Ok(())
        })
    }

    fn load_bindings(
        &self,
        pipeline_id: &str,
        environment: &str,
    ) -> Result<Vec<StoredResourceBinding>, LineageStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(lineage_err)?;
            let rows = client
                .query(
                    "SELECT pipeline_id, node_id, direction, resource_fingerprint, environment,
                            updated_at
                     FROM pipeline_resource_bindings
                     WHERE pipeline_id = $1 AND environment = $2
                     ORDER BY node_id",
                    &[&pipeline_id, &environment],
                )
                .await
                .map_err(lineage_err)?;
            Ok(rows.iter().map(pg_row_to_binding).collect())
        })
    }

    fn all_bindings(
        &self,
        environment: &str,
    ) -> Result<Vec<StoredResourceBinding>, LineageStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(lineage_err)?;
            let rows = client
                .query(
                    "SELECT pipeline_id, node_id, direction, resource_fingerprint, environment,
                            updated_at
                     FROM pipeline_resource_bindings
                     WHERE environment = $1
                     ORDER BY pipeline_id, node_id",
                    &[&environment],
                )
                .await
                .map_err(lineage_err)?;
            Ok(rows.iter().map(pg_row_to_binding).collect())
        })
    }

    fn delete_bindings(&self, pipeline_id: &str) -> Result<(), LineageStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(lineage_err)?;
            client
                .execute(
                    "DELETE FROM pipeline_resource_bindings WHERE pipeline_id = $1",
                    &[&pipeline_id],
                )
                .await
                .map_err(lineage_err)?;
            Ok(())
        })
    }

    fn record_observation(
        &self,
        observation: &LineageObservation,
    ) -> Result<(), LineageStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(lineage_err)?;
            client
                .execute(
                    "INSERT INTO lineage_observations
                        (pipeline_id, node_id, run_id, direction, resource_fingerprint,
                         environment, observed_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)
                     ON CONFLICT (run_id, node_id) DO UPDATE SET
                        pipeline_id          = EXCLUDED.pipeline_id,
                        direction            = EXCLUDED.direction,
                        resource_fingerprint = EXCLUDED.resource_fingerprint,
                        environment          = EXCLUDED.environment,
                        observed_at          = EXCLUDED.observed_at",
                    &[
                        &observation.pipeline_id,
                        &observation.node_id,
                        &observation.run_id,
                        &observation.direction.to_string(),
                        &observation.resource_fingerprint.0,
                        &observation.environment,
                        &observation.observed_at_ms,
                    ],
                )
                .await
                .map_err(lineage_err)?;
            Ok(())
        })
    }

    fn query_observations(
        &self,
        environment: &str,
        since_ms: i64,
    ) -> Result<Vec<LineageObservation>, LineageStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(lineage_err)?;
            let rows = client
                .query(
                    "SELECT pipeline_id, node_id, run_id, direction, resource_fingerprint,
                            environment, observed_at
                     FROM lineage_observations
                     WHERE environment = $1 AND observed_at >= $2
                     ORDER BY observed_at DESC",
                    &[&environment, &since_ms],
                )
                .await
                .map_err(lineage_err)?;
            Ok(rows.iter().map(pg_row_to_observation).collect())
        })
    }

    fn enforce_retention(&self, older_than_ms: i64) -> Result<u64, LineageStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool)
                .await
                .map_err(lineage_err)?;
            let deleted = client
                .execute(
                    "DELETE FROM lineage_observations WHERE observed_at < $1",
                    &[&older_than_ms],
                )
                .await
                .map_err(lineage_err)?;
            Ok(deleted)
        })
    }
}

fn pg_row_to_binding(row: &tokio_postgres::Row) -> StoredResourceBinding {
    let direction_str: String = row.get(2);
    let direction = match direction_str.as_str() {
        "source" => BindingDirection::Source,
        _ => BindingDirection::Sink,
    };
    StoredResourceBinding {
        pipeline_id: row.get(0),
        node_id: row.get(1),
        direction,
        resource_fingerprint: ResourceFingerprint::new(row.get::<_, String>(3)),
        environment: row.get(4),
        updated_at_ms: row.get(5),
    }
}

fn pg_row_to_observation(row: &tokio_postgres::Row) -> LineageObservation {
    let direction_str: String = row.get(3);
    let direction = match direction_str.as_str() {
        "source" => BindingDirection::Source,
        _ => BindingDirection::Sink,
    };
    LineageObservation {
        pipeline_id: row.get(0),
        node_id: row.get(1),
        run_id: row.get(2),
        direction,
        resource_fingerprint: ResourceFingerprint::new(row.get::<_, String>(4)),
        environment: row.get(5),
        observed_at_ms: row.get(6),
    }
}

fn pg_row_to_state(row: &tokio_postgres::Row) -> IncrementalState {
    IncrementalState {
        pipeline_id: row.get(0),
        node_id: row.get(1),
        environment: row.get(2),
        watermark_column: row.get(3),
        watermark_value: row.get(4),
        watermark_type: row.get(5),
        last_run_at_ms: row.get(6),
        last_run_id: row.get(7),
        rows_processed: row.get::<_, i64>(8) as u64,
        schema_fingerprint: row.get(9),
    }
}

fn pg_row_to_schema_record(row: &tokio_postgres::Row) -> IncrementalSchemaRecord {
    IncrementalSchemaRecord {
        pipeline_id: row.get(0),
        node_id: row.get(1),
        environment: row.get(2),
        run_id: row.get(3),
        schema_json: row.get(4),
        fingerprint: row.get(5),
        recorded_at_ms: row.get(6),
    }
}

fn system_time_to_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn ms_to_system_time(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms as u64)
}
