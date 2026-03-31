// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! PostgreSQL-backed storage for pipeline execution history.

use crate::block_on;
use deadpool_postgres::Pool;
use flux_datafusion::error::RunStoreError;
use flux_datafusion::run::{NodeRunStats, PipelineRun, RunId, RunStatus};
use flux_datafusion::storage::RunStorage;
use flux_engine::NodeId;
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

    fn save_node_stats(&self, run_id: &RunId, stats: &NodeRunStats) -> Result<(), RunStoreError> {
        block_on(async {
            let client = crate::retry::get_client(&self.pool).await.map_err(pg_err)?;
            let error_owned = stats.error.clone();
            client
                .execute(
                    "INSERT INTO node_run_stats
                        (run_id, node_id, start_time_ms, end_time_ms, rows_in, rows_out, error)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)
                     ON CONFLICT (run_id, node_id) DO UPDATE SET
                        start_time_ms = EXCLUDED.start_time_ms,
                        end_time_ms = EXCLUDED.end_time_ms,
                        rows_in = EXCLUDED.rows_in,
                        rows_out = EXCLUDED.rows_out,
                        error = EXCLUDED.error",
                    &[
                        &run_id.0.to_string(),
                        &stats.node_id.0,
                        &system_time_to_ms(stats.start_time),
                        &system_time_to_ms(stats.end_time),
                        &(stats.rows_in as i64),
                        &(stats.rows_out as i64),
                        &error_owned as &(dyn tokio_postgres::types::ToSql + Sync),
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
                    "SELECT id, pipeline_name, environment, status, start_time_ms, end_time_ms, error
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
                            "SELECT id, pipeline_name, environment, status, start_time_ms, end_time_ms, error
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
                            "SELECT id, pipeline_name, environment, status, start_time_ms, end_time_ms, error
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
            "SELECT node_id, start_time_ms, end_time_ms, rows_in, rows_out, error
             FROM node_run_stats WHERE run_id = $1
             ORDER BY start_time_ms ASC",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(pg_err)?;

    let mut stats = Vec::with_capacity(rows.len());
    for row in &rows {
        stats.push(NodeRunStats {
            node_id: NodeId::new(row.get::<_, String>(0)),
            start_time: ms_to_system_time(row.get::<_, i64>(1)),
            end_time: ms_to_system_time(row.get::<_, i64>(2)),
            rows_in: row.get::<_, i64>(3) as u64,
            rows_out: row.get::<_, i64>(4) as u64,
            error: row.get(5),
        });
    }
    Ok(stats)
}

fn row_to_pipeline_run(row: &tokio_postgres::Row) -> Result<PipelineRun, RunStoreError> {
    let id_str: String = row.get(0);
    let status_str: String = row.get(3);
    let start_ms: Option<i64> = row.get(4);
    let end_ms: Option<i64> = row.get(5);

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
    })
}

fn pg_err(e: impl std::fmt::Display) -> RunStoreError {
    RunStoreError::Database(e.to_string())
}

fn system_time_to_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn ms_to_system_time(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms as u64)
}
