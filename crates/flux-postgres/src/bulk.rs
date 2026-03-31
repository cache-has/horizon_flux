// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Bulk insert functions for metadata migration.
//!
//! These functions write records to PostgreSQL preserving original IDs,
//! timestamps, and counters — used by `flux metadata export` to copy data
//! from another backend without generating new identifiers.

use deadpool_postgres::Pool;
use flux_datafusion::environment::{Environment, TableOverride};
use flux_datafusion::run::{NodeRunStats, PipelineRun};
use flux_engine::pipeline_store::{PipelineRecord, PipelineVersion};
use std::time::{SystemTime, UNIX_EPOCH};

/// Insert a pipeline record preserving its original ID, timestamps, and run count.
pub async fn insert_pipeline(pool: &Pool, record: &PipelineRecord) -> Result<(), String> {
    let client = crate::retry::get_client(pool)
        .await
        .map_err(|e| format!("pool error: {e}"))?;

    let definition =
        serde_json::to_value(&record.pipeline).map_err(|e| format!("json error: {e}"))?;

    let last_run_ms = record.last_run_at.map(system_time_to_ms);

    client
        .execute(
            "INSERT INTO pipelines (id, name, definition, created_at, updated_at, last_run_at, run_count)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (id) DO NOTHING",
            &[
                &record.id.0.to_string(),
                &record.pipeline.name,
                &definition,
                &system_time_to_ms(record.created_at),
                &system_time_to_ms(record.updated_at),
                &last_run_ms,
                &(record.run_count as i32),
            ],
        )
        .await
        .map_err(|e| format!("insert pipeline: {e}"))?;

    Ok(())
}

/// Insert a pipeline version snapshot preserving the original version number and timestamp.
pub async fn insert_pipeline_version(pool: &Pool, version: &PipelineVersion) -> Result<(), String> {
    let client = crate::retry::get_client(pool)
        .await
        .map_err(|e| format!("pool error: {e}"))?;

    let snapshot =
        serde_json::to_value(&version.snapshot).map_err(|e| format!("json error: {e}"))?;

    client
        .execute(
            "INSERT INTO pipeline_versions (pipeline_id, version, saved_at, snapshot)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (pipeline_id, version) DO NOTHING",
            &[
                &version.pipeline_id.0.to_string(),
                &(version.version as i32),
                &system_time_to_ms(version.saved_at),
                &snapshot,
            ],
        )
        .await
        .map_err(|e| format!("insert pipeline version: {e}"))?;

    Ok(())
}

/// Insert a pipeline run preserving its original ID, timestamps, and status.
pub async fn insert_run(pool: &Pool, run: &PipelineRun) -> Result<(), String> {
    let client = crate::retry::get_client(pool)
        .await
        .map_err(|e| format!("pool error: {e}"))?;

    let start_ms = run.start_time.map(system_time_to_ms);
    let end_ms = run.end_time.map(system_time_to_ms);

    client
        .execute(
            "INSERT INTO pipeline_runs (id, pipeline_name, environment, status, start_time_ms, end_time_ms, error)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (id) DO NOTHING",
            &[
                &run.id.0.to_string(),
                &run.pipeline_name,
                &run.environment,
                &run.status.as_str(),
                &start_ms,
                &end_ms,
                &run.error as &(dyn tokio_postgres::types::ToSql + Sync),
            ],
        )
        .await
        .map_err(|e| format!("insert run: {e}"))?;

    // Insert node stats.
    for stats in &run.node_stats {
        insert_node_stats(&client, &run.id, stats).await?;
    }

    Ok(())
}

async fn insert_node_stats(
    client: &deadpool_postgres::Client,
    run_id: &flux_datafusion::run::RunId,
    stats: &NodeRunStats,
) -> Result<(), String> {
    client
        .execute(
            "INSERT INTO node_run_stats (run_id, node_id, start_time_ms, end_time_ms, rows_in, rows_out, error)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (run_id, node_id) DO NOTHING",
            &[
                &run_id.0.to_string(),
                &stats.node_id.0,
                &system_time_to_ms(stats.start_time),
                &system_time_to_ms(stats.end_time),
                &(stats.rows_in as i64),
                &(stats.rows_out as i64),
                &stats.error as &(dyn tokio_postgres::types::ToSql + Sync),
            ],
        )
        .await
        .map_err(|e| format!("insert node stats: {e}"))?;

    Ok(())
}

/// Insert an environment, skipping if it already exists.
pub async fn insert_environment(pool: &Pool, env: &Environment) -> Result<(), String> {
    let client = crate::retry::get_client(pool)
        .await
        .map_err(|e| format!("pool error: {e}"))?;

    client
        .execute(
            "INSERT INTO environments (name, fallback) VALUES ($1, $2)
             ON CONFLICT (name) DO UPDATE SET fallback = EXCLUDED.fallback",
            &[
                &env.name,
                &env.fallback as &(dyn tokio_postgres::types::ToSql + Sync),
            ],
        )
        .await
        .map_err(|e| format!("insert environment: {e}"))?;

    Ok(())
}

/// Insert a table override, skipping if it already exists.
pub async fn insert_table_override(pool: &Pool, ovr: &TableOverride) -> Result<(), String> {
    let client = crate::retry::get_client(pool)
        .await
        .map_err(|e| format!("pool error: {e}"))?;

    client
        .execute(
            "INSERT INTO table_overrides (environment, schema_name, table_name)
             VALUES ($1, $2, $3)
             ON CONFLICT (environment, schema_name, table_name) DO NOTHING",
            &[&ovr.environment, &ovr.schema_name, &ovr.table_name],
        )
        .await
        .map_err(|e| format!("insert table override: {e}"))?;

    Ok(())
}

fn system_time_to_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}
