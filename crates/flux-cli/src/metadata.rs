// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CLI handlers for `flux metadata` subcommands.
//!
//! - `init`    — create remote PostgreSQL schema
//! - `migrate` — run pending schema migrations
//! - `export`  — copy data from the current backend to a PostgreSQL target
//! - `import`  — copy data from a PostgreSQL source to the current backend

use anyhow::{Context, Result};
use clap::Subcommand;
use flux_datafusion::storage::EnvironmentStorage;

use crate::config::{self, MetadataBackend};
use crate::OutputFormat;

#[derive(Subcommand)]
pub enum MetadataAction {
    /// Create the schema in a remote PostgreSQL database.
    Init {
        /// PostgreSQL connection string (e.g., postgresql://user:pass@host/db).
        #[arg(long)]
        url: String,
    },
    /// Run pending schema migrations on the active metadata backend.
    Migrate,
    /// Copy metadata from the current backend to a remote PostgreSQL database.
    Export {
        /// Target PostgreSQL connection string.
        #[arg(long)]
        to: String,
    },
    /// Copy metadata from a remote PostgreSQL database to the current backend.
    Import {
        /// Source PostgreSQL connection string.
        #[arg(long)]
        from: String,
    },
}

pub fn handle(
    action: MetadataAction,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    match action {
        MetadataAction::Init { url } => handle_init(&url, format),
        MetadataAction::Migrate => handle_migrate(format, metadata_url),
        MetadataAction::Export { to } => handle_export(&to, format, metadata_url),
        MetadataAction::Import { from } => handle_import(&from, format, metadata_url),
    }
}

/// Create (or update) the PostgreSQL schema at the given URL.
fn handle_init(url: &str, format: OutputFormat) -> Result<()> {
    let pool = flux_postgres::create_pool(url)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("failed to create connection pool")?;

    let rt = ensure_runtime()?;
    rt.block_on(async {
        flux_postgres::ensure_schema(&pool)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    })
    .context("failed to initialize schema")?;

    match format {
        OutputFormat::Human => println!("PostgreSQL schema initialized successfully."),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::json!({ "status": "ok", "action": "init" })
            );
        }
    }
    Ok(())
}

/// Run pending migrations on the active metadata backend.
fn handle_migrate(format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let data_dir = config::data_dir()?;
    let backend = MetadataBackend::resolve(metadata_url, &data_dir)?;

    match &backend {
        MetadataBackend::Sqlite => {
            // SQLite schemas are auto-created on open — nothing to migrate.
            match format {
                OutputFormat::Human => {
                    println!("SQLite backend: schema is always up to date (auto-created on open).");
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::json!({ "status": "ok", "action": "migrate", "backend": "sqlite" })
                    );
                }
            }
        }
        MetadataBackend::Postgresql { connection_string } => {
            let pool = flux_postgres::create_pool(connection_string)
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            let rt = ensure_runtime()?;
            rt.block_on(async {
                flux_postgres::ensure_schema(&pool)
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))
            })
            .context("failed to run migrations")?;

            match format {
                OutputFormat::Human => println!("PostgreSQL schema is up to date."),
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::json!({ "status": "ok", "action": "migrate", "backend": "postgresql" })
                    );
                }
            }
        }
    }
    Ok(())
}

/// Copy all metadata from the current backend to a target PostgreSQL database.
fn handle_export(to_url: &str, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let data_dir = config::data_dir()?;
    let backend = MetadataBackend::resolve(metadata_url, &data_dir)?;

    // Open source stores.
    let source = config::open_stores(&backend, &data_dir)?;

    // Open target PostgreSQL pool.
    let target_pool = flux_postgres::create_pool(to_url)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("failed to connect to target PostgreSQL")?;

    let rt = ensure_runtime()?;

    // Ensure target schema exists.
    rt.block_on(async {
        flux_postgres::ensure_schema(&target_pool)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    })
    .context("failed to initialize target schema")?;

    // Export environments first (pipelines/runs may reference them indirectly).
    let environments = source
        .environment_store
        .list()
        .context("failed to list environments")?;
    let mut env_count = 0u32;
    for env in &environments {
        rt.block_on(flux_postgres::bulk::insert_environment(
            &target_pool,
            env,
        ))
        .map_err(|e| anyhow::anyhow!("{e}"))
        .with_context(|| format!("failed to export environment '{}'", env.name))?;
        env_count += 1;

        // Export table overrides for this environment.
        let overrides = source
            .environment_store
            .list_table_overrides(&env.name)
            .with_context(|| {
                format!(
                    "failed to list table overrides for '{}'",
                    env.name
                )
            })?;
        for ovr in &overrides {
            rt.block_on(flux_postgres::bulk::insert_table_override(
                &target_pool,
                ovr,
            ))
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    // Export pipelines.
    let pipeline_count = source
        .pipeline_store
        .count()
        .context("failed to count pipelines")?;
    let pipelines = source
        .pipeline_store
        .list(pipeline_count, 0)
        .context("failed to list pipelines")?;
    let mut exported_pipelines = 0u32;
    let mut exported_versions = 0u32;
    for record in &pipelines {
        rt.block_on(flux_postgres::bulk::insert_pipeline(
            &target_pool,
            record,
        ))
        .map_err(|e| anyhow::anyhow!("{e}"))
        .with_context(|| format!("failed to export pipeline '{}'", record.pipeline.name))?;
        exported_pipelines += 1;

        // Export version history.
        let version_count = source
            .pipeline_store
            .count_versions(&record.id)
            .unwrap_or(0);
        let versions = source
            .pipeline_store
            .list_versions(&record.id, version_count, 0)
            .unwrap_or_default();
        for version in &versions {
            rt.block_on(flux_postgres::bulk::insert_pipeline_version(
                &target_pool,
                version,
            ))
            .map_err(|e| anyhow::anyhow!("{e}"))?;
            exported_versions += 1;
        }
    }

    // Export runs.
    let runs = source
        .run_store
        .list_runs(None, u32::MAX)
        .context("failed to list runs")?;
    let mut exported_runs = 0u32;
    for run in &runs {
        rt.block_on(flux_postgres::bulk::insert_run(&target_pool, run))
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("failed to export run {}", run.id))?;
        exported_runs += 1;
    }

    match format {
        OutputFormat::Human => {
            println!("Exported to PostgreSQL:");
            println!("  Environments:      {env_count}");
            println!("  Pipelines:         {exported_pipelines}");
            println!("  Pipeline versions: {exported_versions}");
            println!("  Runs:              {exported_runs}");
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "status": "ok",
                    "action": "export",
                    "environments": env_count,
                    "pipelines": exported_pipelines,
                    "pipeline_versions": exported_versions,
                    "runs": exported_runs,
                }))?
            );
        }
    }
    Ok(())
}

/// Copy all metadata from a source PostgreSQL database to the current backend.
fn handle_import(from_url: &str, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    let data_dir = config::data_dir()?;
    let backend = MetadataBackend::resolve(metadata_url, &data_dir)?;

    // The current backend is the target. For import to make sense, it should
    // typically be SQLite, but we support importing into any backend.
    let is_sqlite = matches!(backend, MetadataBackend::Sqlite);

    // Open source PostgreSQL stores (read-only via trait methods).
    let source_pool = flux_postgres::create_pool(from_url)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("failed to connect to source PostgreSQL")?;

    let rt = ensure_runtime()?;

    // Validate source connection.
    rt.block_on(async {
        let _client = source_pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!("failed to connect to source: {e}"))?;
        Ok::<_, anyhow::Error>(())
    })?;

    let source_pipeline_store = flux_postgres::PostgresPipelineStore::new(source_pool.clone());
    let source_run_store = flux_postgres::PostgresRunStore::new(source_pool.clone());
    let source_env_store = flux_postgres::PostgresEnvironmentStore::new(source_pool)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    if is_sqlite {
        import_to_sqlite(
            &data_dir,
            &source_pipeline_store,
            &source_run_store,
            &source_env_store,
            format,
        )
    } else {
        // Import into the current backend via trait methods (works for any backend
        // but cannot preserve exact IDs for non-SQLite targets).
        import_via_traits(
            &backend,
            &data_dir,
            &source_pipeline_store,
            &source_run_store,
            &source_env_store,
            format,
        )
    }
}

/// Import into local SQLite using raw insert methods that preserve IDs and timestamps.
fn import_to_sqlite(
    data_dir: &std::path::Path,
    source_pipelines: &dyn flux_engine::PipelineStorage,
    source_runs: &dyn flux_datafusion::RunStorage,
    source_envs: &dyn flux_datafusion::EnvironmentStorage,
    format: OutputFormat,
) -> Result<()> {
    let pipelines_dir = data_dir.join("pipelines");
    let target_pipeline_store = flux_engine::SqlitePipelineStore::open(
        &data_dir.join("pipelines.db"),
        &pipelines_dir,
    )
    .context("failed to open local pipeline store")?;
    let target_run_store = flux_datafusion::SqliteRunStore::open(&data_dir.join("runs.db"))
        .context("failed to open local run store")?;
    let target_env_store =
        flux_datafusion::SqliteEnvironmentStore::open(&data_dir.join("environments.db"))
            .context("failed to open local environment store")?;

    // Import environments.
    let environments = source_envs.list().context("failed to list source environments")?;
    let mut env_count = 0u32;
    for env in &environments {
        // Skip defaults that already exist.
        if let Ok(Some(_)) = target_env_store.get(&env.name) {
            // Update fallback to match source.
            let _ = target_env_store.update_fallback(&env.name, env.fallback.as_deref());
        } else {
            let _ = target_env_store.create(&env.name, env.fallback.as_deref());
        }
        env_count += 1;

        let overrides = source_envs
            .list_table_overrides(&env.name)
            .unwrap_or_default();
        for ovr in &overrides {
            let _ = target_env_store.register_table_override(
                &ovr.environment,
                &ovr.schema_name,
                &ovr.table_name,
            );
        }
    }

    // Import pipelines with versions.
    let pipeline_count = source_pipelines
        .count()
        .context("failed to count source pipelines")?;
    let pipelines = source_pipelines
        .list(pipeline_count, 0)
        .context("failed to list source pipelines")?;
    let mut imported_pipelines = 0u32;
    let mut imported_versions = 0u32;
    for record in &pipelines {
        target_pipeline_store
            .import_record(record)
            .with_context(|| format!("failed to import pipeline '{}'", record.pipeline.name))?;
        imported_pipelines += 1;

        let version_count = source_pipelines.count_versions(&record.id).unwrap_or(0);
        let versions = source_pipelines
            .list_versions(&record.id, version_count, 0)
            .unwrap_or_default();
        for version in &versions {
            target_pipeline_store.import_version(version)?;
            imported_versions += 1;
        }
    }

    // Import runs.
    let runs = source_runs
        .list_runs(None, u32::MAX)
        .context("failed to list source runs")?;
    let mut imported_runs = 0u32;
    for run in &runs {
        target_run_store
            .import_run(run)
            .with_context(|| format!("failed to import run {}", run.id))?;
        imported_runs += 1;
    }

    match format {
        OutputFormat::Human => {
            println!("Imported from PostgreSQL to local SQLite:");
            println!("  Environments:      {env_count}");
            println!("  Pipelines:         {imported_pipelines}");
            println!("  Pipeline versions: {imported_versions}");
            println!("  Runs:              {imported_runs}");
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "status": "ok",
                    "action": "import",
                    "target": "sqlite",
                    "environments": env_count,
                    "pipelines": imported_pipelines,
                    "pipeline_versions": imported_versions,
                    "runs": imported_runs,
                }))?
            );
        }
    }
    Ok(())
}

/// Fallback import path using trait methods (does not preserve original IDs).
fn import_via_traits(
    backend: &MetadataBackend,
    data_dir: &std::path::Path,
    source_pipelines: &dyn flux_engine::PipelineStorage,
    _source_runs: &dyn flux_datafusion::RunStorage,
    source_envs: &dyn flux_datafusion::EnvironmentStorage,
    format: OutputFormat,
) -> Result<()> {
    let target = config::open_stores(backend, data_dir)?;

    // Import environments.
    let environments = source_envs.list().context("failed to list source environments")?;
    let mut env_count = 0u32;
    for env in &environments {
        if target.environment_store.get(&env.name)?.is_none() {
            let _ = target
                .environment_store
                .create(&env.name, env.fallback.as_deref());
        }
        env_count += 1;

        let overrides = source_envs
            .list_table_overrides(&env.name)
            .unwrap_or_default();
        for ovr in &overrides {
            let _ = target.environment_store.register_table_override(
                &ovr.environment,
                &ovr.schema_name,
                &ovr.table_name,
            );
        }
    }

    // Import pipelines (creates new IDs — cannot preserve originals via trait).
    let pipeline_count = source_pipelines
        .count()
        .context("failed to count source pipelines")?;
    let pipelines = source_pipelines
        .list(pipeline_count, 0)
        .context("failed to list source pipelines")?;
    let mut imported_pipelines = 0u32;
    for record in &pipelines {
        if target
            .pipeline_store
            .get_by_name(&record.pipeline.name)?
            .is_none()
        {
            target.pipeline_store.create(record.pipeline.clone())?;
            imported_pipelines += 1;
        }
    }

    match format {
        OutputFormat::Human => {
            println!("Imported from PostgreSQL:");
            println!("  Environments: {env_count}");
            println!("  Pipelines:    {imported_pipelines}");
            println!("  (Run history import requires SQLite target)");
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "status": "ok",
                    "action": "import",
                    "environments": env_count,
                    "pipelines": imported_pipelines,
                }))?
            );
        }
    }
    Ok(())
}

/// Get or create a tokio runtime.
fn ensure_runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().context("failed to create tokio runtime")
}
