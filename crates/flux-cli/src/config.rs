// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Metadata backend configuration.
//!
//! Resolution order (first wins):
//! 1. `--metadata-url` CLI flag
//! 2. `HORIZON_FLUX_METADATA_URL` environment variable
//! 3. `~/.horizon-flux/config.toml` `[metadata]` section
//! 4. Default: local SQLite

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Environment variable that overrides the metadata backend URL.
const METADATA_URL_ENV: &str = "HORIZON_FLUX_METADATA_URL";

/// The active metadata backend.
#[derive(Debug, Clone)]
pub enum MetadataBackend {
    /// Local SQLite files in `~/.horizon-flux/`.
    Sqlite,
    /// Remote PostgreSQL database.
    Postgresql { connection_string: String },
}

/// Top-level config file structure (`~/.horizon-flux/config.toml`).
#[derive(Debug, Deserialize, Default)]
struct ConfigFile {
    #[serde(default)]
    metadata: Option<MetadataSection>,
    #[serde(default)]
    logging: Option<flux_observability::config::LoggingConfig>,
    #[serde(default)]
    metrics: Option<flux_observability::config::MetricsConfig>,
    #[serde(default)]
    tracing: Option<flux_observability::config::TracingConfig>,
    #[serde(default)]
    openlineage: Option<flux_observability::config::OpenLineageConfig>,
}

#[derive(Debug, Deserialize)]
struct MetadataSection {
    /// `"sqlite"` (default) or `"postgresql"`.
    #[serde(default = "default_backend_str")]
    backend: String,
    /// Required when `backend = "postgresql"`.
    connection_string: Option<String>,
}

fn default_backend_str() -> String {
    "sqlite".to_string()
}

impl MetadataBackend {
    /// Resolve the metadata backend from the CLI flag, environment, and config file.
    ///
    /// `cli_url` is the value of the `--metadata-url` flag (if provided).
    pub fn resolve(cli_url: Option<&str>, data_dir: &Path) -> Result<Self> {
        // 1. CLI flag
        if let Some(url) = cli_url {
            return Ok(Self::Postgresql {
                connection_string: url.to_string(),
            });
        }

        // 2. Environment variable
        if let Ok(url) = std::env::var(METADATA_URL_ENV) {
            if !url.is_empty() {
                return Ok(Self::Postgresql {
                    connection_string: url,
                });
            }
        }

        // 3. Config file
        let config_path = data_dir.join("config.toml");
        if config_path.exists() {
            let contents = std::fs::read_to_string(&config_path)
                .with_context(|| format!("failed to read {}", config_path.display()))?;
            let config: ConfigFile = toml::from_str(&contents)
                .with_context(|| format!("failed to parse {}", config_path.display()))?;

            if let Some(meta) = config.metadata {
                match meta.backend.as_str() {
                    "postgresql" | "postgres" => {
                        let conn = meta.connection_string.ok_or_else(|| {
                            anyhow::anyhow!(
                                "metadata backend is 'postgresql' but no connection_string \
                                 is set in {}",
                                config_path.display()
                            )
                        })?;
                        return Ok(Self::Postgresql {
                            connection_string: conn,
                        });
                    }
                    "sqlite" => return Ok(Self::Sqlite),
                    other => {
                        anyhow::bail!(
                            "unknown metadata backend '{other}' in {} \
                             (expected 'sqlite' or 'postgresql')",
                            config_path.display()
                        );
                    }
                }
            }
        }

        // 4. Default
        Ok(Self::Sqlite)
    }

    /// Human-readable description for `flux config show`.
    pub fn display_source(&self, cli_url: Option<&str>, data_dir: &Path) -> &'static str {
        if cli_url.is_some() {
            return "--metadata-url flag";
        }
        if std::env::var(METADATA_URL_ENV)
            .ok()
            .is_some_and(|v| !v.is_empty())
        {
            return "HORIZON_FLUX_METADATA_URL env var";
        }
        let config_path = data_dir.join("config.toml");
        if config_path.exists() {
            return "config.toml";
        }
        "default"
    }
}

/// Resolve the logging configuration from the config file.
///
/// Returns `None` if no config file exists or it has no `[logging]` section,
/// in which case the caller should use defaults.
pub fn resolve_logging_config(
    data_dir: &Path,
) -> Option<flux_observability::config::LoggingConfig> {
    let config_path = data_dir.join("config.toml");
    if !config_path.exists() {
        return None;
    }
    let contents = std::fs::read_to_string(&config_path).ok()?;
    let config: ConfigFile = toml::from_str(&contents).ok()?;
    config.logging
}

/// Resolve the metrics configuration from the config file.
///
/// Returns `None` if no config file exists or it has no `[metrics]` section,
/// in which case the caller should use defaults.
pub fn resolve_metrics_config(
    data_dir: &Path,
) -> Option<flux_observability::config::MetricsConfig> {
    let config_path = data_dir.join("config.toml");
    if !config_path.exists() {
        return None;
    }
    let contents = std::fs::read_to_string(&config_path).ok()?;
    let config: ConfigFile = toml::from_str(&contents).ok()?;
    config.metrics
}

/// Resolve the OpenTelemetry tracing configuration from the config file.
///
/// Returns `None` if no config file exists or it has no `[tracing]` section,
/// in which case OTel export is disabled.
pub fn resolve_tracing_config(
    data_dir: &Path,
) -> Option<flux_observability::config::TracingConfig> {
    let config_path = data_dir.join("config.toml");
    if !config_path.exists() {
        return None;
    }
    let contents = std::fs::read_to_string(&config_path).ok()?;
    let config: ConfigFile = toml::from_str(&contents).ok()?;
    config.tracing
}

/// Resolve the OpenLineage configuration from the config file.
///
/// Returns `None` if no config file exists or it has no `[openlineage]` section,
/// in which case OpenLineage emission is disabled.
pub fn resolve_openlineage_config(
    data_dir: &Path,
) -> Option<flux_observability::config::OpenLineageConfig> {
    let config_path = data_dir.join("config.toml");
    if !config_path.exists() {
        return None;
    }
    let contents = std::fs::read_to_string(&config_path).ok()?;
    let config: ConfigFile = toml::from_str(&contents).ok()?;
    config.openlineage
}

/// Return the data directory (`~/.horizon-flux/`), creating it if needed.
pub fn data_dir() -> Result<PathBuf> {
    let dir = dirs::home_dir()
        .context("could not determine home directory")?
        .join(".horizon-flux");
    std::fs::create_dir_all(&dir).context("failed to create data directory")?;
    Ok(dir)
}

/// Opened metadata stores, backend-agnostic.
pub struct MetadataStores {
    pub pipeline_store: Arc<dyn flux_engine::PipelineStorage>,
    pub run_store: Arc<dyn flux_datafusion::RunStorage>,
    pub environment_store: Arc<dyn flux_datafusion::EnvironmentStorage>,
    pub incremental_state_store: Arc<dyn flux_datafusion::IncrementalStateStorage>,
    pub lineage_store: Arc<dyn flux_datafusion::LineageStorage>,
    pub trigger_store: Arc<dyn flux_scheduler::TriggerStorage>,
    pub backfill_store: Arc<dyn flux_datafusion::BackfillStorage>,
    pub column_lineage_store: Option<Arc<dyn flux_datafusion::ColumnLineageStorage>>,
    pub sla_store: Option<Arc<dyn flux_datafusion::SlaStorage>>,
}

/// Open the metadata stores according to the resolved backend.
///
/// For PostgreSQL, this validates the connection and ensures the schema exists.
/// A tokio runtime must be available (either current or the provided `rt`).
pub fn open_stores(backend: &MetadataBackend, data_dir: &Path) -> Result<MetadataStores> {
    match backend {
        MetadataBackend::Sqlite => open_sqlite_stores(data_dir),
        MetadataBackend::Postgresql { connection_string } => {
            open_postgres_stores(connection_string)
        }
    }
}

fn open_sqlite_stores(data_dir: &Path) -> Result<MetadataStores> {
    let pipelines_dir = data_dir.join("pipelines");
    let pipeline_store: Arc<dyn flux_engine::PipelineStorage> = Arc::new(
        flux_engine::SqlitePipelineStore::open(&data_dir.join("pipelines.db"), &pipelines_dir)
            .context("failed to open pipeline store")?,
    );
    // The run store backs `RunStorage`, `IncrementalStateStorage`, and
    // `LineageStorage` — they share the same SQLite file because the
    // lineage and incremental state tables live alongside `pipeline_runs` /
    // `node_run_stats` (planning docs 27, 31).
    let run_store_concrete = Arc::new(
        flux_datafusion::SqliteRunStore::open(&data_dir.join("runs.db"))
            .context("failed to open run store")?,
    );
    let run_store: Arc<dyn flux_datafusion::RunStorage> = run_store_concrete.clone();
    let incremental_state_store: Arc<dyn flux_datafusion::IncrementalStateStorage> =
        run_store_concrete.clone();
    let lineage_store: Arc<dyn flux_datafusion::LineageStorage> = run_store_concrete.clone();
    let sla_store: Option<Arc<dyn flux_datafusion::SlaStorage>> =
        Some(run_store_concrete.clone());
    let column_lineage_store: Option<Arc<dyn flux_datafusion::ColumnLineageStorage>> =
        Some(run_store_concrete);
    let environment_store: Arc<dyn flux_datafusion::EnvironmentStorage> = Arc::new(
        flux_datafusion::SqliteEnvironmentStore::open(&data_dir.join("environments.db"))
            .context("failed to open environment store")?,
    );
    let trigger_store: Arc<dyn flux_scheduler::TriggerStorage> = Arc::new(
        flux_scheduler::SqliteTriggerStore::open(&data_dir.join("triggers.db"))
            .context("failed to open trigger store")?,
    );
    let backfill_store: Arc<dyn flux_datafusion::BackfillStorage> = Arc::new(
        flux_datafusion::SqliteBackfillStore::open(&data_dir.join("backfills.db"))
            .context("failed to open backfill store")?,
    );
    Ok(MetadataStores {
        pipeline_store,
        run_store,
        environment_store,
        incremental_state_store,
        lineage_store,
        trigger_store,
        backfill_store,
        column_lineage_store,
        sla_store,
    })
}

fn open_postgres_stores(connection_string: &str) -> Result<MetadataStores> {
    let pool = flux_postgres::create_pool(connection_string)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("failed to create PostgreSQL connection pool")?;

    // Validate connection and ensure schema.
    // This requires a tokio runtime — use block_in_place if we're already on one,
    // otherwise create a temporary runtime.
    let ensure = async {
        flux_postgres::ensure_schema(&pool)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    };

    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(ensure))
            .context("failed to initialize PostgreSQL schema")?;
    } else {
        let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
        rt.block_on(ensure)
            .context("failed to initialize PostgreSQL schema")?;
    }

    let pipeline_store: Arc<dyn flux_engine::PipelineStorage> =
        Arc::new(flux_postgres::PostgresPipelineStore::new(pool.clone()));
    let run_store_concrete = Arc::new(flux_postgres::PostgresRunStore::new(pool.clone()));
    let run_store: Arc<dyn flux_datafusion::RunStorage> = run_store_concrete.clone();
    let incremental_state_store: Arc<dyn flux_datafusion::IncrementalStateStorage> =
        run_store_concrete.clone();
    let lineage_store: Arc<dyn flux_datafusion::LineageStorage> = run_store_concrete.clone();
    let sla_store: Option<Arc<dyn flux_datafusion::SlaStorage>> =
        Some(run_store_concrete.clone());
    let column_lineage_store: Option<Arc<dyn flux_datafusion::ColumnLineageStorage>> =
        Some(run_store_concrete);
    let environment_store: Arc<dyn flux_datafusion::EnvironmentStorage> = Arc::new(
        flux_postgres::PostgresEnvironmentStore::new(pool)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .context("failed to initialize PostgreSQL environment store")?,
    );

    // Trigger and backfill stores remain SQLite-backed even with PostgreSQL
    // metadata. The tables live in a local file alongside the data directory.
    let data_dir = data_dir().context("could not determine data directory for trigger store")?;
    let trigger_store: Arc<dyn flux_scheduler::TriggerStorage> = Arc::new(
        flux_scheduler::SqliteTriggerStore::open(&data_dir.join("triggers.db"))
            .context("failed to open trigger store")?,
    );
    let backfill_store: Arc<dyn flux_datafusion::BackfillStorage> = Arc::new(
        flux_datafusion::SqliteBackfillStore::open(&data_dir.join("backfills.db"))
            .context("failed to open backfill store")?,
    );

    Ok(MetadataStores {
        pipeline_store,
        run_store,
        environment_store,
        incremental_state_store,
        lineage_store,
        trigger_store,
        backfill_store,
        column_lineage_store,
        sla_store,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_defaults_to_sqlite() {
        let tmp = tempfile::tempdir().unwrap();
        // Clear any env var that might be set.
        unsafe { std::env::remove_var(METADATA_URL_ENV) };
        let backend = MetadataBackend::resolve(None, tmp.path()).unwrap();
        assert!(matches!(backend, MetadataBackend::Sqlite));
    }

    #[test]
    fn resolve_cli_flag_wins() {
        let tmp = tempfile::tempdir().unwrap();
        let backend =
            MetadataBackend::resolve(Some("postgresql://localhost/test"), tmp.path()).unwrap();
        match backend {
            MetadataBackend::Postgresql { connection_string } => {
                assert_eq!(connection_string, "postgresql://localhost/test");
            }
            _ => panic!("expected Postgresql"),
        }
    }

    #[test]
    fn resolve_env_var() {
        let tmp = tempfile::tempdir().unwrap();
        unsafe { std::env::set_var(METADATA_URL_ENV, "postgresql://envhost/db") };
        let backend = MetadataBackend::resolve(None, tmp.path()).unwrap();
        unsafe { std::env::remove_var(METADATA_URL_ENV) };
        match backend {
            MetadataBackend::Postgresql { connection_string } => {
                assert_eq!(connection_string, "postgresql://envhost/db");
            }
            _ => panic!("expected Postgresql"),
        }
    }

    #[test]
    fn resolve_config_file_sqlite() {
        let tmp = tempfile::tempdir().unwrap();
        unsafe { std::env::remove_var(METADATA_URL_ENV) };
        std::fs::write(
            tmp.path().join("config.toml"),
            r#"
[metadata]
backend = "sqlite"
"#,
        )
        .unwrap();
        let backend = MetadataBackend::resolve(None, tmp.path()).unwrap();
        assert!(matches!(backend, MetadataBackend::Sqlite));
    }

    #[test]
    fn resolve_config_file_postgres() {
        let tmp = tempfile::tempdir().unwrap();
        unsafe { std::env::remove_var(METADATA_URL_ENV) };
        std::fs::write(
            tmp.path().join("config.toml"),
            r#"
[metadata]
backend = "postgresql"
connection_string = "postgresql://confighost/db"
"#,
        )
        .unwrap();
        let backend = MetadataBackend::resolve(None, tmp.path()).unwrap();
        match backend {
            MetadataBackend::Postgresql { connection_string } => {
                assert_eq!(connection_string, "postgresql://confighost/db");
            }
            _ => panic!("expected Postgresql"),
        }
    }

    #[test]
    fn resolve_config_file_postgres_missing_connection_string() {
        let tmp = tempfile::tempdir().unwrap();
        unsafe { std::env::remove_var(METADATA_URL_ENV) };
        std::fs::write(
            tmp.path().join("config.toml"),
            r#"
[metadata]
backend = "postgresql"
"#,
        )
        .unwrap();
        let result = MetadataBackend::resolve(None, tmp.path());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no connection_string")
        );
    }

    #[test]
    fn resolve_unknown_backend_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        unsafe { std::env::remove_var(METADATA_URL_ENV) };
        std::fs::write(
            tmp.path().join("config.toml"),
            r#"
[metadata]
backend = "mysql"
"#,
        )
        .unwrap();
        let result = MetadataBackend::resolve(None, tmp.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown"));
    }

    #[test]
    fn cli_flag_overrides_env_var() {
        let tmp = tempfile::tempdir().unwrap();
        unsafe { std::env::set_var(METADATA_URL_ENV, "postgresql://envhost/db") };
        let backend =
            MetadataBackend::resolve(Some("postgresql://cliflag/db"), tmp.path()).unwrap();
        unsafe { std::env::remove_var(METADATA_URL_ENV) };
        match backend {
            MetadataBackend::Postgresql { connection_string } => {
                assert_eq!(connection_string, "postgresql://cliflag/db");
            }
            _ => panic!("expected Postgresql"),
        }
    }
}
