// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

mod backfill;
mod catalog;
pub mod color;
pub mod config;
mod environment;
mod incremental;
mod lineage;
mod metadata;
mod pipeline;
mod plugin;
mod secret;
mod server;
mod snapshot;
mod snippet;
mod trigger;
mod udf;

/// Exit code for pipeline execution failures (distinct from general errors).
/// Used by `armillary run` when the pipeline itself fails (vs. a CLI/config error).
#[allow(dead_code)]
const EXIT_PIPELINE_FAILURE: u8 = 2;

/// Exit codes for `armillary test`:
/// 0 = all tests passed
/// 1 = at least one test with severity=error failed
/// 2 = warnings only (all severity=warn tests had failures, no errors)
/// 3 = configuration error (invalid pipeline, no test nodes, etc.)
const EXIT_TEST_PASS: i32 = 0;
const EXIT_TEST_FAIL: i32 = 1;
const EXIT_TEST_WARN: i32 = 2;
const EXIT_TEST_CONFIG_ERROR: i32 = 3;

/// Output format: human-readable (default) or JSON for scripting.
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Human,
    Json,
}

#[derive(Parser)]
#[command(
    name = "armillary",
    version,
    about = "Armillary — visual data pipeline builder"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Output results as JSON instead of human-readable text.
    #[arg(long, global = true)]
    json: bool,

    /// Use a PostgreSQL metadata store instead of local SQLite.
    /// Overrides ARMILLARY_METADATA_URL and config.toml.
    #[arg(long, global = true)]
    metadata_url: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    /// Start the Armillary server.
    Start {
        /// Port number for the web server.
        #[arg(long, short, default_value_t = 8080)]
        port: u16,

        /// Address to bind to (default: 127.0.0.1, use 0.0.0.0 for Docker).
        #[arg(long, default_value = "127.0.0.1", env = "ARMILLARY_HOST")]
        host: std::net::IpAddr,

        /// Start without opening the browser.
        #[arg(long)]
        headless: bool,

        /// Proxy frontend requests to the Vite dev server.
        #[arg(long)]
        dev: bool,
    },
    /// Stop a running server instance.
    Stop,
    /// Show server status (running, port, PID).
    Status,
    /// Manage encrypted secrets.
    Secret {
        #[command(subcommand)]
        action: secret::SecretAction,
    },
    /// Manage environments (fallback chains, table overrides).
    Env {
        #[command(subcommand)]
        action: environment::EnvAction,
    },
    /// Export a pipeline definition to a JSON file, or all pipelines to a directory.
    Export {
        /// Pipeline name or UUID (omit when using --all).
        pipeline: Option<String>,
        /// Output file path (single pipeline) or directory (--all).
        /// Defaults to `{pipeline_name}.json` in the current directory.
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
        /// Export all pipelines to the output directory.
        #[arg(long)]
        all: bool,
    },
    /// Import a pipeline definition from a JSON file, or all pipelines from a directory.
    Import {
        /// Path to a JSON pipeline file or a directory of JSON files.
        file: std::path::PathBuf,
        /// How to handle name conflicts: reject, rename, or overwrite.
        #[arg(long, default_value = "reject")]
        on_conflict: String,
    },
    /// Execute a pipeline.
    Run {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Environment to execute in (e.g., dev, prod).
        #[arg(long, short)]
        env: Option<String>,
        /// Variable overrides in key=value format (repeatable).
        #[arg(long, short = 'V', value_parser = pipeline::parse_var)]
        var: Vec<(String, String)>,
        /// Validate the pipeline without executing it.
        #[arg(long)]
        dry_run: bool,
        /// Force a full rebuild — skip watermark filter injection on
        /// incremental sinks but still advance their state at the end.
        #[arg(long)]
        full_refresh: bool,
        /// Allow incremental sinks configured with `first_run: fail` to
        /// perform their initial bootstrap load.
        #[arg(long)]
        bootstrap_incremental: bool,
    },
    /// List all pipelines.
    List,
    /// Show pipeline details (nodes, connections, variables).
    Show {
        /// Pipeline name or UUID.
        pipeline: String,
    },
    /// Show execution history for a pipeline.
    History {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Maximum number of runs to show.
        #[arg(long, default_value_t = 20)]
        limit: u32,
    },
    /// Run a preview and output sample data to stdout.
    Preview {
        /// Pipeline name or UUID.
        pipeline: String,
        /// Variable overrides in key=value format (repeatable).
        #[arg(long, short = 'V', value_parser = pipeline::parse_var)]
        var: Vec<(String, String)>,
    },
    /// Show active metadata configuration.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Manage plugins (discovery, inspection, smoke-test).
    Plugin {
        #[command(subcommand)]
        action: plugin::PluginAction,
    },
    /// Manage the metadata store (schema init, migrations, data transfer).
    Metadata {
        #[command(subcommand)]
        action: metadata::MetadataAction,
    },
    /// Inspect or reset incremental sink materialization state.
    Incremental {
        #[command(subcommand)]
        action: incremental::IncrementalAction,
    },
    /// Inspect SCD2 snapshot sinks: dry-run diff and per-key history.
    Snapshot {
        #[command(subcommand)]
        action: snapshot::SnapshotAction,
    },
    /// Inspect reusable SQL UDFs declared in a pipeline's `udfs_dir`.
    Udf {
        #[command(subcommand)]
        action: udf::UdfAction,
    },
    /// Inspect reusable pipeline snippets declared in a pipeline's `snippets_dir`.
    Snippet {
        #[command(subcommand)]
        action: snippet::SnippetAction,
    },
    /// Cross-pipeline lineage: graph, upstream/downstream, impact, cycles, orphans.
    Lineage {
        #[command(subcommand)]
        action: lineage::LineageAction,
    },
    /// Resource catalog: list, show, search, describe, validate.
    Catalog {
        #[command(subcommand)]
        action: catalog::CatalogAction,
    },
    /// Manage scheduling triggers (cron, interval, file arrival, webhook, pipeline completion).
    Trigger {
        #[command(subcommand)]
        action: Box<trigger::TriggerAction>,
    },
    /// Backfill a pipeline across a range of parameter values.
    Backfill {
        #[command(subcommand)]
        action: backfill::BackfillAction,
    },
    /// Run data quality tests in a pipeline (skip sink writes).
    Test {
        /// Pipeline name or UUID (omit when using --all).
        pipeline: Option<String>,
        /// Environment to execute in (e.g., dev, prod).
        #[arg(long, short)]
        env: Option<String>,
        /// Variable overrides in key=value format (repeatable).
        #[arg(long, short = 'V', value_parser = pipeline::parse_var)]
        var: Vec<(String, String)>,
        /// Run only a specific test node by name or ID.
        #[arg(long)]
        node: Option<String>,
        /// Run tests across all pipelines.
        #[arg(long)]
        all: bool,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Display the active metadata backend configuration.
    Show,
}

fn main() -> ExitCode {
    color::init();

    // Load .env file from the current directory (silently skip if not found).
    let _ = dotenvy::dotenv();

    // Resolve logging, metrics, and tracing config from ~/.armillary/config.toml.
    let data_dir_path = config::data_dir().ok();
    let logging_config = data_dir_path
        .as_deref()
        .and_then(config::resolve_logging_config);
    let metrics_config = data_dir_path
        .as_deref()
        .and_then(config::resolve_metrics_config);
    let tracing_config = data_dir_path
        .as_deref()
        .and_then(config::resolve_tracing_config);
    let _otel_guard = armillary_observability::init_all(
        logging_config.as_ref(),
        metrics_config.as_ref(),
        tracing_config.as_ref(),
    );

    let cli = Cli::parse();
    let format = if cli.json {
        OutputFormat::Json
    } else {
        OutputFormat::Human
    };

    let metadata_url = cli.metadata_url.clone();
    let code = match run(cli, format, metadata_url.as_deref()) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{} {e:#}", color::red("Error:"));
            1
        }
    };
    // Flush OTel spans before force-exiting. This must happen before
    // process::exit because that skips destructors.
    if let Some(guard) = _otel_guard {
        guard.shutdown();
    }

    // Force exit — background threads (tokio tasks, DataFusion thread pools)
    // can prevent a clean shutdown. The server command handles its own
    // lifecycle; all other commands should exit immediately when done.
    std::process::exit(code);
}

fn run(cli: Cli, format: OutputFormat, metadata_url: Option<&str>) -> Result<()> {
    match cli.command {
        // Default (no subcommand) = start the server.
        None => server::start(
            8080,
            armillary_server::port::DEFAULT_HOST,
            false,
            false,
            metadata_url,
        ),

        Some(Command::Start {
            port,
            host,
            headless,
            dev,
        }) => server::start(port, host, headless, dev, metadata_url),

        Some(Command::Stop) => server::handle(server::ServerAction::Stop, format),

        Some(Command::Status) => server::handle(server::ServerAction::Status, format),

        Some(Command::Secret { action }) => secret::handle(action).context("secret command failed"),

        Some(Command::Env { action }) => {
            environment::handle(action, format, metadata_url).context("env command failed")
        }

        Some(Command::Export {
            pipeline,
            output,
            all,
        }) => {
            if all {
                export_all(output.as_deref(), format, metadata_url)
            } else {
                let pipeline = pipeline.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("pipeline name or UUID required (or use --all)")
                })?;
                export_pipeline(pipeline, output.as_deref(), format, metadata_url)
            }
        }

        Some(Command::Import { file, on_conflict }) => {
            if file.is_dir() {
                import_directory(&file, &on_conflict, format, metadata_url)
            } else {
                import_pipeline(&file, &on_conflict, format, metadata_url)
            }
        }

        Some(Command::Run {
            pipeline,
            env,
            var,
            dry_run,
            full_refresh,
            bootstrap_incremental,
        }) => pipeline::run(
            &pipeline,
            env.as_deref(),
            var,
            dry_run,
            full_refresh,
            bootstrap_incremental,
            format,
            metadata_url,
        ),

        Some(Command::List) => pipeline::list(format, metadata_url),

        Some(Command::Show { pipeline }) => pipeline::show(&pipeline, format, metadata_url),

        Some(Command::History { pipeline, limit }) => {
            pipeline::history(&pipeline, limit, format, metadata_url)
        }

        Some(Command::Preview { pipeline, var }) => {
            pipeline::preview(&pipeline, var, format, metadata_url)
        }

        Some(Command::Config { action }) => config_command(action, format, metadata_url),

        Some(Command::Plugin { action }) => {
            plugin::handle(action, format).context("plugin command failed")
        }

        Some(Command::Metadata { action }) => {
            metadata::handle(action, format, metadata_url).context("metadata command failed")
        }

        Some(Command::Incremental { action }) => {
            incremental::handle(action, format, metadata_url).context("incremental command failed")
        }

        Some(Command::Snapshot { action }) => {
            snapshot::handle(action, format, metadata_url).context("snapshot command failed")
        }

        Some(Command::Udf { action }) => udf::handle(action, format).context("udf command failed"),

        Some(Command::Snippet { action }) => {
            snippet::handle(action, format).context("snippet command failed")
        }

        Some(Command::Lineage { action }) => {
            lineage::handle(action, format, metadata_url).context("lineage command failed")
        }

        Some(Command::Catalog { action }) => {
            catalog::handle(action, format, metadata_url).context("catalog command failed")
        }

        Some(Command::Trigger { action }) => {
            trigger::handle(*action, format, metadata_url).context("trigger command failed")
        }

        Some(Command::Backfill { action }) => {
            backfill::handle(action, format, metadata_url).context("backfill command failed")
        }

        Some(Command::Test {
            pipeline,
            env,
            var,
            node,
            all,
        }) => pipeline::test(
            pipeline.as_deref(),
            env.as_deref(),
            var,
            node.as_deref(),
            all,
            format,
            metadata_url,
        ),
    }
}

fn config_command(
    action: ConfigAction,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    match action {
        ConfigAction::Show => {
            let data_dir = config::data_dir()?;
            let backend = config::MetadataBackend::resolve(metadata_url, &data_dir)?;
            let source = backend.display_source(metadata_url, &data_dir);

            match format {
                OutputFormat::Human => {
                    match &backend {
                        config::MetadataBackend::Sqlite => {
                            println!("Metadata backend: sqlite (local)");
                            println!("Data directory:   {}", data_dir.display());
                        }
                        config::MetadataBackend::Postgresql { connection_string } => {
                            println!("Metadata backend: postgresql");
                            println!("Connection:       {}", redact_password(connection_string));
                        }
                    }
                    println!("Source:           {source}");
                }
                OutputFormat::Json => {
                    let out = match &backend {
                        config::MetadataBackend::Sqlite => serde_json::json!({
                            "backend": "sqlite",
                            "data_directory": data_dir.display().to_string(),
                            "source": source,
                        }),
                        config::MetadataBackend::Postgresql { connection_string } => {
                            serde_json::json!({
                                "backend": "postgresql",
                                "connection_string": redact_password(connection_string),
                                "source": source,
                            })
                        }
                    };
                    println!("{}", serde_json::to_string_pretty(&out)?);
                }
            }
            Ok(())
        }
    }
}

/// Redact the password portion of a PostgreSQL connection string for display.
fn redact_password(url: &str) -> String {
    // postgresql://user:pass@host → postgresql://user:***@host
    if let Some(at) = url.find('@') {
        if let Some(colon) = url[..at].rfind(':') {
            // Only redact if there's a scheme prefix (contains "://")
            if url[..colon].contains("://") {
                return format!("{}:***{}", &url[..colon], &url[at..]);
            }
        }
    }
    url.to_string()
}

fn export_pipeline(
    pipeline: &str,
    output: Option<&std::path::Path>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let data_dir = config::data_dir()?;
    let backend = config::MetadataBackend::resolve(metadata_url, &data_dir)?;
    let stores = config::open_stores(&backend, &data_dir)?;
    let pipeline_store = stores.pipeline_store;

    let record = if let Ok(id) = pipeline.parse::<armillary_engine::PipelineId>() {
        pipeline_store.get(&id).context("failed to read pipeline")?
    } else {
        pipeline_store
            .get_by_name(pipeline)
            .context("failed to read pipeline")?
    }
    .ok_or_else(|| anyhow::anyhow!("pipeline `{pipeline}` not found"))?;

    let export_pipeline = record
        .pipeline
        .with_resolved_code()
        .context("failed to resolve code files")?;
    let json = export_pipeline
        .to_json()
        .context("failed to serialize pipeline")?;
    let out_path = match output {
        Some(p) => p.to_path_buf(),
        None => {
            let name = sanitize_name(&record.pipeline.name);
            std::path::PathBuf::from(format!("{name}.json"))
        }
    };
    std::fs::write(&out_path, &json)
        .with_context(|| format!("failed to write {}", out_path.display()))?;

    match format {
        OutputFormat::Human => {
            println!(
                "Exported `{}` → {}",
                record.pipeline.name,
                out_path.display()
            );
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "pipeline": record.pipeline.name,
                "id": record.id.to_string(),
                "path": out_path.display().to_string(),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

fn export_all(
    output_dir: Option<&std::path::Path>,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let data_dir = config::data_dir()?;
    let backend = config::MetadataBackend::resolve(metadata_url, &data_dir)?;
    let stores = config::open_stores(&backend, &data_dir)?;
    let pipeline_store = stores.pipeline_store;

    let out_dir = output_dir
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));

    std::fs::create_dir_all(&out_dir)
        .with_context(|| format!("failed to create directory {}", out_dir.display()))?;

    let total = pipeline_store
        .count()
        .context("failed to count pipelines")?;
    let records = pipeline_store
        .list(total, 0)
        .context("failed to list pipelines")?;

    if records.is_empty() {
        match format {
            OutputFormat::Human => println!("No pipelines to export."),
            OutputFormat::Json => println!("{}", serde_json::json!({ "exported": [] })),
        }
        return Ok(());
    }

    let mut exported = Vec::new();
    for record in &records {
        let export_pipeline = record.pipeline.with_resolved_code().with_context(|| {
            format!(
                "failed to resolve code files for `{}`",
                record.pipeline.name
            )
        })?;
        let json = export_pipeline
            .to_json()
            .with_context(|| format!("failed to serialize pipeline `{}`", record.pipeline.name))?;
        let file_name = sanitize_name(&record.pipeline.name);
        let out_path = out_dir.join(format!("{file_name}.json"));
        std::fs::write(&out_path, &json)
            .with_context(|| format!("failed to write {}", out_path.display()))?;
        exported.push((record.pipeline.name.clone(), out_path));
    }

    match format {
        OutputFormat::Human => {
            println!(
                "Exported {} pipelines to {}/",
                exported.len(),
                out_dir.display()
            );
            for (name, path) in &exported {
                println!("  `{name}` → {}", path.display());
            }
        }
        OutputFormat::Json => {
            let items: Vec<_> = exported
                .iter()
                .map(|(name, path)| {
                    serde_json::json!({
                        "pipeline": name,
                        "path": path.display().to_string(),
                    })
                })
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "exported": items }))?
            );
        }
    }
    Ok(())
}

fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn import_pipeline(
    file: &std::path::Path,
    on_conflict: &str,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let data_dir = config::data_dir()?;
    let backend = config::MetadataBackend::resolve(metadata_url, &data_dir)?;
    let stores = config::open_stores(&backend, &data_dir)?;
    let pipeline_store = stores.pipeline_store;

    let json = std::fs::read_to_string(file)
        .with_context(|| format!("failed to read {}", file.display()))?;

    let (mut pipeline, warnings) = armillary_engine::Pipeline::from_json_with_warnings(&json)
        .context("failed to parse pipeline")?;

    for w in &warnings.undefined_variables {
        eprintln!("warning: {w}");
    }

    let existing = pipeline_store
        .get_by_name(&pipeline.name)
        .context("failed to check for name conflict")?;

    let record = if let Some(existing_record) = existing {
        match on_conflict {
            "rename" => {
                let base_name = pipeline.name.clone();
                let mut counter = 2u32;
                loop {
                    let candidate = format!("{base_name} ({counter})");
                    if pipeline_store.get_by_name(&candidate)?.is_none() {
                        pipeline.name = candidate;
                        break;
                    }
                    counter += 1;
                    anyhow::ensure!(counter <= 100, "could not find a unique name");
                }
                pipeline_store
                    .create(pipeline)
                    .context("failed to create pipeline")?
            }
            "overwrite" => pipeline_store
                .update(&existing_record.id, pipeline)
                .context("failed to overwrite pipeline")?,
            _ => {
                anyhow::bail!(
                    "pipeline `{}` already exists (use --on-conflict rename or overwrite)",
                    pipeline.name
                );
            }
        }
    } else {
        pipeline_store
            .create(pipeline)
            .context("failed to create pipeline")?
    };

    match format {
        OutputFormat::Human => {
            println!("Imported `{}` (id: {})", record.pipeline.name, record.id);
        }
        OutputFormat::Json => {
            let out = serde_json::json!({
                "pipeline": record.pipeline.name,
                "id": record.id.to_string(),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

fn import_directory(
    dir: &std::path::Path,
    on_conflict: &str,
    format: OutputFormat,
    metadata_url: Option<&str>,
) -> Result<()> {
    let data_dir = config::data_dir()?;
    let backend = config::MetadataBackend::resolve(metadata_url, &data_dir)?;
    let stores = config::open_stores(&backend, &data_dir)?;
    let pipeline_store = stores.pipeline_store;

    let mut json_files: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory {}", dir.display()))?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") && path.is_file() {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    json_files.sort();

    if json_files.is_empty() {
        match format {
            OutputFormat::Human => println!("No JSON files found in {}", dir.display()),
            OutputFormat::Json => println!("{}", serde_json::json!({ "imported": [] })),
        }
        return Ok(());
    }

    let mut imported = Vec::new();
    let mut errors = Vec::new();

    for path in &json_files {
        let json = match std::fs::read_to_string(path) {
            Ok(j) => j,
            Err(e) => {
                errors.push((path.display().to_string(), format!("{e}")));
                continue;
            }
        };

        let (mut pipeline, warnings) =
            match armillary_engine::Pipeline::from_json_with_warnings(&json) {
                Ok(pw) => pw,
                Err(e) => {
                    errors.push((path.display().to_string(), format!("{e}")));
                    continue;
                }
            };

        for w in &warnings.undefined_variables {
            eprintln!("warning: {} ({}): {w}", path.display(), pipeline.name);
        }

        let existing = pipeline_store
            .get_by_name(&pipeline.name)
            .context("failed to check for name conflict")?;

        let record = if let Some(existing_record) = existing {
            match on_conflict {
                "rename" => {
                    let base_name = pipeline.name.clone();
                    let mut counter = 2u32;
                    loop {
                        let candidate = format!("{base_name} ({counter})");
                        if pipeline_store.get_by_name(&candidate)?.is_none() {
                            pipeline.name = candidate;
                            break;
                        }
                        counter += 1;
                        anyhow::ensure!(counter <= 100, "could not find a unique name");
                    }
                    pipeline_store
                        .create(pipeline)
                        .context("failed to create pipeline")?
                }
                "overwrite" => pipeline_store
                    .update(&existing_record.id, pipeline)
                    .context("failed to overwrite pipeline")?,
                _ => {
                    errors.push((
                        path.display().to_string(),
                        format!(
                            "pipeline `{}` already exists (use --on-conflict rename or overwrite)",
                            pipeline.name
                        ),
                    ));
                    continue;
                }
            }
        } else {
            pipeline_store
                .create(pipeline)
                .context("failed to create pipeline")?
        };

        imported.push((record.pipeline.name.clone(), record.id.to_string()));
    }

    match format {
        OutputFormat::Human => {
            if !imported.is_empty() {
                println!(
                    "Imported {} pipelines from {}/",
                    imported.len(),
                    dir.display()
                );
                for (name, id) in &imported {
                    println!("  `{name}` (id: {id})");
                }
            }
            if !errors.is_empty() {
                eprintln!("{} files failed:", errors.len());
                for (path, err) in &errors {
                    eprintln!("  {path}: {err}");
                }
            }
        }
        OutputFormat::Json => {
            let items: Vec<_> = imported
                .iter()
                .map(|(name, id)| serde_json::json!({ "pipeline": name, "id": id }))
                .collect();
            let errs: Vec<_> = errors
                .iter()
                .map(|(path, err)| serde_json::json!({ "file": path, "error": err }))
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &serde_json::json!({ "imported": items, "errors": errs })
                )?
            );
        }
    }

    if !errors.is_empty() && imported.is_empty() {
        anyhow::bail!("all imports failed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_no_subcommand() {
        let cli = Cli::try_parse_from(["armillary"]).unwrap();
        assert!(cli.command.is_none());
        assert!(!cli.json);
    }

    #[test]
    fn parse_start_defaults() {
        let cli = Cli::try_parse_from(["armillary", "start"]).unwrap();
        match cli.command {
            Some(Command::Start {
                port,
                host,
                headless,
                dev,
            }) => {
                assert_eq!(port, 8080);
                assert_eq!(host, std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
                assert!(!headless);
                assert!(!dev);
            }
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn parse_start_with_flags() {
        let cli =
            Cli::try_parse_from(["armillary", "start", "--port", "9090", "--headless"]).unwrap();
        match cli.command {
            Some(Command::Start { port, headless, .. }) => {
                assert_eq!(port, 9090);
                assert!(headless);
            }
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn parse_stop() {
        let cli = Cli::try_parse_from(["armillary", "stop"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Stop)));
    }

    #[test]
    fn parse_status() {
        let cli = Cli::try_parse_from(["armillary", "status"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Status)));
    }

    #[test]
    fn parse_global_json_flag() {
        let cli = Cli::try_parse_from(["armillary", "--json", "status"]).unwrap();
        assert!(cli.json);
        assert!(matches!(cli.command, Some(Command::Status)));
    }

    #[test]
    fn parse_json_flag_after_subcommand() {
        let cli = Cli::try_parse_from(["armillary", "stop", "--json"]).unwrap();
        assert!(cli.json);
        assert!(matches!(cli.command, Some(Command::Stop)));
    }

    #[test]
    fn parse_export() {
        let cli =
            Cli::try_parse_from(["armillary", "export", "my-pipeline", "-o", "out.json"]).unwrap();
        match cli.command {
            Some(Command::Export {
                pipeline,
                output,
                all,
            }) => {
                assert_eq!(pipeline.as_deref(), Some("my-pipeline"));
                assert_eq!(output.unwrap().to_str().unwrap(), "out.json");
                assert!(!all);
            }
            _ => panic!("expected Export"),
        }
    }

    #[test]
    fn parse_export_all() {
        let cli =
            Cli::try_parse_from(["armillary", "export", "--all", "-o", "./pipelines/"]).unwrap();
        match cli.command {
            Some(Command::Export {
                pipeline,
                output,
                all,
            }) => {
                assert!(pipeline.is_none());
                assert_eq!(output.unwrap().to_str().unwrap(), "./pipelines/");
                assert!(all);
            }
            _ => panic!("expected Export"),
        }
    }

    #[test]
    fn parse_import_directory() {
        let cli = Cli::try_parse_from(["armillary", "import", "./pipelines/"]).unwrap();
        match cli.command {
            Some(Command::Import { file, on_conflict }) => {
                assert_eq!(file.to_str().unwrap(), "./pipelines/");
                assert_eq!(on_conflict, "reject");
            }
            _ => panic!("expected Import"),
        }
    }

    #[test]
    fn parse_import() {
        let cli = Cli::try_parse_from([
            "armillary",
            "import",
            "pipeline.json",
            "--on-conflict",
            "rename",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Import {
                file, on_conflict, ..
            }) => {
                assert_eq!(file.to_str().unwrap(), "pipeline.json");
                assert_eq!(on_conflict, "rename");
            }
            _ => panic!("expected Import"),
        }
    }

    #[test]
    fn exit_code_constants() {
        assert_eq!(EXIT_PIPELINE_FAILURE, 2);
    }

    #[test]
    fn sanitize_name_replaces_special_chars() {
        assert_eq!(sanitize_name("My Pipeline!"), "My_Pipeline_");
        assert_eq!(sanitize_name("hello-world_2"), "hello-world_2");
        assert_eq!(sanitize_name("a/b c.d"), "a_b_c_d");
    }

    #[test]
    fn parse_run_minimal() {
        let cli = Cli::try_parse_from(["armillary", "run", "my-pipe"]).unwrap();
        match cli.command {
            Some(Command::Run {
                pipeline,
                env,
                var,
                dry_run,
                ..
            }) => {
                assert_eq!(pipeline, "my-pipe");
                assert!(env.is_none());
                assert!(var.is_empty());
                assert!(!dry_run);
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn parse_run_full() {
        let cli = Cli::try_parse_from([
            "armillary",
            "run",
            "etl",
            "--env",
            "prod",
            "-V",
            "date=2026-03-28",
            "-V",
            "region=midwest",
            "--dry-run",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Run {
                pipeline,
                env,
                var,
                dry_run,
                ..
            }) => {
                assert_eq!(pipeline, "etl");
                assert_eq!(env.as_deref(), Some("prod"));
                assert_eq!(var.len(), 2);
                assert_eq!(var[0], ("date".into(), "2026-03-28".into()));
                assert_eq!(var[1], ("region".into(), "midwest".into()));
                assert!(dry_run);
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn parse_run_full_refresh_and_bootstrap() {
        let cli = Cli::try_parse_from([
            "armillary",
            "run",
            "etl",
            "--full-refresh",
            "--bootstrap-incremental",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Run {
                full_refresh,
                bootstrap_incremental,
                ..
            }) => {
                assert!(full_refresh);
                assert!(bootstrap_incremental);
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn parse_incremental_subcommands() {
        for args in [
            vec![
                "armillary",
                "incremental",
                "reset",
                "p",
                "n",
                "--env",
                "dev",
            ],
            vec!["armillary", "incremental", "status", "p"],
            vec!["armillary", "incremental", "list"],
            vec!["armillary", "incremental", "plan", "p", "--env", "prod"],
        ] {
            let cli = Cli::try_parse_from(args).unwrap();
            assert!(matches!(cli.command, Some(Command::Incremental { .. })));
        }
    }

    #[test]
    fn parse_list() {
        let cli = Cli::try_parse_from(["armillary", "list"]).unwrap();
        assert!(matches!(cli.command, Some(Command::List)));
    }

    #[test]
    fn parse_show() {
        let cli = Cli::try_parse_from(["armillary", "show", "my-pipe"]).unwrap();
        match cli.command {
            Some(Command::Show { pipeline }) => {
                assert_eq!(pipeline, "my-pipe");
            }
            _ => panic!("expected Show"),
        }
    }

    #[test]
    fn parse_history() {
        let cli =
            Cli::try_parse_from(["armillary", "history", "my-pipe", "--limit", "50"]).unwrap();
        match cli.command {
            Some(Command::History { pipeline, limit }) => {
                assert_eq!(pipeline, "my-pipe");
                assert_eq!(limit, 50);
            }
            _ => panic!("expected History"),
        }
    }

    #[test]
    fn parse_history_default_limit() {
        let cli = Cli::try_parse_from(["armillary", "history", "my-pipe"]).unwrap();
        match cli.command {
            Some(Command::History { limit, .. }) => {
                assert_eq!(limit, 20);
            }
            _ => panic!("expected History"),
        }
    }

    #[test]
    fn parse_env_list() {
        let cli = Cli::try_parse_from(["armillary", "env", "list"]).unwrap();
        match cli.command {
            Some(Command::Env { action }) => {
                assert!(matches!(action, environment::EnvAction::List));
            }
            _ => panic!("expected Env"),
        }
    }

    #[test]
    fn parse_env_create_with_fallback() {
        let cli = Cli::try_parse_from([
            "armillary",
            "env",
            "create",
            "staging",
            "--fallback",
            "prod",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Env { action }) => match action {
                environment::EnvAction::Create { name, fallback } => {
                    assert_eq!(name, "staging");
                    assert_eq!(fallback.as_deref(), Some("prod"));
                }
                _ => panic!("expected Create"),
            },
            _ => panic!("expected Env"),
        }
    }

    #[test]
    fn parse_env_create_no_fallback() {
        let cli = Cli::try_parse_from(["armillary", "env", "create", "isolated"]).unwrap();
        match cli.command {
            Some(Command::Env { action }) => match action {
                environment::EnvAction::Create { name, fallback } => {
                    assert_eq!(name, "isolated");
                    assert!(fallback.is_none());
                }
                _ => panic!("expected Create"),
            },
            _ => panic!("expected Env"),
        }
    }

    #[test]
    fn parse_env_delete() {
        let cli = Cli::try_parse_from(["armillary", "env", "delete", "staging"]).unwrap();
        match cli.command {
            Some(Command::Env { action }) => match action {
                environment::EnvAction::Delete { name } => {
                    assert_eq!(name, "staging");
                }
                _ => panic!("expected Delete"),
            },
            _ => panic!("expected Env"),
        }
    }

    #[test]
    fn parse_env_show() {
        let cli = Cli::try_parse_from(["armillary", "env", "show", "dev"]).unwrap();
        match cli.command {
            Some(Command::Env { action }) => match action {
                environment::EnvAction::Show { name } => {
                    assert_eq!(name, "dev");
                }
                _ => panic!("expected Show"),
            },
            _ => panic!("expected Env"),
        }
    }

    #[test]
    fn parse_secret_set_from_env() {
        let cli = Cli::try_parse_from([
            "armillary",
            "secret",
            "set",
            "api_key",
            "--from-env",
            "MY_API_KEY",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Secret { action }) => match action {
                secret::SecretAction::Set {
                    name,
                    value,
                    env,
                    from_env,
                } => {
                    assert_eq!(name, "api_key");
                    assert!(value.is_none());
                    assert!(env.is_none());
                    assert_eq!(from_env.as_deref(), Some("MY_API_KEY"));
                }
                _ => panic!("expected Set"),
            },
            _ => panic!("expected Secret"),
        }
    }

    #[test]
    fn parse_secret_set_from_env_conflicts_with_value() {
        let result = Cli::try_parse_from([
            "armillary",
            "secret",
            "set",
            "api_key",
            "literal_value",
            "--from-env",
            "MY_API_KEY",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_preview() {
        let cli = Cli::try_parse_from(["armillary", "preview", "my-pipe", "-V", "date=2026-03-28"])
            .unwrap();
        match cli.command {
            Some(Command::Preview { pipeline, var }) => {
                assert_eq!(pipeline, "my-pipe");
                assert_eq!(var.len(), 1);
                assert_eq!(var[0], ("date".into(), "2026-03-28".into()));
            }
            _ => panic!("expected Preview"),
        }
    }

    #[test]
    fn parse_plugin_subcommands() {
        for (args, want) in [
            (vec!["armillary", "plugin", "list"], "list"),
            (vec!["armillary", "plugin", "info", "openboard"], "info"),
            (vec!["armillary", "plugin", "check", "openboard"], "check"),
            (vec!["armillary", "plugin", "path"], "path"),
        ] {
            let cli = Cli::try_parse_from(args).unwrap();
            match cli.command {
                Some(Command::Plugin { action }) => match (want, action) {
                    ("list", plugin::PluginAction::List) => {}
                    ("info", plugin::PluginAction::Info { name }) => assert_eq!(name, "openboard"),
                    ("check", plugin::PluginAction::Check { name }) => {
                        assert_eq!(name, "openboard")
                    }
                    ("path", plugin::PluginAction::Path) => {}
                    _ => panic!("wrong plugin action for {want}"),
                },
                _ => panic!("expected Plugin"),
            }
        }
    }

    #[test]
    fn parse_metadata_init() {
        let cli = Cli::try_parse_from([
            "armillary",
            "metadata",
            "init",
            "--url",
            "postgresql://localhost/test",
        ])
        .unwrap();
        assert!(matches!(cli.command, Some(Command::Metadata { .. })));
    }

    #[test]
    fn parse_metadata_migrate() {
        let cli = Cli::try_parse_from(["armillary", "metadata", "migrate"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Metadata { .. })));
    }

    #[test]
    fn parse_metadata_export() {
        let cli = Cli::try_parse_from([
            "armillary",
            "metadata",
            "export",
            "--to",
            "postgresql://localhost/target",
        ])
        .unwrap();
        assert!(matches!(cli.command, Some(Command::Metadata { .. })));
    }

    #[test]
    fn parse_metadata_import() {
        let cli = Cli::try_parse_from([
            "armillary",
            "metadata",
            "import",
            "--from",
            "postgresql://localhost/source",
        ])
        .unwrap();
        assert!(matches!(cli.command, Some(Command::Metadata { .. })));
    }

    #[test]
    fn parse_test_single_pipeline() {
        let cli = Cli::try_parse_from(["armillary", "test", "my-pipe"]).unwrap();
        match cli.command {
            Some(Command::Test {
                pipeline,
                env,
                var,
                node,
                all,
            }) => {
                assert_eq!(pipeline.as_deref(), Some("my-pipe"));
                assert!(env.is_none());
                assert!(var.is_empty());
                assert!(node.is_none());
                assert!(!all);
            }
            _ => panic!("expected Test"),
        }
    }

    #[test]
    fn parse_test_with_node_filter() {
        let cli =
            Cli::try_parse_from(["armillary", "test", "my-pipe", "--node", "validate_orders"])
                .unwrap();
        match cli.command {
            Some(Command::Test { pipeline, node, .. }) => {
                assert_eq!(pipeline.as_deref(), Some("my-pipe"));
                assert_eq!(node.as_deref(), Some("validate_orders"));
            }
            _ => panic!("expected Test"),
        }
    }

    #[test]
    fn parse_lineage_subcommands() {
        for args in [
            vec!["armillary", "lineage", "graph"],
            vec!["armillary", "lineage", "graph", "--dot"],
            vec!["armillary", "lineage", "graph", "--env", "prod"],
            vec!["armillary", "lineage", "upstream", "my-pipe"],
            vec!["armillary", "lineage", "downstream", "my-pipe", "-e", "dev"],
            vec!["armillary", "lineage", "impact", "my-pipe"],
            vec!["armillary", "lineage", "cycles"],
            vec!["armillary", "lineage", "orphans", "--env", "prod"],
        ] {
            let cli = Cli::try_parse_from(args).unwrap();
            assert!(matches!(cli.command, Some(Command::Lineage { .. })));
        }
    }

    #[test]
    fn parse_trigger_list() {
        let cli = Cli::try_parse_from(["armillary", "trigger", "list"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Trigger { .. })));
    }

    #[test]
    fn parse_trigger_list_with_filters() {
        let cli = Cli::try_parse_from([
            "armillary",
            "trigger",
            "list",
            "--pipeline",
            "my-pipe",
            "--env",
            "prod",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Trigger { action }) => match *action {
                trigger::TriggerAction::List { pipeline, env } => {
                    assert_eq!(pipeline.as_deref(), Some("my-pipe"));
                    assert_eq!(env.as_deref(), Some("prod"));
                }
                _ => panic!("expected List"),
            },
            _ => panic!("expected Trigger"),
        }
    }

    #[test]
    fn parse_trigger_show() {
        let cli = Cli::try_parse_from([
            "armillary",
            "trigger",
            "show",
            "550e8400-e29b-41d4-a716-446655440000",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Trigger { action }) => match *action {
                trigger::TriggerAction::Show { trigger_id } => {
                    assert_eq!(trigger_id, "550e8400-e29b-41d4-a716-446655440000");
                }
                _ => panic!("expected Show"),
            },
            _ => panic!("expected Trigger"),
        }
    }

    #[test]
    fn parse_trigger_enable_disable() {
        for (sub, check_enable) in [("enable", true), ("disable", false)] {
            let cli = Cli::try_parse_from([
                "armillary",
                "trigger",
                sub,
                "550e8400-e29b-41d4-a716-446655440000",
            ])
            .unwrap();
            match cli.command {
                Some(Command::Trigger { action }) => {
                    if check_enable {
                        assert!(matches!(*action, trigger::TriggerAction::Enable { .. }));
                    } else {
                        assert!(matches!(*action, trigger::TriggerAction::Disable { .. }));
                    }
                }
                _ => panic!("expected Trigger"),
            }
        }
    }

    #[test]
    fn parse_trigger_create_cron() {
        let cli = Cli::try_parse_from([
            "armillary",
            "trigger",
            "create",
            "--pipeline",
            "etl",
            "--kind",
            "cron",
            "--expression",
            "0 */6 * * *",
            "--env",
            "prod",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Trigger { action }) => match *action {
                trigger::TriggerAction::Create {
                    pipeline,
                    kind,
                    expression,
                    env,
                    ..
                } => {
                    assert_eq!(pipeline, "etl");
                    assert_eq!(kind, "cron");
                    assert_eq!(expression.as_deref(), Some("0 */6 * * *"));
                    assert_eq!(env, "prod");
                }
                _ => panic!("expected Create"),
            },
            _ => panic!("expected Trigger"),
        }
    }

    #[test]
    fn parse_trigger_delete() {
        let cli = Cli::try_parse_from([
            "armillary",
            "trigger",
            "delete",
            "550e8400-e29b-41d4-a716-446655440000",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Trigger { action }) => {
                assert!(matches!(*action, trigger::TriggerAction::Delete { .. }));
            }
            _ => panic!("expected Trigger"),
        }
    }

    #[test]
    fn parse_trigger_history() {
        let cli = Cli::try_parse_from([
            "armillary",
            "trigger",
            "history",
            "550e8400-e29b-41d4-a716-446655440000",
            "--limit",
            "25",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Trigger { action }) => match *action {
                trigger::TriggerAction::History { trigger_id, limit } => {
                    assert_eq!(trigger_id, "550e8400-e29b-41d4-a716-446655440000");
                    assert_eq!(limit, 25);
                }
                _ => panic!("expected History"),
            },
            _ => panic!("expected Trigger"),
        }
    }

    #[test]
    fn parse_trigger_history_default_limit() {
        let cli = Cli::try_parse_from([
            "armillary",
            "trigger",
            "history",
            "550e8400-e29b-41d4-a716-446655440000",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Trigger { action }) => match *action {
                trigger::TriggerAction::History { limit, .. } => {
                    assert_eq!(limit, 50);
                }
                _ => panic!("expected History"),
            },
            _ => panic!("expected Trigger"),
        }
    }

    #[test]
    fn parse_trigger_fire() {
        let cli = Cli::try_parse_from([
            "armillary",
            "trigger",
            "fire",
            "550e8400-e29b-41d4-a716-446655440000",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Trigger { action }) => {
                assert!(matches!(*action, trigger::TriggerAction::Fire { .. }));
            }
            _ => panic!("expected Trigger"),
        }
    }

    #[test]
    fn parse_test_all() {
        let cli = Cli::try_parse_from(["armillary", "test", "--all"]).unwrap();
        match cli.command {
            Some(Command::Test { pipeline, all, .. }) => {
                assert!(pipeline.is_none());
                assert!(all);
            }
            _ => panic!("expected Test"),
        }
    }

    #[test]
    fn parse_backfill_start_date_range() {
        let cli = Cli::try_parse_from([
            "armillary",
            "backfill",
            "start",
            "my-pipeline",
            "--env",
            "prod",
            "--date-range",
            "2024-01-01..2024-01-31",
            "--granularity",
            "day",
            "--var-mapping",
            "run_date=$iteration.start",
            "--concurrency",
            "4",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Backfill { action }) => match action {
                backfill::BackfillAction::Start {
                    pipeline,
                    env,
                    date_range,
                    granularity,
                    concurrency,
                    ..
                } => {
                    assert_eq!(pipeline, "my-pipeline");
                    assert_eq!(env, "prod");
                    assert_eq!(date_range.as_deref(), Some("2024-01-01..2024-01-31"));
                    assert_eq!(granularity, "day");
                    assert_eq!(concurrency, 4);
                }
                _ => panic!("expected Start"),
            },
            _ => panic!("expected Backfill"),
        }
    }

    #[test]
    fn parse_backfill_start_list() {
        let cli = Cli::try_parse_from([
            "armillary",
            "backfill",
            "start",
            "my-pipeline",
            "--list",
            "US,EU,APAC",
            "--var-mapping",
            "region=$iteration.value",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Backfill { action }) => match action {
                backfill::BackfillAction::Start { list, .. } => {
                    assert_eq!(list.as_deref(), Some("US,EU,APAC"));
                }
                _ => panic!("expected Start"),
            },
            _ => panic!("expected Backfill"),
        }
    }

    #[test]
    fn parse_backfill_list() {
        let cli = Cli::try_parse_from([
            "armillary",
            "backfill",
            "list",
            "--pipeline",
            "my-pipe",
            "--status",
            "running",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Backfill { action }) => match action {
                backfill::BackfillAction::List {
                    pipeline, status, ..
                } => {
                    assert_eq!(pipeline.as_deref(), Some("my-pipe"));
                    assert_eq!(status.as_deref(), Some("running"));
                }
                _ => panic!("expected List"),
            },
            _ => panic!("expected Backfill"),
        }
    }

    #[test]
    fn parse_backfill_status() {
        let cli = Cli::try_parse_from([
            "armillary",
            "backfill",
            "status",
            "550e8400-e29b-41d4-a716-446655440000",
        ])
        .unwrap();
        assert!(matches!(cli.command, Some(Command::Backfill { .. })));
    }

    #[test]
    fn parse_backfill_resume() {
        let cli = Cli::try_parse_from([
            "armillary",
            "backfill",
            "resume",
            "550e8400-e29b-41d4-a716-446655440000",
        ])
        .unwrap();
        assert!(matches!(cli.command, Some(Command::Backfill { .. })));
    }

    #[test]
    fn parse_backfill_cancel() {
        let cli = Cli::try_parse_from([
            "armillary",
            "backfill",
            "cancel",
            "550e8400-e29b-41d4-a716-446655440000",
        ])
        .unwrap();
        assert!(matches!(cli.command, Some(Command::Backfill { .. })));
    }

    #[test]
    fn parse_backfill_delete() {
        let cli = Cli::try_parse_from([
            "armillary",
            "backfill",
            "delete",
            "550e8400-e29b-41d4-a716-446655440000",
        ])
        .unwrap();
        assert!(matches!(cli.command, Some(Command::Backfill { .. })));
    }
}
