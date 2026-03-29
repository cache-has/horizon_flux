// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

pub mod color;
mod environment;
mod pipeline;
mod secret;
mod server;

/// Exit code for pipeline execution failures (distinct from general errors).
/// Used by `flux run` when the pipeline itself fails (vs. a CLI/config error).
#[allow(dead_code)]
const EXIT_PIPELINE_FAILURE: u8 = 2;

/// Output format: human-readable (default) or JSON for scripting.
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Human,
    Json,
}

#[derive(Parser)]
#[command(
    name = "horizon-flux",
    version,
    about = "Horizon Flux — visual data pipeline builder"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Output results as JSON instead of human-readable text.
    #[arg(long, global = true)]
    json: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Start the Horizon Flux server.
    Start {
        /// Port number for the web server.
        #[arg(long, short, default_value_t = 8080)]
        port: u16,

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
}

fn main() -> ExitCode {
    color::init();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let format = if cli.json {
        OutputFormat::Json
    } else {
        OutputFormat::Human
    };

    match run(cli, format) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{} {e:#}", color::red("Error:"));
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli, format: OutputFormat) -> Result<()> {
    match cli.command {
        // Default (no subcommand) = start the server.
        None => server::start(8080, false, false),

        Some(Command::Start {
            port,
            headless,
            dev,
        }) => server::start(port, headless, dev),

        Some(Command::Stop) => server::handle(server::ServerAction::Stop, format),

        Some(Command::Status) => server::handle(server::ServerAction::Status, format),

        Some(Command::Secret { action }) => secret::handle(action).context("secret command failed"),

        Some(Command::Env { action }) => {
            environment::handle(action, format).context("env command failed")
        }

        Some(Command::Export {
            pipeline,
            output,
            all,
        }) => {
            if all {
                export_all(output.as_deref(), format)
            } else {
                let pipeline = pipeline
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("pipeline name or UUID required (or use --all)"))?;
                export_pipeline(pipeline, output.as_deref(), format)
            }
        }

        Some(Command::Import { file, on_conflict }) => {
            if file.is_dir() {
                import_directory(&file, &on_conflict, format)
            } else {
                import_pipeline(&file, &on_conflict, format)
            }
        }

        Some(Command::Run {
            pipeline,
            env,
            var,
            dry_run,
        }) => pipeline::run(&pipeline, env.as_deref(), var, dry_run, format),

        Some(Command::List) => pipeline::list(format),

        Some(Command::Show { pipeline }) => pipeline::show(&pipeline, format),

        Some(Command::History { pipeline, limit }) => pipeline::history(&pipeline, limit, format),

        Some(Command::Preview { pipeline, var }) => pipeline::preview(&pipeline, var, format),
    }
}

fn export_pipeline(
    pipeline: &str,
    output: Option<&std::path::Path>,
    format: OutputFormat,
) -> Result<()> {
    let data_dir = dirs::home_dir()
        .context("could not determine home directory")?
        .join(".horizon-flux");
    let pipelines_dir = data_dir.join("pipelines");
    let pipeline_store =
        flux_engine::PipelineStore::open(&data_dir.join("pipelines.db"), &pipelines_dir)
            .context("failed to open pipeline store")?;

    let record = if let Ok(id) = pipeline.parse::<flux_engine::PipelineId>() {
        pipeline_store.get(&id).context("failed to read pipeline")?
    } else {
        pipeline_store
            .get_by_name(pipeline)
            .context("failed to read pipeline")?
    }
    .ok_or_else(|| anyhow::anyhow!("pipeline `{pipeline}` not found"))?;

    let json = record
        .pipeline
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

fn export_all(output_dir: Option<&std::path::Path>, format: OutputFormat) -> Result<()> {
    let data_dir = dirs::home_dir()
        .context("could not determine home directory")?
        .join(".horizon-flux");
    let pipelines_dir = data_dir.join("pipelines");
    let pipeline_store =
        flux_engine::PipelineStore::open(&data_dir.join("pipelines.db"), &pipelines_dir)
            .context("failed to open pipeline store")?;

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
        let json = record
            .pipeline
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
            println!("Exported {} pipelines to {}/", exported.len(), out_dir.display());
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
            println!("{}", serde_json::to_string_pretty(&serde_json::json!({ "exported": items }))?);
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

fn import_pipeline(file: &std::path::Path, on_conflict: &str, format: OutputFormat) -> Result<()> {
    let data_dir = dirs::home_dir()
        .context("could not determine home directory")?
        .join(".horizon-flux");
    let pipelines_dir = data_dir.join("pipelines");
    std::fs::create_dir_all(&data_dir).context("failed to create data directory")?;
    let pipeline_store =
        flux_engine::PipelineStore::open(&data_dir.join("pipelines.db"), &pipelines_dir)
            .context("failed to open pipeline store")?;

    let json = std::fs::read_to_string(file)
        .with_context(|| format!("failed to read {}", file.display()))?;

    let (mut pipeline, warnings) = flux_engine::Pipeline::from_json_with_warnings(&json)
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
) -> Result<()> {
    let data_dir = dirs::home_dir()
        .context("could not determine home directory")?
        .join(".horizon-flux");
    let pipelines_dir = data_dir.join("pipelines");
    std::fs::create_dir_all(&data_dir).context("failed to create data directory")?;
    let pipeline_store =
        flux_engine::PipelineStore::open(&data_dir.join("pipelines.db"), &pipelines_dir)
            .context("failed to open pipeline store")?;

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

        let (mut pipeline, warnings) = match flux_engine::Pipeline::from_json_with_warnings(&json) {
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
                println!("Imported {} pipelines from {}/", imported.len(), dir.display());
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
        let cli = Cli::try_parse_from(["horizon-flux"]).unwrap();
        assert!(cli.command.is_none());
        assert!(!cli.json);
    }

    #[test]
    fn parse_start_defaults() {
        let cli = Cli::try_parse_from(["horizon-flux", "start"]).unwrap();
        match cli.command {
            Some(Command::Start {
                port,
                headless,
                dev,
            }) => {
                assert_eq!(port, 8080);
                assert!(!headless);
                assert!(!dev);
            }
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn parse_start_with_flags() {
        let cli =
            Cli::try_parse_from(["horizon-flux", "start", "--port", "9090", "--headless"]).unwrap();
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
        let cli = Cli::try_parse_from(["horizon-flux", "stop"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Stop)));
    }

    #[test]
    fn parse_status() {
        let cli = Cli::try_parse_from(["horizon-flux", "status"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Status)));
    }

    #[test]
    fn parse_global_json_flag() {
        let cli = Cli::try_parse_from(["horizon-flux", "--json", "status"]).unwrap();
        assert!(cli.json);
        assert!(matches!(cli.command, Some(Command::Status)));
    }

    #[test]
    fn parse_json_flag_after_subcommand() {
        let cli = Cli::try_parse_from(["horizon-flux", "stop", "--json"]).unwrap();
        assert!(cli.json);
        assert!(matches!(cli.command, Some(Command::Stop)));
    }

    #[test]
    fn parse_export() {
        let cli = Cli::try_parse_from(["horizon-flux", "export", "my-pipeline", "-o", "out.json"])
            .unwrap();
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
            Cli::try_parse_from(["horizon-flux", "export", "--all", "-o", "./pipelines/"]).unwrap();
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
        let cli = Cli::try_parse_from(["horizon-flux", "import", "./pipelines/"]).unwrap();
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
            "horizon-flux",
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
        let cli = Cli::try_parse_from(["horizon-flux", "run", "my-pipe"]).unwrap();
        match cli.command {
            Some(Command::Run {
                pipeline,
                env,
                var,
                dry_run,
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
            "horizon-flux",
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
    fn parse_list() {
        let cli = Cli::try_parse_from(["horizon-flux", "list"]).unwrap();
        assert!(matches!(cli.command, Some(Command::List)));
    }

    #[test]
    fn parse_show() {
        let cli = Cli::try_parse_from(["horizon-flux", "show", "my-pipe"]).unwrap();
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
            Cli::try_parse_from(["horizon-flux", "history", "my-pipe", "--limit", "50"]).unwrap();
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
        let cli = Cli::try_parse_from(["horizon-flux", "history", "my-pipe"]).unwrap();
        match cli.command {
            Some(Command::History { limit, .. }) => {
                assert_eq!(limit, 20);
            }
            _ => panic!("expected History"),
        }
    }

    #[test]
    fn parse_env_list() {
        let cli = Cli::try_parse_from(["horizon-flux", "env", "list"]).unwrap();
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
            "horizon-flux",
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
        let cli = Cli::try_parse_from(["horizon-flux", "env", "create", "isolated"]).unwrap();
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
        let cli = Cli::try_parse_from(["horizon-flux", "env", "delete", "staging"]).unwrap();
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
        let cli = Cli::try_parse_from(["horizon-flux", "env", "show", "dev"]).unwrap();
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
            "horizon-flux",
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
            "horizon-flux",
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
        let cli = Cli::try_parse_from([
            "horizon-flux",
            "preview",
            "my-pipe",
            "-V",
            "date=2026-03-28",
        ])
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
}
