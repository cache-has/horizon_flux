// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Python transform runtime.
//!
//! Executes Python transforms by spawning a subprocess that communicates via
//! Arrow IPC files. The protocol:
//!
//! 1. Write each upstream input as an Arrow IPC file in a temp directory
//! 2. Write a JSON manifest with input paths, pipeline params, and user code
//! 3. Spawn `python3 python_runner.py <manifest> <output>`
//! 4. Read the output Arrow IPC file back as `RecordBatch`es
//!
//! This approach was chosen over PyO3 (embedded interpreter) for:
//! - Build simplicity: no compile-time Python version coupling
//! - Process isolation: no GIL concerns, clean error boundaries
//! - Flexibility: uses whatever Python the user has installed

use crate::error::NodeErrorKind;
use crate::python_env;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use flux_engine::NodeId;
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tracing::{debug, warn};

/// The Python runner script, embedded at compile time.
const RUNNER_SCRIPT: &str = include_str!("python_runner.py");

/// The Polars lineage walker, embedded at compile time.
const LINEAGE_SCRIPT: &str = include_str!("polars_lineage.py");

/// JSON manifest sent to the Python subprocess.
#[derive(Serialize)]
struct Manifest {
    inputs: HashMap<String, String>,
    params: HashMap<String, serde_json::Value>,
    code: String,
    /// Path where the Python runner should write column lineage JSON (if
    /// the user's transform returns a LazyFrame).
    #[serde(skip_serializing_if = "Option::is_none")]
    lineage_path: Option<String>,
}

/// Result of executing a Python transform, including optional column lineage.
#[derive(Debug)]
pub struct PythonTransformResult {
    /// The output Arrow RecordBatches.
    pub batches: Vec<RecordBatch>,
    /// Column lineage extracted from a LazyFrame plan, if available.
    pub lineage: Option<PythonColumnLineage>,
}

/// Column lineage extracted from a Polars LazyFrame plan.
#[derive(Debug, Clone)]
pub struct PythonColumnLineage {
    /// Individual column edges.
    pub edges: Vec<PythonColumnEdge>,
    /// Confidence level: "lazyframe" or "opaque".
    pub confidence: String,
    /// Any warnings from the lineage walker.
    pub warnings: Vec<String>,
}

/// A single column lineage edge from the Python walker.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PythonColumnEdge {
    pub upstream_column: String,
    pub downstream_column: String,
    pub relationship: String,
    #[serde(default)]
    pub expression_text: Option<String>,
}

/// Raw JSON structure returned by the Python lineage walker.
#[derive(serde::Deserialize)]
struct RawPythonLineage {
    edges: Vec<PythonColumnEdge>,
    confidence: String,
    #[serde(default)]
    warnings: Vec<String>,
}

/// Configuration for Python subprocess execution.
///
/// Defaults are read from environment variables at construction time:
/// - `HORIZON_FLUX_PYTHON_TIMEOUT`: max seconds (default 300 = 5 minutes)
/// - `HORIZON_FLUX_PYTHON_MEMORY_LIMIT`: max RSS in bytes (default: none)
#[derive(Debug, Clone)]
pub struct PythonConfig {
    /// Maximum wall-clock time before the subprocess is killed.
    pub timeout: Duration,
    /// Best-effort RSS memory limit in bytes. `None` disables monitoring.
    pub memory_limit: Option<usize>,
}

impl Default for PythonConfig {
    fn default() -> Self {
        let timeout = std::env::var("HORIZON_FLUX_PYTHON_TIMEOUT")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(300));

        let memory_limit = std::env::var("HORIZON_FLUX_PYTHON_MEMORY_LIMIT")
            .ok()
            .and_then(|s| s.parse::<usize>().ok());

        Self {
            timeout,
            memory_limit,
        }
    }
}

/// Execute a Python transform as a subprocess.
///
/// Serializes upstream Arrow data to IPC files, invokes the Python runner with
/// a JSON manifest, and reads the output IPC file back into `RecordBatch`es.
///
/// The subprocess is killed if it exceeds the configured timeout or memory
/// limit (see [`PythonConfig`]).
pub async fn execute_python_transform(
    code: &str,
    upstream_data: HashMap<NodeId, &Vec<RecordBatch>>,
    variables: &HashMap<String, serde_json::Value>,
    config: &PythonConfig,
) -> Result<PythonTransformResult, NodeErrorKind> {
    // Create a temp directory for this transform's IPC exchange.
    let tmp_dir = tempfile::tempdir()
        .map_err(|e| NodeErrorKind::Python(format!("failed to create temp directory: {e}")))?;
    let tmp_path = tmp_dir.path();

    // Write each upstream input as an Arrow IPC file.
    let mut input_paths: HashMap<String, String> = HashMap::new();
    for (node_id, batches) in &upstream_data {
        if batches.is_empty() {
            continue;
        }
        let ipc_path = tmp_path.join(format!("{}.arrow", node_id));
        write_ipc(&ipc_path, batches)?;
        input_paths.insert(node_id.to_string(), ipc_path.to_string_lossy().into_owned());
    }

    // Lineage sidecar path.
    let lineage_path = tmp_path.join("lineage.json");

    // Write the manifest.
    let manifest = Manifest {
        inputs: input_paths,
        params: variables.clone(),
        code: code.to_string(),
        lineage_path: Some(lineage_path.to_string_lossy().into_owned()),
    };
    let manifest_path = tmp_path.join("manifest.json");
    let manifest_json = serde_json::to_string(&manifest)
        .map_err(|e| NodeErrorKind::Python(format!("failed to serialize manifest: {e}")))?;
    std::fs::write(&manifest_path, &manifest_json)
        .map_err(|e| NodeErrorKind::Python(format!("failed to write manifest: {e}")))?;

    // Write the runner script to the temp directory.
    let runner_path = tmp_path.join("_runner.py");
    std::fs::write(&runner_path, RUNNER_SCRIPT)
        .map_err(|e| NodeErrorKind::Python(format!("failed to write runner script: {e}")))?;

    // Write the lineage walker script so the runner can import it.
    let lineage_script_path = tmp_path.join("polars_lineage.py");
    std::fs::write(&lineage_script_path, LINEAGE_SCRIPT)
        .map_err(|e| NodeErrorKind::Python(format!("failed to write lineage script: {e}")))?;

    // Output path for the result.
    let output_path = tmp_path.join("output.arrow");

    // Ensure the managed Python environment exists (best-effort — if uv is not
    // available we still fall through to system Python via find_python).
    match tokio::task::spawn_blocking(python_env::ensure_python_env).await {
        Ok(Ok(path)) => {
            debug!(path = %path.display(), "managed Python environment ready");
        }
        Ok(Err(e)) => {
            warn!("managed Python environment not available: {e}");
        }
        Err(e) => {
            warn!("failed to check managed Python environment: {e}");
        }
    }

    // Spawn the Python process.
    let python = find_python();
    debug!(python = %python, timeout = ?config.timeout, memory_limit = ?config.memory_limit,
           "spawning Python subprocess");

    let mut child = tokio::process::Command::new(&python)
        .arg(runner_path.to_string_lossy().as_ref())
        .arg(manifest_path.to_string_lossy().as_ref())
        .arg(output_path.to_string_lossy().as_ref())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                NodeErrorKind::PythonNotFound(python.clone())
            } else {
                NodeErrorKind::Python(format!("failed to spawn {python}: {e}"))
            }
        })?;

    let child_pid = child.id();

    // Take ownership of the pipes so we can read them independently of wait().
    let stdout_pipe = child.stdout.take();
    let stderr_pipe = child.stderr.take();

    let stdout_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut pipe) = stdout_pipe {
            let _ = pipe.read_to_end(&mut buf).await;
        }
        buf
    });
    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut pipe) = stderr_pipe {
            let _ = pipe.read_to_end(&mut buf).await;
        }
        buf
    });

    // Race: process exit vs timeout vs memory limit exceeded.
    let memory_limit = config.memory_limit;
    let timeout = config.timeout;

    enum Outcome {
        Exited(std::io::Result<std::process::ExitStatus>),
        Timeout,
        MemoryExceeded { used_bytes: usize },
    }

    let outcome = tokio::select! {
        status = child.wait() => Outcome::Exited(status),
        _ = tokio::time::sleep(timeout) => Outcome::Timeout,
        used = monitor_memory(child_pid, memory_limit) => {
            Outcome::MemoryExceeded { used_bytes: used }
        }
    };

    match outcome {
        Outcome::Timeout => {
            warn!("Python subprocess timed out after {timeout:?}, killing");
            let _ = child.start_kill();
            let _ = child.wait().await;
            Err(NodeErrorKind::PythonTimeout(timeout))
        }
        Outcome::MemoryExceeded { used_bytes } => {
            let limit = memory_limit.unwrap_or(0);
            warn!(
                used_bytes,
                limit_bytes = limit,
                "Python subprocess exceeded memory limit, killing"
            );
            let _ = child.start_kill();
            let _ = child.wait().await;
            Err(NodeErrorKind::PythonMemoryExceeded {
                used_mb: used_bytes as f64 / (1024.0 * 1024.0),
                limit_mb: limit as f64 / (1024.0 * 1024.0),
            })
        }
        Outcome::Exited(Err(e)) => {
            Err(NodeErrorKind::Python(format!("Python process failed: {e}")))
        }
        Outcome::Exited(Ok(status)) => {
            // Collect pipe output.
            let stderr_bytes = stderr_task.await.unwrap_or_default();
            let stderr = String::from_utf8_lossy(&stderr_bytes);

            if !stderr.is_empty() {
                for line in stderr.as_ref().lines() {
                    debug!(target: "python_transform", "{}", line);
                }
            }

            // Drop stdout — we don't use it but waited so the pipe doesn't block.
            drop(stdout_task.await);

            match status.code() {
                Some(0) => {
                    let batches = read_ipc(&output_path)?;
                    let lineage = read_lineage_sidecar(&lineage_path);
                    Ok(PythonTransformResult { batches, lineage })
                }
                Some(1) => {
                    let message = parse_python_error(&stderr);
                    Err(NodeErrorKind::Python(message))
                }
                Some(2) => {
                    let message = parse_runner_error(&stderr);
                    Err(NodeErrorKind::Python(message))
                }
                Some(code) => Err(NodeErrorKind::Python(format!(
                    "Python process exited with unexpected code {code}: {stderr}"
                ))),
                None => Err(NodeErrorKind::Python(
                    "Python process was killed by a signal".to_string(),
                )),
            }
        }
    }
}

/// Best-effort RSS memory monitor for a subprocess.
///
/// Polls the process's RSS via `ps` every second. Returns the RSS in bytes
/// when it exceeds the configured limit. If no limit is set or the PID is
/// unavailable, the future never completes (allowing `tokio::select!` to
/// ignore it).
async fn monitor_memory(pid: Option<u32>, limit: Option<usize>) -> usize {
    let (pid, limit_bytes) = match (pid, limit) {
        (Some(pid), Some(limit)) => (pid, limit),
        _ => std::future::pending().await,
    };

    let limit_kb = limit_bytes / 1024;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Some(rss_kb) = check_rss(pid).await {
            if rss_kb > limit_kb {
                return rss_kb * 1024;
            }
        }
    }
}

/// Read the RSS (in KiB) of a process via `ps`. Returns `None` if the process
/// has exited or the command fails.
async fn check_rss(pid: u32) -> Option<usize> {
    let output = tokio::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .await
        .ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    text.trim().parse::<usize>().ok()
}

/// Write a set of `RecordBatch`es to an Arrow IPC file.
fn write_ipc(path: &Path, batches: &[RecordBatch]) -> Result<(), NodeErrorKind> {
    if batches.is_empty() {
        return Ok(());
    }

    let schema = batches[0].schema();
    let file = std::fs::File::create(path).map_err(|e| {
        NodeErrorKind::Python(format!("failed to create IPC file {}: {e}", path.display()))
    })?;

    let mut writer = FileWriter::try_new(file, &schema)
        .map_err(|e| NodeErrorKind::Python(format!("failed to create IPC writer: {e}")))?;

    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| NodeErrorKind::Python(format!("failed to write IPC batch: {e}")))?;
    }

    writer
        .finish()
        .map_err(|e| NodeErrorKind::Python(format!("failed to finalize IPC file: {e}")))?;

    Ok(())
}

/// Read `RecordBatch`es from an Arrow IPC file.
fn read_ipc(path: &Path) -> Result<Vec<RecordBatch>, NodeErrorKind> {
    let file = std::fs::File::open(path).map_err(|e| {
        NodeErrorKind::Python(format!(
            "failed to open output IPC file {}: {e}",
            path.display()
        ))
    })?;

    let reader = FileReader::try_new(file, None)
        .map_err(|e| NodeErrorKind::Python(format!("failed to read output IPC: {e}")))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| NodeErrorKind::Python(format!("failed to read IPC batch: {e}")))?;
        batches.push(batch);
    }

    Ok(batches)
}

/// Read the lineage sidecar JSON file written by the Python walker.
///
/// Returns `None` if the file doesn't exist (user returned a DataFrame, not a
/// LazyFrame) or if parsing fails (with a warning logged).
fn read_lineage_sidecar(path: &Path) -> Option<PythonColumnLineage> {
    if !path.exists() {
        return None;
    }

    let contents = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            warn!("failed to read lineage sidecar: {e}");
            return None;
        }
    };

    let raw: RawPythonLineage = match serde_json::from_str(&contents) {
        Ok(r) => r,
        Err(e) => {
            warn!("failed to parse lineage sidecar: {e}");
            return None;
        }
    };

    for w in &raw.warnings {
        debug!(target: "polars_lineage", "walker warning: {}", w);
    }

    Some(PythonColumnLineage {
        edges: raw.edges,
        confidence: raw.confidence,
        warnings: raw.warnings,
    })
}

/// Find a Python 3 interpreter.
///
/// Search order:
/// 1. `HORIZON_FLUX_PYTHON` env var (explicit override)
/// 2. `VIRTUAL_ENV/bin/python3` (active venv)
/// 3. `.venv/bin/python3` relative to the workspace root (project-local venv)
/// 4. `~/.horizon-flux/python/bin/python3` (managed env created by `ensure_python_env`)
/// 5. `python3` on PATH
fn find_python() -> String {
    // 1. Explicit override.
    if let Ok(p) = std::env::var("HORIZON_FLUX_PYTHON") {
        return p;
    }

    // 2. Active virtual environment.
    if let Ok(venv) = std::env::var("VIRTUAL_ENV") {
        let candidate = std::path::PathBuf::from(&venv).join("bin").join("python3");
        if candidate.exists() {
            return candidate.to_string_lossy().into_owned();
        }
    }

    // 3. Local .venv (uv-managed) — walk up from the executable or use
    //    CARGO_MANIFEST_DIR at test time to find the workspace root.
    for env_key in &["CARGO_MANIFEST_DIR", "CARGO_WORKSPACE_DIR"] {
        if let Ok(dir) = std::env::var(env_key) {
            // Walk up to find .venv (CARGO_MANIFEST_DIR points to the crate, not workspace root).
            let mut path = std::path::PathBuf::from(&dir);
            for _ in 0..5 {
                let candidate = path.join(".venv").join("bin").join("python3");
                if candidate.exists() {
                    return candidate.to_string_lossy().into_owned();
                }
                if !path.pop() {
                    break;
                }
            }
        }
    }

    // Also check relative to the current executable.
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut path = dir.to_path_buf();
            for _ in 0..5 {
                let candidate = path.join(".venv").join("bin").join("python3");
                if candidate.exists() {
                    return candidate.to_string_lossy().into_owned();
                }
                if !path.pop() {
                    break;
                }
            }
        }
    }

    // 4. Managed environment (~/.horizon-flux/python/).
    if let Some(managed) = python_env::managed_python_path() {
        if managed.exists() {
            return managed.to_string_lossy().into_owned();
        }
    }

    // 5. System PATH fallback.
    "python3".to_string()
}

/// Parse a Python user error from stderr output.
fn parse_python_error(stderr: &str) -> String {
    let stderr = stderr.trim();
    if stderr.is_empty() {
        return "Python transform failed with no error output".to_string();
    }

    // Strip RUNNER_ERROR / USER_ERROR prefixes for cleaner messages.
    if let Some(msg) = stderr.strip_prefix("USER_ERROR: ") {
        return msg.to_string();
    }

    // For tracebacks, return as-is — they're already informative.
    stderr.to_string()
}

/// Parse a runner infrastructure error from stderr output.
fn parse_runner_error(stderr: &str) -> String {
    let stderr = stderr.trim();
    if let Some(msg) = stderr.strip_prefix("RUNNER_ERROR: ") {
        return msg.to_string();
    }
    if stderr.is_empty() {
        return "Python runner encountered an internal error".to_string();
    }
    stderr.to_string()
}
