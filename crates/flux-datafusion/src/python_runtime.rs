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
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use flux_engine::NodeId;
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use tracing::debug;

/// The Python runner script, embedded at compile time.
const RUNNER_SCRIPT: &str = include_str!("python_runner.py");

/// JSON manifest sent to the Python subprocess.
#[derive(Serialize)]
struct Manifest {
    inputs: HashMap<String, String>,
    params: HashMap<String, serde_json::Value>,
    code: String,
}

/// Execute a Python transform as a subprocess.
///
/// Serializes upstream Arrow data to IPC files, invokes the Python runner with
/// a JSON manifest, and reads the output IPC file back into `RecordBatch`es.
pub async fn execute_python_transform(
    code: &str,
    upstream_data: HashMap<NodeId, &Vec<RecordBatch>>,
    variables: &HashMap<String, serde_json::Value>,
) -> Result<Vec<RecordBatch>, NodeErrorKind> {
    // Create a temp directory for this transform's IPC exchange.
    let tmp_dir = tempfile::tempdir().map_err(|e| {
        NodeErrorKind::Python(format!("failed to create temp directory: {e}"))
    })?;
    let tmp_path = tmp_dir.path();

    // Write each upstream input as an Arrow IPC file.
    let mut input_paths: HashMap<String, String> = HashMap::new();
    for (node_id, batches) in &upstream_data {
        if batches.is_empty() {
            continue;
        }
        let ipc_path = tmp_path.join(format!("{}.arrow", node_id));
        write_ipc(&ipc_path, batches)?;
        input_paths.insert(
            node_id.to_string(),
            ipc_path.to_string_lossy().into_owned(),
        );
    }

    // Write the manifest.
    let manifest = Manifest {
        inputs: input_paths,
        params: variables.clone(),
        code: code.to_string(),
    };
    let manifest_path = tmp_path.join("manifest.json");
    let manifest_json = serde_json::to_string(&manifest).map_err(|e| {
        NodeErrorKind::Python(format!("failed to serialize manifest: {e}"))
    })?;
    std::fs::write(&manifest_path, &manifest_json).map_err(|e| {
        NodeErrorKind::Python(format!("failed to write manifest: {e}"))
    })?;

    // Write the runner script to the temp directory.
    let runner_path = tmp_path.join("_runner.py");
    std::fs::write(&runner_path, RUNNER_SCRIPT).map_err(|e| {
        NodeErrorKind::Python(format!("failed to write runner script: {e}"))
    })?;

    // Output path for the result.
    let output_path = tmp_path.join("output.arrow");

    // Spawn the Python process.
    let python = find_python();
    debug!(python = %python, "spawning Python subprocess");

    let child = tokio::process::Command::new(&python)
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

    let output = child.wait_with_output().await.map_err(|e| {
        NodeErrorKind::Python(format!("Python process failed: {e}"))
    })?;

    // Capture stderr for diagnostics.
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.is_empty() {
        // Log non-empty stderr even on success (user might have print statements).
        for line in stderr.as_ref().lines() {
            debug!(target: "python_transform", "{}", line);
        }
    }

    match output.status.code() {
        Some(0) => {
            // Success — read the output IPC file.
            read_ipc(&output_path)
        }
        Some(1) => {
            // User code error — stderr contains the traceback or message.
            let message = parse_python_error(&stderr);
            Err(NodeErrorKind::Python(message))
        }
        Some(2) => {
            // Runner infrastructure error.
            let message = parse_runner_error(&stderr);
            Err(NodeErrorKind::Python(message))
        }
        Some(code) => {
            Err(NodeErrorKind::Python(format!(
                "Python process exited with unexpected code {code}: {stderr}"
            )))
        }
        None => {
            Err(NodeErrorKind::Python(
                "Python process was killed by a signal".to_string(),
            ))
        }
    }
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

    let mut writer = FileWriter::try_new(file, &schema).map_err(|e| {
        NodeErrorKind::Python(format!("failed to create IPC writer: {e}"))
    })?;

    for batch in batches {
        writer.write(batch).map_err(|e| {
            NodeErrorKind::Python(format!("failed to write IPC batch: {e}"))
        })?;
    }

    writer.finish().map_err(|e| {
        NodeErrorKind::Python(format!("failed to finalize IPC file: {e}"))
    })?;

    Ok(())
}

/// Read `RecordBatch`es from an Arrow IPC file.
fn read_ipc(path: &Path) -> Result<Vec<RecordBatch>, NodeErrorKind> {
    let file = std::fs::File::open(path).map_err(|e| {
        NodeErrorKind::Python(format!("failed to open output IPC file {}: {e}", path.display()))
    })?;

    let reader = FileReader::try_new(file, None).map_err(|e| {
        NodeErrorKind::Python(format!("failed to read output IPC: {e}"))
    })?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            NodeErrorKind::Python(format!("failed to read IPC batch: {e}"))
        })?;
        batches.push(batch);
    }

    Ok(batches)
}

/// Find a Python 3 interpreter.
///
/// Search order:
/// 1. `HORIZON_FLUX_PYTHON` env var (explicit override)
/// 2. `VIRTUAL_ENV/bin/python3` (active venv)
/// 3. `.venv/bin/python3` relative to the workspace root (uv-managed venv)
/// 4. `python3` / `python` on PATH
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

    // 4. System PATH fallback.
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
