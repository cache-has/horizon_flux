// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Managed Python environment bootstrapping.
//!
//! On first use, creates a Python virtual environment at `~/.armillary/python/`
//! using `uv`, installs required packages (polars, numpy, scipy, etc.), and marks
//! the environment ready with a marker file. Subsequent calls are a fast no-op
//! (two file-existence checks).
//!
//! The environment location can be overridden via `ARMILLARY_PYTHON_ENV` for
//! testing or custom deployments.

use crate::error::NodeErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;
use tracing::{debug, info};

/// Default Python version to install via `uv`.
const PYTHON_VERSION: &str = "3.12";

/// Required Python packages for the transform runtime.
const REQUIRED_PACKAGES: &[&str] = &["polars>=1.39.3", "numpy", "scipy", "requests", "httpx"];

/// Marker file name written after successful environment setup.
const MARKER_FILE: &str = ".armillary-ready";

/// Returns the managed Python environment directory.
///
/// Priority:
/// 1. `ARMILLARY_PYTHON_ENV` env var (override for testing/custom deployments)
/// 2. `~/.armillary/python/`
pub fn managed_env_dir() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("ARMILLARY_PYTHON_ENV") {
        return Some(PathBuf::from(p));
    }
    home_dir().map(|h| h.join(".armillary").join("python"))
}

/// Returns the Python interpreter path within the managed environment.
pub fn managed_python_path() -> Option<PathBuf> {
    managed_env_dir().map(|d| python_bin_in(&d))
}

/// Ensures the managed Python environment exists and has required packages.
///
/// This is idempotent — if the environment already exists and the marker file
/// is present, this returns immediately. If `uv` is not available, returns an
/// error but callers may fall back to system Python.
pub fn ensure_python_env() -> Result<PathBuf, NodeErrorKind> {
    let env_dir = managed_env_dir()
        .ok_or_else(|| NodeErrorKind::Python("could not determine home directory".to_string()))?;

    let python_path = python_bin_in(&env_dir);
    let marker = env_dir.join(MARKER_FILE);

    // Fast path: environment already exists and is marked ready.
    if marker.exists() && python_path.exists() {
        debug!(
            path = %env_dir.display(),
            "managed Python environment already ready"
        );
        return Ok(python_path);
    }

    // Find uv.
    let uv = find_uv()?;

    info!(path = %env_dir.display(), "creating managed Python environment");

    // Create parent directories if needed.
    if let Some(parent) = env_dir.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            NodeErrorKind::Python(format!(
                "failed to create directory {}: {e}",
                parent.display()
            ))
        })?;
    }

    // Create the venv with the specified Python version.
    let output = Command::new(&uv)
        .args(["venv", "--python", PYTHON_VERSION])
        .arg(&env_dir)
        .output()
        .map_err(|e| NodeErrorKind::Python(format!("failed to run `uv venv`: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NodeErrorKind::Python(format!(
            "`uv venv` failed (exit code {:?}): {stderr}\n\
             Ensure `uv` is installed and Python {PYTHON_VERSION} is available.",
            output.status.code()
        )));
    }

    // Install required packages.
    info!(packages = ?REQUIRED_PACKAGES, "installing Python packages");

    let output = Command::new(&uv)
        .args(["pip", "install", "--python"])
        .arg(&python_path)
        .args(REQUIRED_PACKAGES)
        .output()
        .map_err(|e| NodeErrorKind::Python(format!("failed to run `uv pip install`: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NodeErrorKind::Python(format!(
            "`uv pip install` failed (exit code {:?}): {stderr}\n\
             Check your network connection and try again.",
            output.status.code()
        )));
    }

    // Write marker file so subsequent calls are a fast no-op.
    std::fs::write(&marker, PYTHON_VERSION)
        .map_err(|e| NodeErrorKind::Python(format!("failed to write marker file: {e}")))?;

    info!(path = %env_dir.display(), "managed Python environment ready");
    Ok(python_path)
}

/// Find the `uv` binary.
///
/// Checks `ARMILLARY_UV` env var first, then falls back to `uv` on PATH.
pub fn find_uv() -> Result<String, NodeErrorKind> {
    if let Ok(uv) = std::env::var("ARMILLARY_UV") {
        return Ok(uv);
    }

    match Command::new("uv").arg("--version").output() {
        Ok(o) if o.status.success() => Ok("uv".to_string()),
        _ => Err(NodeErrorKind::Python(
            "`uv` is not installed. Install it from https://docs.astral.sh/uv/ \
             or set ARMILLARY_UV to the path of the uv binary."
                .to_string(),
        )),
    }
}

/// Returns the platform-appropriate Python binary path within a venv directory.
fn python_bin_in(env_dir: &Path) -> PathBuf {
    if cfg!(windows) {
        env_dir.join("Scripts").join("python.exe")
    } else {
        env_dir.join("bin").join("python3")
    }
}

/// Cross-platform home directory lookup without external crate dependencies.
fn home_dir() -> Option<PathBuf> {
    #[cfg(unix)]
    {
        std::env::var("HOME").ok().map(PathBuf::from)
    }
    #[cfg(windows)]
    {
        std::env::var("USERPROFILE").ok().map(PathBuf::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_env_dir_uses_override() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_string_lossy().into_owned();

        // SAFETY: Test is single-threaded for this env var; no other test uses
        // ARMILLARY_PYTHON_ENV concurrently.
        unsafe {
            std::env::set_var("ARMILLARY_PYTHON_ENV", &path);
        }
        let dir = managed_env_dir().unwrap();
        unsafe {
            std::env::remove_var("ARMILLARY_PYTHON_ENV");
        }

        assert_eq!(dir, PathBuf::from(&path));
    }

    #[test]
    fn python_bin_path_is_platform_correct() {
        let dir = PathBuf::from("/fake/env");
        let bin = python_bin_in(&dir);

        if cfg!(windows) {
            assert_eq!(bin, PathBuf::from("/fake/env/Scripts/python.exe"));
        } else {
            assert_eq!(bin, PathBuf::from("/fake/env/bin/python3"));
        }
    }

    #[test]
    fn ensure_env_fast_path_with_marker() {
        let tmp = tempfile::tempdir().unwrap();
        let env_dir = tmp.path().to_path_buf();

        // Create the marker file and a fake python3 binary.
        std::fs::write(env_dir.join(MARKER_FILE), PYTHON_VERSION).unwrap();
        let bin_dir = if cfg!(windows) {
            env_dir.join("Scripts")
        } else {
            env_dir.join("bin")
        };
        std::fs::create_dir_all(&bin_dir).unwrap();
        let python_name = if cfg!(windows) {
            "python.exe"
        } else {
            "python3"
        };
        std::fs::write(bin_dir.join(python_name), "fake").unwrap();

        // SAFETY: Test is single-threaded for this env var.
        unsafe {
            std::env::set_var("ARMILLARY_PYTHON_ENV", env_dir.to_string_lossy().as_ref());
        }
        let result = ensure_python_env();
        unsafe {
            std::env::remove_var("ARMILLARY_PYTHON_ENV");
        }

        // Should succeed via the fast path without needing uv.
        assert!(result.is_ok());
        assert!(result.unwrap().ends_with(python_name));
    }
}
