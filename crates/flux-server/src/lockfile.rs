// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::error::ServerError;

/// Contents of the instance lockfile.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstanceInfo {
    pub pid: u32,
    pub port: u16,
}

/// Return the default lockfile path: `~/.horizon-flux/instance.lock`.
pub fn default_path() -> Result<PathBuf, ServerError> {
    dirs::home_dir()
        .map(|h| h.join(".horizon-flux").join("instance.lock"))
        .ok_or(ServerError::NoHomeDir)
}

/// Read an existing lockfile. Returns `None` if the file doesn't exist.
pub fn read(path: &Path) -> Result<Option<InstanceInfo>, ServerError> {
    match fs::read_to_string(path) {
        Ok(contents) => {
            let info: InstanceInfo = serde_json::from_str(&contents)?;
            Ok(Some(info))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Write a lockfile with the given instance info.
/// Creates parent directories if they don't exist.
pub fn write(path: &Path, info: &InstanceInfo) -> Result<(), ServerError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string(info)?;
    fs::write(path, json)?;
    Ok(())
}

/// Remove the lockfile. Silently ignores "not found" errors.
pub fn remove(path: &Path) {
    if let Err(e) = fs::remove_file(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            warn!("Failed to remove lockfile {}: {e}", path.display());
        }
    }
}

/// Check if a process with the given PID is alive.
///
/// On Unix, sends signal 0 (existence check, no actual signal delivered).
/// On Windows, opens the process and polls its handle for liveness.
/// On other platforms, conservatively returns false (treat as dead).
pub fn is_pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // SAFETY: signal 0 does not deliver a signal; it only checks
        // whether the process exists and we have permission to signal it.
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{CloseHandle, WAIT_TIMEOUT};
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject,
        };
        // SAFETY: we open a handle solely to poll the process's liveness and
        // then close it. `OpenProcess` returns NULL when the pid has no live
        // process (or we lack access), which we treat as "not alive".
        unsafe {
            let handle = OpenProcess(PROCESS_SYNCHRONIZE, 0, pid);
            if handle.is_null() {
                return false;
            }
            // A running process leaves its handle un-signaled (WAIT_TIMEOUT);
            // an exited process signals it (WAIT_OBJECT_0).
            let alive = WaitForSingleObject(handle, 0) == WAIT_TIMEOUT;
            CloseHandle(handle);
            alive
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        false
    }
}

/// Check for a running instance. Returns `Some(info)` if a live
/// instance is detected, `None` otherwise. Cleans up stale lockfiles.
pub fn check_existing(path: &Path) -> Result<Option<InstanceInfo>, ServerError> {
    match read(path)? {
        Some(info) if is_pid_alive(info.pid) => {
            debug!(
                "Found running instance: PID {} on port {}",
                info.pid, info.port
            );
            Ok(Some(info))
        }
        Some(info) => {
            debug!("Stale lockfile: PID {} is dead, cleaning up", info.pid);
            remove(path);
            Ok(None)
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_and_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance.lock");
        let info = InstanceInfo {
            pid: 12345,
            port: 8080,
        };
        write(&path, &info).unwrap();
        let read_back = read(&path).unwrap();
        assert_eq!(read_back, Some(info));
    }

    #[test]
    fn read_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.lock");
        assert_eq!(read(&path).unwrap(), None);
    }

    #[test]
    fn remove_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance.lock");
        let info = InstanceInfo { pid: 1, port: 8080 };
        write(&path, &info).unwrap();
        assert!(path.exists());
        remove(&path);
        assert!(!path.exists());
    }

    #[test]
    fn remove_missing_is_silent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.lock");
        remove(&path); // Should not panic.
    }

    #[test]
    fn current_pid_is_alive() {
        assert!(is_pid_alive(std::process::id()));
    }

    #[test]
    fn bogus_pid_is_dead() {
        // PID 4_000_000 is almost certainly not running.
        assert!(!is_pid_alive(4_000_000));
    }

    #[test]
    fn check_existing_cleans_stale() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance.lock");
        let info = InstanceInfo {
            pid: 4_000_000,
            port: 9090,
        };
        write(&path, &info).unwrap();
        let result = check_existing(&path).unwrap();
        assert_eq!(result, None);
        assert!(!path.exists(), "stale lockfile should be removed");
    }

    #[test]
    fn check_existing_finds_live_instance() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance.lock");
        let info = InstanceInfo {
            pid: std::process::id(),
            port: 8080,
        };
        write(&path, &info).unwrap();
        let result = check_existing(&path).unwrap();
        assert_eq!(result, Some(info));
    }
}
