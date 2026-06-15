// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! File arrival sensor — detects new files matching a glob pattern.
//!
//! Supports local filesystem paths. Cloud storage (S3, GCS, Azure) support
//! will be added via a pluggable `FileLister` trait in a future iteration,
//! leveraging the existing `armillary-connectors` cloud store infrastructure.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tracing::warn;

/// Persisted state for a file arrival sensor, stored as JSON in
/// `TriggerState.sensor_state`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileArrivalState {
    /// Canonical paths of files that have already been seen and processed.
    /// Using `BTreeSet` for deterministic serialization order.
    pub seen_files: BTreeSet<String>,
}

/// Scan the local filesystem for files matching `pattern` and return paths
/// that are not in `seen`.
///
/// Returns an error only for invalid glob patterns; individual unreadable
/// entries are logged and skipped.
pub fn detect_new_local_files(
    pattern: &str,
    seen: &FileArrivalState,
) -> Result<Vec<String>, String> {
    let entries =
        glob::glob(pattern).map_err(|e| format!("invalid glob pattern '{pattern}': {e}"))?;

    let mut new_files = Vec::new();
    for entry in entries {
        match entry {
            Ok(path) => {
                let path_str = path.to_string_lossy().to_string();
                if !seen.seen_files.contains(&path_str) {
                    new_files.push(path_str);
                }
            }
            Err(e) => {
                warn!("glob entry error: {e}");
            }
        }
    }
    Ok(new_files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn detects_new_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.csv"), "data").unwrap();
        fs::write(dir.path().join("b.csv"), "data").unwrap();

        let pattern = format!("{}/*.csv", dir.path().display());
        let seen = FileArrivalState::default();
        let new = detect_new_local_files(&pattern, &seen).unwrap();
        assert_eq!(new.len(), 2);
    }

    #[test]
    fn suppresses_already_seen() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.csv"), "data").unwrap();
        fs::write(dir.path().join("b.csv"), "data").unwrap();

        let pattern = format!("{}/*.csv", dir.path().display());

        let mut seen = FileArrivalState::default();
        let a_path = dir.path().join("a.csv").to_string_lossy().to_string();
        seen.seen_files.insert(a_path);

        let new = detect_new_local_files(&pattern, &seen).unwrap();
        assert_eq!(new.len(), 1);
        assert!(new[0].contains("b.csv"));
    }

    #[test]
    fn no_match_returns_empty() {
        let dir = TempDir::new().unwrap();
        let pattern = format!("{}/*.parquet", dir.path().display());
        let new = detect_new_local_files(&pattern, &FileArrivalState::default()).unwrap();
        assert!(new.is_empty());
    }

    #[test]
    fn invalid_glob_returns_error() {
        let result = detect_new_local_files("[invalid", &FileArrivalState::default());
        assert!(result.is_err());
    }

    #[test]
    fn state_roundtrips_through_json() {
        let mut state = FileArrivalState::default();
        state.seen_files.insert("/data/a.csv".to_string());
        state.seen_files.insert("/data/b.csv".to_string());

        let json = serde_json::to_value(&state).unwrap();
        let restored: FileArrivalState = serde_json::from_value(json).unwrap();
        assert_eq!(restored.seen_files, state.seen_files);
    }
}
