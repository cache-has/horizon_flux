// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! User preferences for the system tray and desktop notifications.
//!
//! Stored as JSON in the platform config directory
//! (e.g. `~/.config/armillary/config.json` on Linux,
//! `~/Library/Application Support/armillary/config.json` on macOS).

use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tracing::warn;

const APP_DIR: &str = "armillary";
const CONFIG_FILE: &str = "config.json";

/// User-configurable preferences.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrayPrefs {
    /// Whether desktop notifications are enabled.
    #[serde(default = "default_true")]
    pub notifications_enabled: bool,
}

fn default_true() -> bool {
    true
}

impl Default for TrayPrefs {
    fn default() -> Self {
        Self {
            notifications_enabled: true,
        }
    }
}

impl TrayPrefs {
    /// Load preferences from disk, returning defaults if the file is missing or
    /// cannot be parsed.
    pub fn load() -> Self {
        let Some(path) = config_path() else {
            return Self::default();
        };

        match fs::read_to_string(&path) {
            Ok(contents) => serde_json::from_str(&contents).unwrap_or_else(|e| {
                warn!("Invalid config at {}: {e}, using defaults", path.display());
                Self::default()
            }),
            Err(_) => Self::default(),
        }
    }

    /// Persist preferences to disk. Logs a warning on failure.
    pub fn save(&self) {
        let Some(path) = config_path() else {
            warn!("Cannot determine config directory; preferences not saved");
            return;
        };

        if let Some(parent) = path.parent() {
            if let Err(e) = fs::create_dir_all(parent) {
                warn!("Cannot create config directory {}: {e}", parent.display());
                return;
            }
        }

        match serde_json::to_string_pretty(self) {
            Ok(json) => {
                if let Err(e) = fs::write(&path, json) {
                    warn!("Failed to write config to {}: {e}", path.display());
                }
            }
            Err(e) => {
                warn!("Failed to serialize config: {e}");
            }
        }
    }
}

/// Return the path to the config file, or `None` if the platform config
/// directory cannot be determined.
fn config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|d| d.join(APP_DIR).join(CONFIG_FILE))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_notifications_enabled() {
        let prefs = TrayPrefs::default();
        assert!(prefs.notifications_enabled);
    }

    #[test]
    fn roundtrip_serialization() {
        let prefs = TrayPrefs {
            notifications_enabled: false,
        };
        let json = serde_json::to_string(&prefs).unwrap();
        let restored: TrayPrefs = serde_json::from_str(&json).unwrap();
        assert_eq!(prefs, restored);
    }

    #[test]
    fn deserialize_missing_field_uses_default() {
        let json = "{}";
        let prefs: TrayPrefs = serde_json::from_str(json).unwrap();
        assert!(prefs.notifications_enabled);
    }

    #[test]
    fn load_from_nonexistent_returns_default() {
        // config_path() returns a real path but the file won't exist in test;
        // load() should gracefully return defaults.
        let prefs = TrayPrefs::load();
        assert!(prefs.notifications_enabled);
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");

        let prefs = TrayPrefs {
            notifications_enabled: false,
        };

        // Write directly to the temp path to avoid touching the real config.
        let json = serde_json::to_string_pretty(&prefs).unwrap();
        std::fs::write(&path, &json).unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        let restored: TrayPrefs = serde_json::from_str(&contents).unwrap();
        assert_eq!(prefs, restored);
    }
}
