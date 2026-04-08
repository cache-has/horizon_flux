// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Plugin discovery — scans the platform-appropriate directories described in
//! `docs/plugins/discovery.md` and produces a [`PluginRegistry`].
//!
//! Scan order (later entries shadow earlier ones with the same `name`):
//! 1. Platform user-data dir (`directories::ProjectDirs`).
//! 2. Legacy `~/.horizon-flux/plugins` if it exists.
//! 3. `HORIZON_FLUX_PLUGIN_PATH` env var (platform-separator delimited).
//! 4. Workspace-local `./plugins/` against the current working directory.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::manifest::Manifest;

/// Environment variable consulted for additional scan roots.
pub const PLUGIN_PATH_ENV: &str = "HORIZON_FLUX_PLUGIN_PATH";

/// Final state of a plugin after discovery + manifest validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PluginStatus {
    /// Manifest parsed and validated successfully.
    Ok,
    /// Manifest failed to parse, fields were invalid, or directory structure
    /// was wrong. Plugin is visible to operators but not spawnable.
    Invalid { error: String },
}

/// One discovered plugin entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredPlugin {
    pub name: String,
    pub directory: PathBuf,
    pub manifest: Option<Manifest>,
    pub status: PluginStatus,
}

/// Output of a discovery pass.
#[derive(Debug, Clone, Default)]
pub struct PluginRegistry {
    /// Plugins keyed by name. Last writer wins (later scan roots shadow earlier).
    plugins: BTreeMap<String, DiscoveredPlugin>,
    /// Sink type → plugin name. Built from `Ok` plugins only.
    sink_index: BTreeMap<String, String>,
}

impl PluginRegistry {
    pub fn get(&self, name: &str) -> Option<&DiscoveredPlugin> {
        self.plugins.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &DiscoveredPlugin> {
        self.plugins.values()
    }

    pub fn len(&self) -> usize {
        self.plugins.len()
    }

    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    /// Look up the plugin that provides a given sink type.
    pub fn find_sink(&self, sink_type: &str) -> Option<&DiscoveredPlugin> {
        self.sink_index
            .get(sink_type)
            .and_then(|name| self.plugins.get(name))
    }

    /// Insert/replace a plugin entry. If a sink type would collide with an
    /// already-registered Ok plugin, the *new* plugin is downgraded to
    /// `Invalid` instead.
    fn install(&mut self, mut plugin: DiscoveredPlugin) {
        if let PluginStatus::Ok = &plugin.status {
            if let Some(manifest) = &plugin.manifest {
                for sink in &manifest.sinks {
                    if let Some(existing) = self.sink_index.get(&sink.ty) {
                        if existing != &plugin.name {
                            plugin.status = PluginStatus::Invalid {
                                error: format!(
                                    "sink type `{}` already provided by plugin `{}`",
                                    sink.ty, existing
                                ),
                            };
                            self.plugins.insert(plugin.name.clone(), plugin);
                            return;
                        }
                    }
                }
            }
        }

        // Remove any prior sink-index entries belonging to a shadowed plugin
        // of the same name.
        if let Some(prev) = self.plugins.remove(&plugin.name)
            && let Some(prev_manifest) = prev.manifest
        {
            for sink in &prev_manifest.sinks {
                if self.sink_index.get(&sink.ty) == Some(&prev.name) {
                    self.sink_index.remove(&sink.ty);
                }
            }
        }

        if let (PluginStatus::Ok, Some(manifest)) = (&plugin.status, &plugin.manifest) {
            for sink in &manifest.sinks {
                self.sink_index
                    .insert(sink.ty.clone(), plugin.name.clone());
            }
        }
        self.plugins.insert(plugin.name.clone(), plugin);
    }
}

/// Run discovery against the default scan roots and return a registry.
///
/// `cwd` is used to resolve the workspace-local `./plugins/` root. Pass the
/// flux process's working directory.
pub fn discover_plugins(cwd: &Path) -> PluginRegistry {
    discover_plugins_in(&scan_roots(cwd))
}

/// Like [`discover_plugins`] but scans only the explicitly provided roots
/// and skips all platform-derived locations (`ProjectDirs`, the legacy
/// home dir, `HORIZON_FLUX_PLUGIN_PATH`, and the workspace `./plugins/`).
///
/// This is the right entry point for tests and for callers that want full
/// control over which directories contribute plugins (e.g. an embedded
/// flux instance shipping its own plugin set).
pub fn discover_plugins_in(roots: &[PathBuf]) -> PluginRegistry {
    let mut registry = PluginRegistry::default();
    for root in roots {
        scan_root(root, &mut registry);
    }
    registry
}

/// Resolve the ordered list of scan roots. Public so `flux plugin path` can
/// print it.
pub fn scan_roots(cwd: &Path) -> Vec<PathBuf> {
    let mut roots: Vec<PathBuf> = Vec::new();

    if let Some(pd) = ProjectDirs::from("com", "horizon-analytic", "horizon-flux") {
        roots.push(pd.data_dir().join("plugins"));
    }

    if let Some(home) = dirs_home() {
        let legacy = home.join(".horizon-flux").join("plugins");
        if legacy.exists() {
            roots.push(legacy);
        }
    }

    if let Ok(val) = std::env::var(PLUGIN_PATH_ENV) {
        for entry in std::env::split_paths(&val) {
            if !entry.as_os_str().is_empty() {
                roots.push(entry);
            }
        }
    }

    roots.push(cwd.join("plugins"));
    roots
}

fn dirs_home() -> Option<PathBuf> {
    #[allow(deprecated)]
    std::env::home_dir()
}

fn scan_root(root: &Path, registry: &mut PluginRegistry) {
    let entries = match std::fs::read_dir(root) {
        Ok(it) => it,
        Err(e) => {
            debug!(root = %root.display(), error = %e, "skipping plugin scan root");
            return;
        }
    };

    let mut seen_in_root: HashSet<String> = HashSet::new();
    for entry in entries.flatten() {
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }
        let manifest_path = dir.join("plugin.toml");
        if !manifest_path.is_file() {
            continue;
        }
        let dir_name = match dir.file_name().and_then(|s| s.to_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };

        let plugin = load_plugin(&dir, &manifest_path, &dir_name);

        if !seen_in_root.insert(plugin.name.clone()) {
            warn!(
                root = %root.display(),
                name = %plugin.name,
                "duplicate plugin name in same scan root; ignoring second occurrence"
            );
            continue;
        }

        if registry.plugins.contains_key(&plugin.name) {
            info!(
                name = %plugin.name,
                root = %root.display(),
                "shadowing previously discovered plugin with higher-priority copy"
            );
        }
        registry.install(plugin);
    }
}

fn load_plugin(dir: &Path, manifest_path: &Path, dir_name: &str) -> DiscoveredPlugin {
    match Manifest::from_path(manifest_path) {
        Ok(manifest) => {
            if manifest.name != dir_name {
                return DiscoveredPlugin {
                    name: dir_name.to_string(),
                    directory: dir.to_path_buf(),
                    manifest: Some(manifest.clone()),
                    status: PluginStatus::Invalid {
                        error: format!(
                            "manifest name `{}` does not match directory `{}`",
                            manifest.name, dir_name
                        ),
                    },
                };
            }
            DiscoveredPlugin {
                name: manifest.name.clone(),
                directory: dir.to_path_buf(),
                manifest: Some(manifest),
                status: PluginStatus::Ok,
            }
        }
        Err(e) => DiscoveredPlugin {
            name: dir_name.to_string(),
            directory: dir.to_path_buf(),
            manifest: None,
            status: PluginStatus::Invalid {
                error: format!("{e}"),
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    fn write_plugin(root: &Path, name: &str, manifest: &str) {
        let dir = root.join(name);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("plugin.toml"), manifest).unwrap();
    }

    fn good_manifest(name: &str, sink_type: &str) -> String {
        format!(
            r#"
name = "{name}"
version = "0.1.0"
flux_plugin_protocol = 1
flux_min_version = "0.1.0"
executable = "{name}-plugin"

[[sinks]]
type = "{sink_type}"
display_name = "X"
config_schema = "schema.json"
"#
        )
    }

    #[test]
    fn discovers_a_valid_plugin() {
        let cwd = tempdir().unwrap();
        let plugins = cwd.path().join("plugins");
        fs::create_dir_all(&plugins).unwrap();
        write_plugin(&plugins, "alpha", &good_manifest("alpha", "alpha_sink"));

        let reg = discover_plugins(cwd.path());
        let p = reg.get("alpha").unwrap();
        assert!(matches!(p.status, PluginStatus::Ok));
        assert_eq!(reg.find_sink("alpha_sink").unwrap().name, "alpha");
    }

    #[test]
    fn directory_name_must_match_manifest_name() {
        let cwd = tempdir().unwrap();
        let plugins = cwd.path().join("plugins");
        fs::create_dir_all(&plugins).unwrap();
        write_plugin(&plugins, "wrong-dir", &good_manifest("alpha", "x"));

        let reg = discover_plugins(cwd.path());
        let p = reg.get("wrong-dir").unwrap();
        match &p.status {
            PluginStatus::Invalid { error } => assert!(error.contains("does not match")),
            _ => panic!("expected invalid"),
        }
    }

    #[test]
    fn invalid_manifest_is_visible_but_not_in_sink_index() {
        let cwd = tempdir().unwrap();
        let plugins = cwd.path().join("plugins");
        fs::create_dir_all(&plugins).unwrap();
        write_plugin(&plugins, "broken", "this is not toml = =");

        let reg = discover_plugins(cwd.path());
        assert!(matches!(
            reg.get("broken").unwrap().status,
            PluginStatus::Invalid { .. }
        ));
        assert!(reg.find_sink("anything").is_none());
    }

    #[test]
    fn sink_type_collision_marks_second_invalid() {
        // Drive `scan_root` directly so this test is fully isolated from
        // the developer machine's real plugin install dir (`ProjectDirs`,
        // `HORIZON_FLUX_PLUGIN_PATH`, etc.). `discover_plugins` is exercised
        // by the other tests in this module.
        let plugins = tempdir().unwrap();
        write_plugin(plugins.path(), "alpha", &good_manifest("alpha", "shared"));
        write_plugin(plugins.path(), "beta", &good_manifest("beta", "shared"));

        let mut reg = PluginRegistry::default();
        scan_root(plugins.path(), &mut reg);

        // One must be Ok, one Invalid. Order on disk is non-deterministic.
        let mut ok = 0;
        let mut bad = 0;
        for p in reg.iter() {
            match &p.status {
                PluginStatus::Ok => ok += 1,
                PluginStatus::Invalid { error } => {
                    assert!(error.contains("already provided"));
                    bad += 1;
                }
            }
        }
        assert_eq!((ok, bad), (1, 1));
    }
}
