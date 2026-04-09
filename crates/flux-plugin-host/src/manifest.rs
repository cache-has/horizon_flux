// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Parser and validator for `plugin.toml` manifests.
//!
//! See `docs/plugins/manifest.md` for the normative reference. Validation
//! rules implemented here:
//!
//! - Top-level and `[[sinks]]` reject unknown fields (typo surfacing).
//! - `name` must be lowercase `[a-z0-9_-]+`.
//! - `version` and `flux_min_version` must be SemVer 2.0.
//! - `flux_plugin_protocol` must equal [`crate::PROTOCOL_VERSION`].
//! - At least one `[[sinks]]` entry is required.
//! - Sink `type` must match `[a-z0-9_]+` and be unique within the manifest.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use regex::Regex;
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::protocol::PROTOCOL_VERSION;

fn name_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-z0-9_-]+$").unwrap())
}

fn sink_type_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-z0-9_]+$").unwrap())
}

/// Parsed and validated `plugin.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub author: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub license: Option<String>,
    #[serde(default)]
    pub homepage: Option<String>,

    pub flux_plugin_protocol: u32,
    pub flux_min_version: String,

    pub executable: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,

    #[serde(default, rename = "sinks")]
    pub sinks: Vec<SinkDeclaration>,
}

/// One `[[sinks]]` entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkDeclaration {
    #[serde(rename = "type")]
    pub ty: String,
    pub display_name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub config_schema: String,
    #[serde(default)]
    pub capabilities: SinkCapabilities,
}

/// Optional capability flags. Unknown keys are accepted and ignored to allow
/// forward-compatible additions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SinkCapabilities {
    #[serde(default)]
    pub transactional: bool,
    #[serde(default)]
    pub upsert: bool,
    #[serde(default)]
    pub schema_validation: bool,
    /// Materialization strategies the plugin supports (doc 27 / doc 24 §3.1).
    /// Omitted = plugin only supports `write_strategy: append`.
    #[serde(default)]
    pub materialization: Option<MaterializationCapabilities>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, toml::Value>,
}

/// Per-strategy capability flags echoed by `[sinks.capabilities.materialization]`
/// in `plugin.toml`. See `planning/24-plugin-system.md` §3.1.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializationCapabilities {
    #[serde(default)]
    pub append: bool,
    #[serde(default)]
    pub merge: bool,
    #[serde(default)]
    pub delete_insert: bool,
    #[serde(default)]
    pub insert_overwrite: bool,
    #[serde(default)]
    pub truncate_insert: bool,
    /// SCD2 snapshot materialization (doc 28). When `true`, the plugin
    /// implements stage-diff-merge with `flux_valid_from`/`flux_valid_to`/
    /// `flux_is_current`/`flux_scd_id` metadata columns and accepts the
    /// `snapshot:` sub-block from the `MaterializationPolicy` forwarded via
    /// `ConfigureSink.materialization`.
    #[serde(default)]
    pub snapshot: bool,
    /// Subset of {`fail`, `ignore`, `append_new_columns`, `sync_all_columns`}.
    #[serde(default)]
    pub on_schema_change: Vec<String>,
}

impl MaterializationCapabilities {
    /// True if the plugin declares support for the named write strategy.
    /// Strategy names use the canonical snake_case form ("append", "merge", ...).
    pub fn supports_strategy(&self, strategy: &str) -> bool {
        match strategy {
            "append" => self.append,
            "merge" => self.merge,
            "delete_insert" => self.delete_insert,
            "insert_overwrite" => self.insert_overwrite,
            "truncate_insert" => self.truncate_insert,
            "snapshot" => self.snapshot,
            _ => false,
        }
    }

    /// True if the plugin declares support for the named on_schema_change policy.
    pub fn supports_on_schema_change(&self, policy: &str) -> bool {
        self.on_schema_change.iter().any(|p| p == policy)
    }
}

impl Manifest {
    /// Read `plugin.toml` from disk and validate it.
    pub fn from_path(path: &Path) -> Result<Self> {
        let bytes = std::fs::read_to_string(path).map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_str(&bytes, path)
    }

    /// Parse and validate from a TOML string. `source_path` is used only for
    /// error messages.
    pub fn from_str(input: &str, source_path: &Path) -> Result<Self> {
        let manifest: Manifest = toml::from_str(input).map_err(|e| Error::Manifest {
            path: source_path.to_path_buf(),
            message: format!("invalid TOML: {e}"),
        })?;
        manifest.validate(source_path)?;
        Ok(manifest)
    }

    fn validate(&self, source_path: &Path) -> Result<()> {
        let bad = |message: String| Error::Manifest {
            path: source_path.to_path_buf(),
            message,
        };

        if !name_re().is_match(&self.name) {
            return Err(bad(format!("name `{}` must match [a-z0-9_-]+", self.name)));
        }
        Version::parse(&self.version)
            .map_err(|e| bad(format!("version `{}` is not SemVer: {e}", self.version)))?;
        Version::parse(&self.flux_min_version).map_err(|e| {
            bad(format!(
                "flux_min_version `{}` is not SemVer: {e}",
                self.flux_min_version
            ))
        })?;

        if self.flux_plugin_protocol != PROTOCOL_VERSION {
            return Err(bad(format!(
                "flux_plugin_protocol = {} but this flux build supports {}",
                self.flux_plugin_protocol, PROTOCOL_VERSION
            )));
        }

        if self.executable.trim().is_empty() {
            return Err(bad("executable must not be empty".into()));
        }

        if self.sinks.is_empty() {
            return Err(bad("at least one [[sinks]] entry is required in v1".into()));
        }

        let mut seen: HashSet<&str> = HashSet::new();
        for sink in &self.sinks {
            if !sink_type_re().is_match(&sink.ty) {
                return Err(bad(format!(
                    "sink type `{}` must match [a-z0-9_]+",
                    sink.ty
                )));
            }
            if !seen.insert(&sink.ty) {
                return Err(bad(format!(
                    "duplicate sink type `{}` within manifest",
                    sink.ty
                )));
            }
            if sink.display_name.trim().is_empty() {
                return Err(bad(format!(
                    "sink `{}` display_name must not be empty",
                    sink.ty
                )));
            }
            if sink.config_schema.trim().is_empty() {
                return Err(bad(format!(
                    "sink `{}` config_schema must not be empty",
                    sink.ty
                )));
            }
        }

        Ok(())
    }

    /// Resolve the executable path relative to the plugin directory,
    /// appending `.exe` on Windows if missing.
    pub fn resolve_executable(&self, plugin_dir: &Path) -> PathBuf {
        let mut p = plugin_dir.join(&self.executable);
        if cfg!(windows) && p.extension().is_none() {
            p.set_extension("exe");
        }
        p
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn p() -> PathBuf {
        PathBuf::from("plugin.toml")
    }

    const GOOD: &str = r#"
name = "openboard"
version = "0.1.0"
flux_plugin_protocol = 1
flux_min_version = "0.5.0"
executable = "openboard-plugin"

[[sinks]]
type = "openboard_duckdb"
display_name = "OpenBoard (DuckDB)"
config_schema = "config_schema.json"

[sinks.capabilities]
transactional = true
"#;

    #[test]
    fn parses_a_valid_manifest() {
        let m = Manifest::from_str(GOOD, &p()).unwrap();
        assert_eq!(m.name, "openboard");
        assert_eq!(m.sinks.len(), 1);
        assert!(m.sinks[0].capabilities.transactional);
        assert!(!m.sinks[0].capabilities.upsert);
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        let bad = format!("unknown_field = \"x\"\n{GOOD}");
        let err = Manifest::from_str(&bad, &p()).unwrap_err();
        assert!(matches!(err, Error::Manifest { .. }));
    }

    #[test]
    fn rejects_bad_protocol_version() {
        let bad = GOOD.replace("flux_plugin_protocol = 1", "flux_plugin_protocol = 99");
        let err = Manifest::from_str(&bad, &p()).unwrap_err();
        assert!(format!("{err}").contains("flux_plugin_protocol"));
    }

    #[test]
    fn rejects_bad_name() {
        let bad = GOOD.replace("\"openboard\"", "\"OpenBoard!\"");
        let err = Manifest::from_str(&bad, &p()).unwrap_err();
        assert!(format!("{err}").contains("name"));
    }

    #[test]
    fn rejects_no_sinks() {
        let bad = r#"
name = "x"
version = "0.1.0"
flux_plugin_protocol = 1
flux_min_version = "0.1.0"
executable = "x"
"#;
        let err = Manifest::from_str(bad, &p()).unwrap_err();
        assert!(format!("{err}").contains("[[sinks]]"));
    }

    #[test]
    fn snapshot_capability_round_trips_and_supports_strategy() {
        let toml = format!(
            "{GOOD}\n[sinks.capabilities.materialization]\nappend = true\nsnapshot = true\n"
        );
        let m = Manifest::from_str(&toml, &p()).unwrap();
        let caps = m.sinks[0]
            .capabilities
            .materialization
            .as_ref()
            .expect("materialization block parsed");
        assert!(caps.snapshot);
        assert!(caps.supports_strategy("snapshot"));
        assert!(caps.supports_strategy("append"));
        assert!(!caps.supports_strategy("merge"));
    }

    #[test]
    fn snapshot_defaults_to_false_when_omitted() {
        let toml = format!("{GOOD}\n[sinks.capabilities.materialization]\nappend = true\n");
        let m = Manifest::from_str(&toml, &p()).unwrap();
        let caps = m.sinks[0].capabilities.materialization.as_ref().unwrap();
        assert!(!caps.snapshot);
        assert!(!caps.supports_strategy("snapshot"));
    }

    #[test]
    fn rejects_duplicate_sink_type() {
        let bad = format!(
            "{GOOD}\n[[sinks]]\ntype = \"openboard_duckdb\"\ndisplay_name = \"x\"\nconfig_schema = \"x.json\"\n"
        );
        let err = Manifest::from_str(&bad, &p()).unwrap_err();
        assert!(format!("{err}").contains("duplicate sink type"));
    }
}
