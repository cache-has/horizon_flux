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
    #[serde(flatten)]
    pub extra: BTreeMap<String, toml::Value>,
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
            return Err(bad(format!(
                "name `{}` must match [a-z0-9_-]+",
                self.name
            )));
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
    fn rejects_duplicate_sink_type() {
        let bad = format!(
            "{GOOD}\n[[sinks]]\ntype = \"openboard_duckdb\"\ndisplay_name = \"x\"\nconfig_schema = \"x.json\"\n"
        );
        let err = Manifest::from_str(&bad, &p()).unwrap_err();
        assert!(format!("{err}").contains("duplicate sink type"));
    }
}
