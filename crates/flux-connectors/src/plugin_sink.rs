// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Adapter that exposes a discovered plugin sink as a [`PipelineSink`].
//!
//! At runtime, the executor calls [`PipelineSink::write`] with the
//! `SinkConfig` for a sink node. The adapter looks up the sink type in a
//! shared [`PluginRegistry`], spawns the plugin subprocess, and drives the
//! sink lifecycle described in `docs/plugins/protocol-v1.md`:
//! handshake → configure → stream record batches → commit/abort → shutdown.
//!
//! One `PluginSink` instance is shared across every plugin sink type. The
//! sink type is read from `SinkConfig::connector` on each call.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use flux_datafusion::provider::{
    MaterializationContext, MaterializationReceipt, PipelineSink, ProviderError, WriteOptions,
    WriteStats,
};
use flux_engine::materialization::MaterializationPolicy;
use flux_engine::node::SinkConfig;
use flux_plugin_host::{
    DiscoveredPlugin, MaterializationCapabilities, PROTOCOL_VERSION, PluginProcess, PluginRegistry,
    PluginSession, PluginStatus, SpawnOptions,
};

/// `PipelineSink` implementation that delegates writes to plugin subprocesses.
pub struct PluginSink {
    plugins: Arc<PluginRegistry>,
}

impl PluginSink {
    pub fn new(plugins: Arc<PluginRegistry>) -> Self {
        Self { plugins }
    }
}

#[async_trait]
impl PipelineSink for PluginSink {
    async fn write(
        &self,
        config: &SinkConfig,
        data: Vec<RecordBatch>,
        _options: &WriteOptions,
        ctx: &MaterializationContext,
    ) -> Result<MaterializationReceipt, ProviderError> {
        let start = Instant::now();
        let sink_type = config.connector.clone();
        let cfg = config.config.clone();
        // Forward the full MaterializationPolicy as JSON so plugins (notably
        // OpenBoard's DuckDB sink) can implement non-trivial write strategies
        // like `snapshot` without us re-encoding the policy into the legacy
        // `config` blob. None for sinks with no policy declared.
        let materialization_json = config.materialization.as_ref().map(|p| {
            serde_json::to_value(p).expect("MaterializationPolicy serialization is infallible")
        });
        let plugins = Arc::clone(&self.plugins);

        // No batches → nothing to write. Skip spawning the plugin entirely.
        let Some(first) = data.first() else {
            let stats = WriteStats {
                rows_written: 0,
                bytes_written: 0,
                duration: start.elapsed(),
            };
            return Ok(MaterializationReceipt::from_write_stats(&stats, ctx));
        };
        let schema = first.schema();

        // PluginSession is sync/blocking. Run the lifecycle on a blocking
        // thread so we don't stall the tokio runtime on I/O with the child.
        let result =
            tokio::task::spawn_blocking(move || -> Result<(WriteStats, u64, u64), String> {
                let plugin = plugins
                    .find_sink(&sink_type)
                    .ok_or_else(|| format!("no plugin provides sink type `{sink_type}`"))?;

                let process = PluginProcess::spawn(plugin, SpawnOptions::default())
                    .map_err(|e| format!("failed to spawn plugin `{}`: {e}", plugin.name))?;
                let mut session =
                    PluginSession::new(process, PROTOCOL_VERSION, env!("CARGO_PKG_VERSION"));

                session
                    .handshake()
                    .map_err(|e| format!("plugin `{}` handshake failed: {e}", plugin.name))?;
                session
                    .configure(&sink_type, cfg, schema.as_ref(), materialization_json)
                    .map_err(|e| format!("plugin `{}` configure failed: {e}", plugin.name))?;

                let mut accepted_rows: u64 = 0;
                for batch in &data {
                    match session.send_batch(batch) {
                        Ok(ack) => accepted_rows += ack.rows_accepted,
                        Err(e) => {
                            let _ = session.abort(format!("send_batch failed: {e}"));
                            return Err(format!("plugin `{}` rejected batch: {e}", plugin.name));
                        }
                    }
                }

                let commit = match session.commit() {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = session.abort(format!("commit failed: {e}"));
                        return Err(format!("plugin `{}` commit failed: {e}", plugin.name));
                    }
                };
                let _ = session.shutdown();

                Ok((
                    WriteStats {
                        rows_written: if commit.rows > 0 {
                            commit.rows
                        } else {
                            accepted_rows
                        },
                        bytes_written: commit.bytes,
                        duration: Duration::from_millis(commit.duration_ms),
                    },
                    commit.rows_updated,
                    commit.rows_deleted,
                ))
            })
            .await
            .map_err(|e| -> ProviderError { format!("plugin sink task join error: {e}").into() })?;

        let (mut stats, rows_updated, rows_deleted) =
            result.map_err(|e| -> ProviderError { e.into() })?;
        // Use wall-clock duration if the plugin reported zero.
        if stats.duration.is_zero() {
            stats.duration = start.elapsed();
        }
        let mut receipt = MaterializationReceipt::from_write_stats(&stats, ctx);
        // Snapshot-capable plugins (doc 28) populate the close/delete counts on
        // CommitAck; surface them on the receipt so the planner sees the full
        // SnapshotMergeStats roll-up. Plugins that don't set these report 0.
        receipt.rows_updated = rows_updated;
        receipt.rows_deleted = rows_deleted;
        Ok(receipt)
    }

    fn validate_config(&self, config: &SinkConfig) -> Result<(), ProviderError> {
        let plugin =
            self.plugins
                .find_sink(&config.connector)
                .ok_or_else(|| -> ProviderError {
                    format!("no plugin provides sink type `{}`", config.connector).into()
                })?;
        check_materialization_against_plugin(
            plugin,
            &config.connector,
            config.materialization.as_ref(),
        )
    }
}

/// Find the `[[sinks]]` declaration in `plugin`'s manifest matching `sink_type`
/// and check the user-supplied materialization policy against the plugin's
/// declared capabilities. Plugins with no `[sinks.capabilities.materialization]`
/// table are treated as `append`-only per doc 24 §3.1.
fn check_materialization_against_plugin(
    plugin: &DiscoveredPlugin,
    sink_type: &str,
    policy: Option<&MaterializationPolicy>,
) -> Result<(), ProviderError> {
    if let PluginStatus::Invalid { error } = &plugin.status {
        return Err(format!("plugin `{}` is not loadable: {error}", plugin.name).into());
    }
    let manifest = plugin.manifest.as_ref().ok_or_else(|| -> ProviderError {
        format!("plugin `{}` has no manifest loaded", plugin.name).into()
    })?;
    let decl = manifest
        .sinks
        .iter()
        .find(|s| s.ty == sink_type)
        .ok_or_else(|| -> ProviderError {
            format!(
                "plugin `{}` does not declare sink type `{sink_type}`",
                plugin.name
            )
            .into()
        })?;

    // Determine effective capabilities. Omitted table = append-only.
    let default_caps = MaterializationCapabilities {
        append: true,
        ..Default::default()
    };
    let caps = decl
        .capabilities
        .materialization
        .as_ref()
        .unwrap_or(&default_caps);

    let policy = match policy {
        Some(p) => p,
        // No policy declared = default `append`, which every plugin sink must
        // accept (the default `append_only` fallback above guarantees it).
        None => return Ok(()),
    };

    let strategy = policy.write_strategy.as_str();
    if !caps.supports_strategy(strategy) {
        return Err(format!(
            "plugin `{}` sink `{sink_type}` does not support write_strategy `{strategy}` \
             (declared in [sinks.capabilities.materialization])",
            plugin.name
        )
        .into());
    }

    // on_schema_change is only enforced when the plugin declares a non-empty
    // allow-list. An empty list means "plugin didn't say" → don't gate.
    if !caps.on_schema_change.is_empty() {
        let policy_name = policy.on_schema_change.as_str();
        if !caps.supports_on_schema_change(policy_name) {
            return Err(format!(
                "plugin `{}` sink `{sink_type}` does not support on_schema_change \
                 `{policy_name}` (allowed: {:?})",
                plugin.name, caps.on_schema_change
            )
            .into());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flux_engine::materialization::{
        ChangeDetection, HardDeletes, MaterializationPolicy, OnSchemaChange, SnapshotPolicy,
        WriteStrategy,
    };
    use flux_plugin_host::{PluginRegistry, discover_plugins_in};
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn write_plugin(plugins_root: &std::path::Path, name: &str, manifest: &str) {
        let dir = plugins_root.join(name);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("plugin.toml"), manifest).unwrap();
        // config_schema must exist for the manifest to point at something real;
        // discovery doesn't actually parse it, only the executor does.
        fs::write(dir.join("schema.json"), "{}").unwrap();
    }

    fn manifest_with_caps(name: &str, sink_type: &str, materialization_block: &str) -> String {
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

[sinks.capabilities]
{materialization_block}
"#
        )
    }

    fn registry_with(plugin_name: &str, sink_type: &str, mat_block: &str) -> Arc<PluginRegistry> {
        let dir = Box::leak(Box::new(tempdir().unwrap()));
        let plugins_root: PathBuf = dir.path().to_path_buf();
        write_plugin(
            &plugins_root,
            plugin_name,
            &manifest_with_caps(plugin_name, sink_type, mat_block),
        );
        Arc::new(discover_plugins_in(&[plugins_root]))
    }

    fn merge_policy() -> MaterializationPolicy {
        MaterializationPolicy {
            write_strategy: WriteStrategy::Merge,
            unique_keys: Some(vec!["id".into()]),
            ..Default::default()
        }
    }

    #[test]
    fn validate_config_rejects_unknown_sink_type() {
        let sink = PluginSink::new(Arc::new(PluginRegistry::default()));
        let cfg = SinkConfig {
            connector: "nonexistent_plugin_sink".into(),
            materialization: None,
            config: serde_json::json!({}),
        };
        let err = sink.validate_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("nonexistent_plugin_sink"));
    }

    #[test]
    fn validate_config_rejects_merge_when_plugin_has_no_materialization_table() {
        // No `[sinks.capabilities.materialization]` table → append-only.
        let plugins = registry_with("alpha", "alpha_sink", "");
        let sink = PluginSink::new(plugins);
        let cfg = SinkConfig {
            connector: "alpha_sink".into(),
            materialization: Some(merge_policy()),
            config: serde_json::json!({}),
        };
        let err = sink.validate_config(&cfg).unwrap_err().to_string();
        assert!(err.contains("write_strategy `merge`"), "{err}");
    }

    #[test]
    fn validate_config_accepts_merge_when_declared() {
        let plugins = registry_with(
            "alpha",
            "alpha_sink",
            "[sinks.capabilities.materialization]\nmerge = true\nappend = true\n",
        );
        let sink = PluginSink::new(plugins);
        let cfg = SinkConfig {
            connector: "alpha_sink".into(),
            materialization: Some(merge_policy()),
            config: serde_json::json!({}),
        };
        sink.validate_config(&cfg).unwrap();
    }

    fn snapshot_policy() -> MaterializationPolicy {
        MaterializationPolicy {
            write_strategy: WriteStrategy::Snapshot,
            unique_keys: Some(vec!["id".into()]),
            snapshot: Some(SnapshotPolicy {
                change_detection: ChangeDetection::Check,
                check_columns: Some(vec!["email".into(), "plan".into()]),
                updated_at_column: None,
                hard_deletes: HardDeletes::Ignore,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn validate_config_rejects_snapshot_when_plugin_does_not_declare_capability() {
        let plugins = registry_with(
            "alpha",
            "alpha_sink",
            "[sinks.capabilities.materialization]\nappend = true\nmerge = true\n",
        );
        let sink = PluginSink::new(plugins);
        let cfg = SinkConfig {
            connector: "alpha_sink".into(),
            materialization: Some(snapshot_policy()),
            config: serde_json::json!({}),
        };
        let err = sink.validate_config(&cfg).unwrap_err().to_string();
        assert!(err.contains("write_strategy `snapshot`"), "{err}");
    }

    #[test]
    fn validate_config_accepts_snapshot_when_plugin_declares_capability() {
        let plugins = registry_with(
            "alpha",
            "alpha_sink",
            "[sinks.capabilities.materialization]\nappend = true\nsnapshot = true\n",
        );
        let sink = PluginSink::new(plugins);
        let cfg = SinkConfig {
            connector: "alpha_sink".into(),
            materialization: Some(snapshot_policy()),
            config: serde_json::json!({}),
        };
        sink.validate_config(&cfg).unwrap();
    }

    #[test]
    fn validate_config_rejects_unsupported_on_schema_change() {
        let plugins = registry_with(
            "alpha",
            "alpha_sink",
            "[sinks.capabilities.materialization]\nappend = true\non_schema_change = [\"fail\"]\n",
        );
        let sink = PluginSink::new(plugins);
        let policy = MaterializationPolicy {
            write_strategy: WriteStrategy::Append,
            on_schema_change: OnSchemaChange::AppendNewColumns,
            ..Default::default()
        };
        let cfg = SinkConfig {
            connector: "alpha_sink".into(),
            materialization: Some(policy),
            config: serde_json::json!({}),
        };
        let err = sink.validate_config(&cfg).unwrap_err().to_string();
        assert!(err.contains("on_schema_change"), "{err}");
    }

    #[test]
    fn validate_config_no_policy_passes_even_without_materialization_table() {
        let plugins = registry_with("alpha", "alpha_sink", "");
        let sink = PluginSink::new(plugins);
        let cfg = SinkConfig {
            connector: "alpha_sink".into(),
            materialization: None,
            config: serde_json::json!({}),
        };
        sink.validate_config(&cfg).unwrap();
    }

    #[tokio::test]
    async fn write_with_no_batches_short_circuits() {
        let sink = PluginSink::new(Arc::new(PluginRegistry::default()));
        let cfg = SinkConfig {
            connector: "any".into(),
            materialization: None,
            config: serde_json::json!({}),
        };
        // Empty data means we never look up the plugin or spawn anything.
        let receipt = sink
            .write(
                &cfg,
                vec![],
                &WriteOptions::default(),
                &MaterializationContext::default(),
            )
            .await
            .unwrap();
        assert_eq!(receipt.rows_written, 0);
    }
}
