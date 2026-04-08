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
use flux_datafusion::provider::{PipelineSink, ProviderError, WriteOptions, WriteStats};
use flux_engine::node::SinkConfig;
use flux_plugin_host::{
    PROTOCOL_VERSION, PluginProcess, PluginRegistry, PluginSession, SpawnOptions,
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
    ) -> Result<WriteStats, ProviderError> {
        let start = Instant::now();
        let sink_type = config.connector.clone();
        let cfg = config.config.clone();
        let plugins = Arc::clone(&self.plugins);

        // No batches → nothing to write. Skip spawning the plugin entirely.
        let Some(first) = data.first() else {
            return Ok(WriteStats {
                rows_written: 0,
                bytes_written: 0,
                duration: start.elapsed(),
            });
        };
        let schema = first.schema();

        // PluginSession is sync/blocking. Run the lifecycle on a blocking
        // thread so we don't stall the tokio runtime on I/O with the child.
        let result = tokio::task::spawn_blocking(move || -> Result<WriteStats, String> {
            let plugin = plugins
                .find_sink(&sink_type)
                .ok_or_else(|| format!("no plugin provides sink type `{sink_type}`"))?;

            let process = PluginProcess::spawn(plugin, SpawnOptions::default())
                .map_err(|e| format!("failed to spawn plugin `{}`: {e}", plugin.name))?;
            let mut session = PluginSession::new(
                process,
                PROTOCOL_VERSION,
                env!("CARGO_PKG_VERSION"),
            );

            session
                .handshake()
                .map_err(|e| format!("plugin `{}` handshake failed: {e}", plugin.name))?;
            session
                .configure(&sink_type, cfg, schema.as_ref())
                .map_err(|e| format!("plugin `{}` configure failed: {e}", plugin.name))?;

            let mut accepted_rows: u64 = 0;
            for batch in &data {
                match session.send_batch(batch) {
                    Ok(ack) => accepted_rows += ack.rows_accepted,
                    Err(e) => {
                        let _ = session.abort(format!("send_batch failed: {e}"));
                        return Err(format!(
                            "plugin `{}` rejected batch: {e}",
                            plugin.name
                        ));
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

            Ok(WriteStats {
                rows_written: if commit.rows > 0 { commit.rows } else { accepted_rows },
                bytes_written: commit.bytes,
                duration: Duration::from_millis(commit.duration_ms),
            })
        })
        .await
        .map_err(|e| -> ProviderError { format!("plugin sink task join error: {e}").into() })?;

        let mut stats = result.map_err(|e| -> ProviderError { e.into() })?;
        // Use wall-clock duration if the plugin reported zero.
        if stats.duration.is_zero() {
            stats.duration = start.elapsed();
        }
        Ok(stats)
    }

    fn validate_config(&self, config: &SinkConfig) -> Result<(), ProviderError> {
        if self.plugins.find_sink(&config.connector).is_none() {
            return Err(format!(
                "no plugin provides sink type `{}`",
                config.connector
            )
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flux_plugin_host::PluginRegistry;

    #[test]
    fn validate_config_rejects_unknown_sink_type() {
        let sink = PluginSink::new(Arc::new(PluginRegistry::default()));
        let cfg = SinkConfig {
            connector: "nonexistent_plugin_sink".into(),
            config: serde_json::json!({}),
        };
        let err = sink.validate_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("nonexistent_plugin_sink"));
    }

    #[tokio::test]
    async fn write_with_no_batches_short_circuits() {
        let sink = PluginSink::new(Arc::new(PluginRegistry::default()));
        let cfg = SinkConfig {
            connector: "any".into(),
            config: serde_json::json!({}),
        };
        // Empty data means we never look up the plugin or spawn anything.
        let stats = sink
            .write(&cfg, vec![], &WriteOptions::default())
            .await
            .unwrap();
        assert_eq!(stats.rows_written, 0);
    }
}
