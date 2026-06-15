// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Traits for source and sink connectors, and a registry to look them up by name.
//!
//! **Sources** implement the [`SourceConnector`] trait, which is a factory that
//! creates a DataFusion [`TableProvider`] from connector-specific JSON config.
//! This design gives sources automatic filter and projection pushdown through
//! DataFusion's query planning.
//!
//! **Sinks** implement the [`PipelineSink`] trait, which writes record batches
//! to an external system and returns [`WriteStats`].

use armillary_engine::materialization::{MaterializationPolicy, ReadMode, WriteStrategy};
use armillary_engine::node::{SinkConfig, SourceConfig};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Error type returned by connector implementations.
pub type ProviderError = Box<dyn std::error::Error + Send + Sync>;

// ---------------------------------------------------------------------------
// Source connector
// ---------------------------------------------------------------------------

/// Factory that creates a DataFusion `TableProvider` from connector config.
///
/// Source connectors implement DataFusion's `TableProvider` trait directly,
/// which enables automatic filter and projection pushdown. The
/// `SourceConnector` is a factory registered once per connector type (e.g.
/// `"csv"`, `"postgresql"`); the executor calls [`create_table_provider`] for
/// each source node, passing the node's connector-specific JSON config.
///
/// [`create_table_provider`]: SourceConnector::create_table_provider
pub trait SourceConnector: Send + Sync {
    /// Create a [`TableProvider`] for the given source configuration.
    fn create_table_provider(
        &self,
        config: &SourceConfig,
    ) -> Result<Arc<dyn TableProvider>, ProviderError>;

    /// Configure a DataFusion session context with any resources this source
    /// needs at scan time (e.g., cloud object stores).
    ///
    /// Called by the executor after creating the [`SessionContext`] but before
    /// scanning the table provider. The default implementation is a no-op.
    ///
    /// [`SessionContext`]: datafusion::prelude::SessionContext
    fn configure_session(
        &self,
        _config: &SourceConfig,
        _ctx: &datafusion::prelude::SessionContext,
    ) -> Result<(), ProviderError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Sink connector
// ---------------------------------------------------------------------------

/// Statistics from a completed sink write operation.
///
/// **Note:** `WriteStats` is the legacy return type from the
/// pre-doc-27 [`PipelineSink`] trait. The richer
/// [`MaterializationReceipt`] (defined below) will replace it once the
/// trait-signature task from `planning/27-incremental-materializations.md`
/// lands; both types coexist during the migration window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteStats {
    /// Number of rows successfully written.
    pub rows_written: u64,
    /// Total bytes written (best-effort estimate; may be 0 when not measurable).
    pub bytes_written: u64,
    /// Wall-clock duration of the write operation.
    pub duration: Duration,
}

// ---------------------------------------------------------------------------
// Materialization receipt (doc 27)
// ---------------------------------------------------------------------------

/// A serialized watermark value, paired with its declared type.
///
/// `value` is the canonical string form produced by the engine's coercion
/// rules (see "Watermark Type Coercion Rules" in
/// `planning/27-incremental-materializations.md`). The receipt carries it
/// verbatim so it can be persisted to run history without needing Arrow at
/// the consumer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatermarkValue {
    /// Canonical string form (RFC 3339 for timestamp, decimal for int64,
    /// verbatim for string).
    pub value: String,
    /// The declared watermark type (`"timestamp"`, `"int64"`, `"string"`).
    pub r#type: String,
}

/// Difference between the schema armillary saw on the previous successful run and
/// the schema of the current incoming stream. Field names only — Arrow types
/// are recorded as their `Display` form so this struct stays Arrow-free.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaDiff {
    /// Columns present in the current stream but not in the stored schema.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<SchemaField>,
    /// Columns present in the stored schema but missing from the current stream.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<SchemaField>,
    /// Columns whose Arrow data type changed between runs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub type_changed: Vec<SchemaTypeChange>,
}

impl SchemaDiff {
    /// Returns true when no columns were added, removed, or retyped.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.type_changed.is_empty()
    }
}

/// A column reference inside a [`SchemaDiff`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: String,
    /// Arrow data type, formatted via `Display` (e.g. `"Int64"`, `"Utf8"`).
    pub data_type: String,
}

/// A column whose Arrow data type changed between runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaTypeChange {
    pub name: String,
    pub from: String,
    pub to: String,
}

/// Structured outcome of a sink write under doc 27's materialization model.
///
/// Every successful sink write produces one of these. It is the canonical
/// answer to "what did this run do?" and is the input the executor persists
/// to run history, the WebSocket layer broadcasts to the canvas, and the
/// `incremental status` / `incremental plan` CLI commands read.
///
/// `rows_inserted/updated/deleted` are best-effort: sinks that can compute
/// exact counts (Postgres via `RETURNING`, DuckDB via row counts) populate
/// them; sinks that can't report `rows_written` only and leave the rest at
/// zero. For v1, zero means "either zero or unmeasured."
///
/// This type is defined now per the doc-27 design checklist; the
/// [`PipelineSink`] trait still returns [`WriteStats`] until the
/// trait-signature task from doc 27 lands.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationReceipt {
    pub write_strategy: WriteStrategy,
    pub read_mode: ReadMode,
    /// Rows the sink received (post any upstream watermark filter).
    pub rows_scanned: u64,
    /// Rows filtered out at the source by watermark pushdown.
    pub rows_filtered_by_watermark: u64,
    /// Total rows that hit the target.
    pub rows_written: u64,
    pub rows_inserted: u64,
    /// Nonzero for `merge` and `delete_insert`.
    pub rows_updated: u64,
    /// Nonzero for `delete_insert` and `insert_overwrite`.
    pub rows_deleted: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark_before: Option<WatermarkValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark_after: Option<WatermarkValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_diff: Option<SchemaDiff>,
    pub duration: Duration,
}

impl MaterializationReceipt {
    /// Build a minimal receipt from a [`WriteStats`] in the default
    /// "full read, append strategy" shape. Used by sinks that don't yet
    /// populate strategy-specific counts and want a sensible default.
    pub fn from_write_stats_default(stats: &WriteStats) -> Self {
        Self::from_write_stats(stats, &MaterializationContext::default())
    }

    /// Build a receipt from a [`WriteStats`] using the [`MaterializationContext`]
    /// the executor handed to the sink. Sinks that can't populate exact
    /// `inserted/updated/deleted` counts use this to surface their strategy
    /// and read mode while reporting `rows_written` as the only authoritative
    /// count.
    pub fn from_write_stats(stats: &WriteStats, ctx: &MaterializationContext) -> Self {
        let inserted = match ctx.write_strategy {
            // For append we know every written row is an insert.
            WriteStrategy::Append => stats.rows_written,
            // For other strategies, exact insert/update/delete counts are
            // best-effort and require sink-specific introspection (RETURNING
            // on Postgres, row-count APIs on DuckDB, etc.). Until those are
            // wired up, leave them at zero — receipt v1 documents that zero
            // means "either zero or unmeasured."
            _ => 0,
        };
        Self {
            write_strategy: ctx.write_strategy,
            read_mode: ctx.read_mode,
            rows_scanned: stats.rows_written,
            rows_filtered_by_watermark: 0,
            rows_written: stats.rows_written,
            rows_inserted: inserted,
            rows_updated: 0,
            rows_deleted: 0,
            watermark_before: None,
            watermark_after: None,
            schema_diff: None,
            duration: stats.duration,
        }
    }
}

// ---------------------------------------------------------------------------
// Materialization context (doc 27)
// ---------------------------------------------------------------------------

/// Per-sink-write context handed to [`PipelineSink::write`] by the executor.
///
/// Built from the sink node's [`MaterializationPolicy`] (or defaulted to
/// "full read + append strategy" when no policy is configured). Sinks read
/// this to:
/// - decide which write path to take (`write_strategy`),
/// - know whether the upstream read was watermark-filtered (`read_mode`),
/// - access `unique_keys`/`partition_column` without re-parsing the policy,
/// - populate the [`MaterializationReceipt`] they return on success.
///
/// Defaulting to full+append makes existing call sites — sinks for pipelines
/// that pre-date doc 27 and don't carry a `materialization` block — work
/// without any caller change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializationContext {
    pub read_mode: ReadMode,
    pub write_strategy: WriteStrategy,
    pub unique_keys: Vec<String>,
    pub partition_column: Option<String>,
    /// Watermark column name, when `read_mode == Incremental`. The actual
    /// stored watermark value is owned by the executor's incremental state
    /// path (still being wired up); sinks see only the column name in v1.
    pub watermark_column: Option<String>,
    /// Set by the executor when the configured `on_schema_change` policy
    /// produced a `SchemaAction::ProceedWithAlter` for this run. Sinks that
    /// support target-side schema evolution (e.g. PostgresSink under
    /// `append_new_columns`) read this flag and adapt the target before
    /// writing data. Sinks that don't support evolution may safely ignore it
    /// — the executor already logged a WARN at the seam in that case.
    pub apply_schema_changes: bool,
}

impl Default for MaterializationContext {
    fn default() -> Self {
        Self {
            read_mode: ReadMode::Full,
            write_strategy: WriteStrategy::Append,
            unique_keys: Vec::new(),
            partition_column: None,
            watermark_column: None,
            apply_schema_changes: false,
        }
    }
}

impl MaterializationContext {
    /// Build a context from an optional [`MaterializationPolicy`]. `None`
    /// yields the default "full read + append strategy" context.
    pub fn from_policy(policy: Option<&MaterializationPolicy>) -> Self {
        let Some(p) = policy else {
            return Self::default();
        };
        Self {
            read_mode: p.read_mode,
            write_strategy: p.write_strategy,
            unique_keys: p.unique_keys.clone().unwrap_or_default(),
            partition_column: p.partition_column.clone(),
            watermark_column: p.watermark.as_ref().map(|w| w.column.clone()),
            apply_schema_changes: false,
        }
    }
}

/// Options controlling how a sink writes data.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WriteOptions {
    /// Maximum rows per batch or insert statement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,

    /// Conflict handling for database sinks (e.g. ON CONFLICT behavior).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_conflict: Option<OnConflict>,

    /// Format-specific options (e.g. compression codec, CSV delimiter).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub format_options: HashMap<String, String>,
}

/// How to handle conflicts during database writes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnConflict {
    /// Fail the write on conflict.
    Fail,
    /// Skip conflicting rows.
    Skip,
    /// Update existing rows with new values.
    Update,
}

/// Writes pipeline data to an external system (file, database, stdout, etc.).
///
/// Sink connectors are registered once per connector type. The executor calls
/// [`write`] for each sink node, passing the node's config and upstream data.
///
/// [`write`]: PipelineSink::write
#[async_trait]
pub trait PipelineSink: Send + Sync {
    /// Write record batches to the sink, returning a structured
    /// [`MaterializationReceipt`] describing what happened.
    ///
    /// `ctx` carries the resolved materialization policy for this sink (read
    /// mode, write strategy, unique keys, etc.). The default
    /// [`MaterializationContext::default`] is "full read + append strategy",
    /// which is the right shape for sinks that pre-date doc 27 or have no
    /// materialization block configured.
    async fn write(
        &self,
        config: &SinkConfig,
        data: Vec<RecordBatch>,
        options: &WriteOptions,
        ctx: &MaterializationContext,
    ) -> Result<MaterializationReceipt, ProviderError>;

    /// Validate that a sink configuration is complete and well-formed.
    fn validate_config(&self, config: &SinkConfig) -> Result<(), ProviderError>;
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Maps connector type names (e.g. `"csv"`, `"postgresql"`) to source and
/// sink implementations.
#[derive(Default)]
pub struct ProviderRegistry {
    sources: HashMap<String, Arc<dyn SourceConnector>>,
    sinks: HashMap<String, Arc<dyn PipelineSink>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    // -- sources --

    pub fn register_source(
        &mut self,
        connector: impl Into<String>,
        source: Arc<dyn SourceConnector>,
    ) {
        self.sources.insert(connector.into(), source);
    }

    pub fn get_source(&self, connector: &str) -> Option<&Arc<dyn SourceConnector>> {
        self.sources.get(connector)
    }

    /// List registered source connector names.
    pub fn source_names(&self) -> Vec<&str> {
        self.sources.keys().map(|s| s.as_str()).collect()
    }

    // -- sinks --

    pub fn register_sink(&mut self, connector: impl Into<String>, sink: Arc<dyn PipelineSink>) {
        self.sinks.insert(connector.into(), sink);
    }

    pub fn get_sink(&self, connector: &str) -> Option<&Arc<dyn PipelineSink>> {
        self.sinks.get(connector)
    }

    /// List registered sink connector names.
    pub fn sink_names(&self) -> Vec<&str> {
        self.sinks.keys().map(|s| s.as_str()).collect()
    }

    /// List all registered connector names (sources + sinks, deduplicated).
    pub fn connector_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self
            .sources
            .keys()
            .chain(self.sinks.keys())
            .map(|s| s.as_str())
            .collect();
        names.sort_unstable();
        names.dedup();
        names
    }
}
