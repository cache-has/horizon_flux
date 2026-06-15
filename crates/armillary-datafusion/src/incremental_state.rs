// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Persistent state for incremental sink materializations (planning doc 27).
//!
//! Two records are stored per `(pipeline_id, node_id, environment)`:
//!
//! - [`IncrementalState`] — the latest watermark and run metadata for an
//!   incremental sink. There is exactly one row per key; subsequent runs
//!   upsert it.
//! - [`IncrementalSchemaRecord`] — the Arrow schema observed on each
//!   successful run, retained as history so the schema-change detector can
//!   diff against the most recent record.
//!
//! Both records are deliberately backend-agnostic: they use plain `String`
//! columns and unix-epoch milliseconds for timestamps so the SQLite and
//! PostgreSQL backends can share the same Rust types.

use serde::{Deserialize, Serialize};

/// Latest watermark + run metadata for one incremental sink node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementalState {
    /// Pipeline this state belongs to. In SQLite the pipelines table lives in
    /// a separate database file, so this is stored as a plain string with no
    /// foreign-key constraint. In PostgreSQL it is enforced via FK.
    pub pipeline_id: String,
    /// Node id within the pipeline (typically the sink node).
    pub node_id: String,
    /// Environment scope (e.g. `dev`, `prod`). Watermarks never cross
    /// environments.
    pub environment: String,
    /// Name of the column the watermark is read from.
    pub watermark_column: String,
    /// Canonical serialized form of the watermark value. Format depends on
    /// `watermark_type` per the "Watermark Type Coercion Rules" section of
    /// planning doc 27.
    pub watermark_value: String,
    /// `"timestamp"` | `"int64"` | `"string"`.
    pub watermark_type: String,
    /// Wall-clock time of the most recent successful run, unix-epoch ms.
    pub last_run_at_ms: i64,
    /// Pipeline-run id that produced this state.
    pub last_run_id: String,
    /// Number of rows the most recent run actually wrote (post-watermark
    /// filtering).
    pub rows_processed: u64,
    /// Optional xxhash64 fingerprint of the last successful Arrow schema for
    /// fast change detection. `None` for runs that pre-date the schema
    /// fingerprinting work.
    pub schema_fingerprint: Option<String>,
}

/// One historical Arrow-schema observation for an incremental node.
///
/// Used by the schema-change handler to compute diffs against the most recent
/// successful run and to surface a schema timeline in the UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementalSchemaRecord {
    pub pipeline_id: String,
    pub node_id: String,
    pub environment: String,
    /// Pipeline-run id that observed this schema.
    pub run_id: String,
    /// Arrow schema serialized to JSON. Currently produced via
    /// `serde_json::to_string` over an Arrow `Schema` value; the exact
    /// encoding is opaque to this layer.
    pub schema_json: String,
    /// xxhash64 hex of `schema_json` for cheap equality checks.
    pub fingerprint: String,
    /// When the schema was observed, unix-epoch ms.
    pub recorded_at_ms: i64,
}
