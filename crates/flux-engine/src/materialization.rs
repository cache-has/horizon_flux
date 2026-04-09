// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Sink materialization policy types.
//!
//! Implements the orthogonal `read_mode` × `write_strategy` model from
//! `planning/27-incremental-materializations.md` and the normative JSON shape
//! in `planning/14-pipeline-format.md` ("Sink Materialization Block").
//!
//! Design summary: a sink's materialization is described by **two independent
//! axes** — how much upstream data to read (`read_mode`) and how rows land at
//! the destination (`write_strategy`). Every combination is valid (subject to
//! a small set of structural exclusions enforced by [`validate_policy`]).
//!
//! `WriteStrategy::Snapshot` (doc 28) carries an additional `snapshot:`
//! sub-block of type [`SnapshotPolicy`] describing how row changes are
//! detected and how hard-deletes are handled. Validation rules for snapshots
//! live alongside the rest of [`validate_policy`].

use serde::{Deserialize, Serialize};

/// How much upstream data the sink reads on each run.
///
/// Orthogonal to [`WriteStrategy`] — every combination is valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadMode {
    /// Read everything upstream every run. Default.
    #[default]
    Full,
    /// Read only rows new or changed since the last run. Requires `watermark`.
    Incremental,
}

/// How rows land at the destination.
///
/// Orthogonal to [`ReadMode`] — every combination is valid (subject to the
/// per-strategy field requirements documented on each variant).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteStrategy {
    /// Insert new rows, no deduplication. Default.
    #[default]
    Append,
    /// Upsert on `unique_keys`. Requires `unique_keys`.
    Merge,
    /// Delete existing rows matching the incoming batch's unique keys, then
    /// insert. Requires `unique_keys`.
    DeleteInsert,
    /// Replace an entire partition with the incoming data. Requires
    /// `partition_column`.
    InsertOverwrite,
    /// Truncate the target, then insert. Subsumes the legacy
    /// `WriteMode::TruncateInsert` from pre-doc-27 PostgresSinkConfig.
    TruncateInsert,
    /// Maintain SCD2 history on the target via stage-diff-merge. Requires
    /// `unique_keys` and a `snapshot:` sub-block. See doc 28.
    Snapshot,
}

impl WriteStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            WriteStrategy::Append => "append",
            WriteStrategy::Merge => "merge",
            WriteStrategy::DeleteInsert => "delete_insert",
            WriteStrategy::InsertOverwrite => "insert_overwrite",
            WriteStrategy::TruncateInsert => "truncate_insert",
            WriteStrategy::Snapshot => "snapshot",
        }
    }
}

/// How "did this row change?" is determined for a snapshot. See doc 28.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeDetection {
    /// Compare the listed [`SnapshotPolicy::check_columns`]. Safest default.
    #[default]
    Check,
    /// Trust a source-provided `updated_at_column`. Cheaper but requires the
    /// source to update that column reliably.
    Timestamp,
}

impl ChangeDetection {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeDetection::Check => "check",
            ChangeDetection::Timestamp => "timestamp",
        }
    }
}

/// What to do with rows present in the target but missing from the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardDeletes {
    /// Leave the current version untouched. Default — safest.
    #[default]
    Ignore,
    /// Close the current version with `flux_valid_to = now()`.
    Invalidate,
    /// Physically remove all versions of the row. Violates SCD2 semantics
    /// but sometimes wanted.
    Delete,
}

impl HardDeletes {
    pub fn as_str(&self) -> &'static str {
        match self {
            HardDeletes::Ignore => "ignore",
            HardDeletes::Invalidate => "invalidate",
            HardDeletes::Delete => "delete",
        }
    }
}

/// Snapshot-specific policy block, nested under
/// [`MaterializationPolicy::snapshot`] when `write_strategy: snapshot`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotPolicy {
    #[serde(default)]
    pub change_detection: ChangeDetection,

    /// Required for `change_detection: check`. Columns that, when changed,
    /// constitute a new version. The literal string `"*"` selects all columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_columns: Option<Vec<String>>,

    /// Required for `change_detection: timestamp`. Source column that flags
    /// candidate rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_column: Option<String>,

    #[serde(default)]
    pub hard_deletes: HardDeletes,
}

/// Type of the watermark column. Drives serialization and comparison rules
/// (see "Watermark Type Coercion Rules" in doc 27).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkType {
    Timestamp,
    Int64,
    String,
}

impl WatermarkType {
    fn as_str(&self) -> &'static str {
        match self {
            WatermarkType::Timestamp => "timestamp",
            WatermarkType::Int64 => "int64",
            WatermarkType::String => "string",
        }
    }
}

/// The watermark column flux uses to determine "new since last run."
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    pub column: String,
    #[serde(rename = "type")]
    pub watermark_type: WatermarkType,
}

/// What happens when the incoming Arrow schema differs from the stored schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnSchemaChange {
    Fail,
    Ignore,
    #[default]
    AppendNewColumns,
    SyncAllColumns,
}

impl OnSchemaChange {
    pub fn as_str(&self) -> &'static str {
        match self {
            OnSchemaChange::Fail => "fail",
            OnSchemaChange::Ignore => "ignore",
            OnSchemaChange::AppendNewColumns => "append_new_columns",
            OnSchemaChange::SyncAllColumns => "sync_all_columns",
        }
    }
}

/// What to do on the very first run, when no prior incremental state exists.
///
/// Note: an `empty` variant was considered and rejected — see doc 27 "Open
/// Questions" resolution. The "skip the bootstrap" use case is better served
/// by adding a `WHERE` clause to the source query, which puts the cut point
/// in the version-controlled pipeline definition instead of in opaque
/// metadata-store state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FirstRun {
    #[default]
    Full,
    Fail,
}

fn default_lookback() -> String {
    "PT0S".to_string()
}

/// The materialization block on a sink node. See `14-pipeline-format.md`.
///
/// `read_mode` and `write_strategy` are orthogonal axes — every combination
/// is valid subject to the field-presence rules enforced by [`validate_policy`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationPolicy {
    #[serde(default)]
    pub read_mode: ReadMode,

    #[serde(default)]
    pub write_strategy: WriteStrategy,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark: Option<Watermark>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_keys: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_column: Option<String>,

    #[serde(default)]
    pub on_schema_change: OnSchemaChange,

    #[serde(default)]
    pub first_run: FirstRun,

    /// ISO 8601 duration subtracted from the stored watermark before filtering.
    /// Defaults to `"PT0S"`. Only meaningful under `read_mode: incremental`
    /// with a `timestamp` watermark; rejected at validation time otherwise.
    #[serde(default = "default_lookback")]
    pub lookback: String,

    /// Required iff `write_strategy: snapshot`; forbidden otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotPolicy>,
}

impl Default for MaterializationPolicy {
    fn default() -> Self {
        Self {
            read_mode: ReadMode::default(),
            write_strategy: WriteStrategy::default(),
            watermark: None,
            unique_keys: None,
            partition_column: None,
            on_schema_change: OnSchemaChange::default(),
            first_run: FirstRun::default(),
            lookback: default_lookback(),
            snapshot: None,
        }
    }
}

/// Validation problems detected on a materialization policy.
///
/// These mirror the field rules in `14-pipeline-format.md` ("Sink
/// Materialization Block"). Surfaced through
/// [`crate::error::ValidationError::Materialization`] during pipeline import.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MaterializationError {
    #[error("sink `{node_id}`: `watermark` is required when `read_mode` is `incremental`")]
    WatermarkMissing { node_id: String },

    #[error("sink `{node_id}`: `watermark` must not be set when `read_mode` is `full`")]
    WatermarkForbidden { node_id: String },

    #[error("sink `{node_id}`: `watermark.column` must not be empty")]
    WatermarkColumnEmpty { node_id: String },

    #[error(
        "sink `{node_id}`: `unique_keys` is required for write_strategy `{strategy}` and must be non-empty"
    )]
    UniqueKeysRequired { node_id: String, strategy: String },

    #[error("sink `{node_id}`: `unique_keys` must not be set for write_strategy `{strategy}`")]
    UniqueKeysForbidden { node_id: String, strategy: String },

    #[error(
        "sink `{node_id}`: `partition_column` is required for write_strategy `insert_overwrite`"
    )]
    PartitionColumnRequired { node_id: String },

    #[error("sink `{node_id}`: `partition_column` must not be set for write_strategy `{strategy}`")]
    PartitionColumnForbidden { node_id: String, strategy: String },

    #[error(
        "sink `{node_id}`: `lookback` is only meaningful under `read_mode: incremental` with a timestamp watermark (got read_mode={read_mode}, watermark_type={watermark_type})"
    )]
    LookbackUnsupported {
        node_id: String,
        read_mode: String,
        watermark_type: String,
    },

    #[error("sink `{node_id}`: `lookback` `{value}` is not a valid ISO 8601 duration")]
    LookbackInvalid { node_id: String, value: String },

    #[error("sink `{node_id}`: `snapshot` block is required when `write_strategy` is `snapshot`")]
    SnapshotBlockRequired { node_id: String },

    #[error("sink `{node_id}`: `snapshot` block must not be set for write_strategy `{strategy}`")]
    SnapshotBlockForbidden { node_id: String, strategy: String },

    #[error(
        "sink `{node_id}`: `snapshot.check_columns` is required and must be non-empty when `change_detection` is `check`"
    )]
    SnapshotCheckColumnsRequired { node_id: String },

    #[error(
        "sink `{node_id}`: `snapshot.updated_at_column` is required when `change_detection` is `timestamp`"
    )]
    SnapshotUpdatedAtRequired { node_id: String },

    #[error(
        "sink `{node_id}`: `change_detection: check` is incoherent with `read_mode: incremental` (cannot push down a column-set diff); use `change_detection: timestamp` or `read_mode: full`"
    )]
    SnapshotCheckIncrementalIncoherent { node_id: String },

    #[error(
        "sink `{node_id}`: `read_mode: incremental` requires `watermark.column` (`{watermark_column}`) to match `snapshot.updated_at_column` (`{updated_at_column}`)"
    )]
    SnapshotWatermarkMismatch {
        node_id: String,
        watermark_column: String,
        updated_at_column: String,
    },
}

/// Validate a single sink's materialization policy. `node_id` is used purely
/// for error messages.
pub fn validate_policy(
    node_id: &str,
    policy: &MaterializationPolicy,
) -> Result<(), Vec<MaterializationError>> {
    let mut errors = Vec::new();
    let strategy_str = policy.write_strategy.as_str().to_string();

    // -- watermark presence by read_mode --
    match policy.read_mode {
        ReadMode::Incremental => match &policy.watermark {
            None => errors.push(MaterializationError::WatermarkMissing {
                node_id: node_id.to_string(),
            }),
            Some(w) if w.column.trim().is_empty() => {
                errors.push(MaterializationError::WatermarkColumnEmpty {
                    node_id: node_id.to_string(),
                });
            }
            Some(_) => {}
        },
        ReadMode::Full => {
            if policy.watermark.is_some() {
                errors.push(MaterializationError::WatermarkForbidden {
                    node_id: node_id.to_string(),
                });
            }
        }
    }

    // -- unique_keys by write_strategy (independent of read_mode) --
    let needs_unique_keys = matches!(
        policy.write_strategy,
        WriteStrategy::Merge | WriteStrategy::DeleteInsert | WriteStrategy::Snapshot
    );
    let has_unique_keys = policy
        .unique_keys
        .as_ref()
        .map(|k| !k.is_empty())
        .unwrap_or(false);

    if needs_unique_keys && !has_unique_keys {
        errors.push(MaterializationError::UniqueKeysRequired {
            node_id: node_id.to_string(),
            strategy: strategy_str.clone(),
        });
    }
    if !needs_unique_keys && policy.unique_keys.is_some() {
        errors.push(MaterializationError::UniqueKeysForbidden {
            node_id: node_id.to_string(),
            strategy: strategy_str.clone(),
        });
    }

    // -- partition_column by write_strategy (independent of read_mode) --
    let needs_partition = matches!(policy.write_strategy, WriteStrategy::InsertOverwrite);
    if needs_partition && policy.partition_column.is_none() {
        errors.push(MaterializationError::PartitionColumnRequired {
            node_id: node_id.to_string(),
        });
    }
    if !needs_partition && policy.partition_column.is_some() {
        errors.push(MaterializationError::PartitionColumnForbidden {
            node_id: node_id.to_string(),
            strategy: strategy_str.clone(),
        });
    }

    // -- lookback rules: only meaningful under incremental + timestamp --
    if policy.lookback != "PT0S" {
        if !is_iso8601_duration(&policy.lookback) {
            errors.push(MaterializationError::LookbackInvalid {
                node_id: node_id.to_string(),
                value: policy.lookback.clone(),
            });
        }
        let watermark_is_timestamp = matches!(
            policy.watermark.as_ref().map(|w| w.watermark_type),
            Some(WatermarkType::Timestamp)
        );
        if !matches!(policy.read_mode, ReadMode::Incremental) || !watermark_is_timestamp {
            let read_mode = match policy.read_mode {
                ReadMode::Full => "full",
                ReadMode::Incremental => "incremental",
            };
            let wt = policy
                .watermark
                .as_ref()
                .map(|w| w.watermark_type.as_str())
                .unwrap_or("none");
            errors.push(MaterializationError::LookbackUnsupported {
                node_id: node_id.to_string(),
                read_mode: read_mode.to_string(),
                watermark_type: wt.to_string(),
            });
        }
    }

    // -- snapshot block presence by write_strategy --
    let is_snapshot = matches!(policy.write_strategy, WriteStrategy::Snapshot);
    match (&policy.snapshot, is_snapshot) {
        (None, true) => errors.push(MaterializationError::SnapshotBlockRequired {
            node_id: node_id.to_string(),
        }),
        (Some(_), false) => errors.push(MaterializationError::SnapshotBlockForbidden {
            node_id: node_id.to_string(),
            strategy: strategy_str.clone(),
        }),
        (Some(snap), true) => {
            // change_detection-specific field requirements
            match snap.change_detection {
                ChangeDetection::Check => {
                    let has_cols = snap
                        .check_columns
                        .as_ref()
                        .map(|c| !c.is_empty())
                        .unwrap_or(false);
                    if !has_cols {
                        errors.push(MaterializationError::SnapshotCheckColumnsRequired {
                            node_id: node_id.to_string(),
                        });
                    }
                    if matches!(policy.read_mode, ReadMode::Incremental) {
                        errors.push(MaterializationError::SnapshotCheckIncrementalIncoherent {
                            node_id: node_id.to_string(),
                        });
                    }
                }
                ChangeDetection::Timestamp => {
                    let updated_at = snap
                        .updated_at_column
                        .as_ref()
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty());
                    match updated_at {
                        None => errors.push(MaterializationError::SnapshotUpdatedAtRequired {
                            node_id: node_id.to_string(),
                        }),
                        Some(col) => {
                            // Under incremental read, the watermark column must
                            // match the updated_at_column — one source of truth.
                            if matches!(policy.read_mode, ReadMode::Incremental)
                                && let Some(wm) = policy.watermark.as_ref()
                                && wm.column != col
                            {
                                errors.push(MaterializationError::SnapshotWatermarkMismatch {
                                    node_id: node_id.to_string(),
                                    watermark_column: wm.column.clone(),
                                    updated_at_column: col.to_string(),
                                });
                            }
                        }
                    }
                }
            }
        }
        (None, false) => {}
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Cheap structural check for ISO 8601 durations.
///
/// Accepts strings of the shape `P[nY][nM][nD][T[nH][nM][nS]]`. We don't
/// pull in `chrono`/`iso8601` here because flux-engine deliberately stays
/// dependency-light; the executor will re-validate via `chrono::Duration` at
/// runtime when applying the lookback.
fn is_iso8601_duration(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() < 3 || bytes[0] != b'P' {
        return false;
    }
    let rest = &s[1..];
    if !rest.chars().any(|c| c.is_ascii_digit()) {
        return false;
    }
    let mut seen_t = false;
    for c in rest.chars() {
        match c {
            '0'..='9' => {}
            'Y' | 'M' | 'W' | 'D' | 'H' | 'S' => {}
            'T' => {
                if seen_t {
                    return false;
                }
                seen_t = true;
            }
            _ => return false,
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> MaterializationPolicy {
        MaterializationPolicy::default()
    }

    fn incr(strategy: WriteStrategy) -> MaterializationPolicy {
        MaterializationPolicy {
            read_mode: ReadMode::Incremental,
            write_strategy: strategy,
            watermark: Some(Watermark {
                column: "updated_at".into(),
                watermark_type: WatermarkType::Timestamp,
            }),
            ..base()
        }
    }

    #[test]
    fn default_round_trips() {
        let p = base();
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["read_mode"], "full");
        assert_eq!(json["write_strategy"], "append");
        assert!(json.get("watermark").is_none());
        let p2: MaterializationPolicy = serde_json::from_value(json).unwrap();
        assert_eq!(p2, p);
    }

    #[test]
    fn defaults_apply_when_fields_omitted() {
        // Empty object should deserialize as full+append.
        let p: MaterializationPolicy = serde_json::from_str("{}").unwrap();
        assert_eq!(p, base());
    }

    #[test]
    fn delete_insert_wire_spelling() {
        let json = serde_json::to_value(WriteStrategy::DeleteInsert).unwrap();
        assert_eq!(json, serde_json::json!("delete_insert"));
    }

    #[test]
    fn truncate_insert_wire_spelling() {
        let json = serde_json::to_value(WriteStrategy::TruncateInsert).unwrap();
        assert_eq!(json, serde_json::json!("truncate_insert"));
    }

    #[test]
    fn full_merge_is_valid_orthogonal_combo() {
        // The flagship orthogonality test: full read + merge on key.
        let p = MaterializationPolicy {
            write_strategy: WriteStrategy::Merge,
            unique_keys: Some(vec!["id".into()]),
            ..base()
        };
        validate_policy("sink", &p).unwrap();
    }

    #[test]
    fn full_truncate_insert_is_valid() {
        let p = MaterializationPolicy {
            write_strategy: WriteStrategy::TruncateInsert,
            ..base()
        };
        validate_policy("sink", &p).unwrap();
    }

    #[test]
    fn incremental_truncate_insert_is_allowed_no_error() {
        // Doc 27 calls this pathological but valid; we allow it without
        // error (warn channel is future work).
        let p = incr(WriteStrategy::TruncateInsert);
        validate_policy("sink", &p).unwrap();
    }

    #[test]
    fn merge_requires_unique_keys() {
        let p = incr(WriteStrategy::Merge);
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(matches!(
            errs[0],
            MaterializationError::UniqueKeysRequired { .. }
        ));
    }

    #[test]
    fn merge_with_unique_keys_ok() {
        let mut p = incr(WriteStrategy::Merge);
        p.unique_keys = Some(vec!["id".into()]);
        validate_policy("sink", &p).unwrap();
    }

    #[test]
    fn append_rejects_unique_keys() {
        let mut p = incr(WriteStrategy::Append);
        p.unique_keys = Some(vec!["id".into()]);
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(matches!(
            errs[0],
            MaterializationError::UniqueKeysForbidden { .. }
        ));
    }

    #[test]
    fn insert_overwrite_requires_partition_column() {
        let p = incr(WriteStrategy::InsertOverwrite);
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, MaterializationError::PartitionColumnRequired { .. }))
        );
    }

    #[test]
    fn insert_overwrite_with_partition_column_ok() {
        let mut p = incr(WriteStrategy::InsertOverwrite);
        p.partition_column = Some("event_date".into());
        validate_policy("sink", &p).unwrap();
    }

    #[test]
    fn merge_rejects_partition_column() {
        let mut p = incr(WriteStrategy::Merge);
        p.unique_keys = Some(vec!["id".into()]);
        p.partition_column = Some("d".into());
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, MaterializationError::PartitionColumnForbidden { .. }))
        );
    }

    #[test]
    fn full_mode_rejects_watermark() {
        let p = MaterializationPolicy {
            watermark: Some(Watermark {
                column: "updated_at".into(),
                watermark_type: WatermarkType::Timestamp,
            }),
            ..base()
        };
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(matches!(
            errs[0],
            MaterializationError::WatermarkForbidden { .. }
        ));
    }

    #[test]
    fn incremental_requires_watermark() {
        let p = MaterializationPolicy {
            read_mode: ReadMode::Incremental,
            ..base()
        };
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, MaterializationError::WatermarkMissing { .. }))
        );
    }

    #[test]
    fn lookback_rejected_for_int64_watermark() {
        let mut p = incr(WriteStrategy::Append);
        p.watermark = Some(Watermark {
            column: "id".into(),
            watermark_type: WatermarkType::Int64,
        });
        p.lookback = "PT1H".into();
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, MaterializationError::LookbackUnsupported { .. }))
        );
    }

    #[test]
    fn lookback_rejected_under_full_read_mode() {
        let p = MaterializationPolicy {
            lookback: "PT1H".into(),
            ..base()
        };
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, MaterializationError::LookbackUnsupported { .. }))
        );
    }

    #[test]
    fn lookback_invalid_format_rejected() {
        let mut p = incr(WriteStrategy::Append);
        p.lookback = "1 hour".into();
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, MaterializationError::LookbackInvalid { .. }))
        );
    }

    fn snap_base() -> SnapshotPolicy {
        SnapshotPolicy {
            change_detection: ChangeDetection::Check,
            check_columns: Some(vec!["email".into()]),
            updated_at_column: None,
            hard_deletes: HardDeletes::Ignore,
        }
    }

    #[test]
    fn snapshot_wire_spelling() {
        assert_eq!(
            serde_json::to_value(WriteStrategy::Snapshot).unwrap(),
            serde_json::json!("snapshot")
        );
        assert_eq!(
            serde_json::to_value(ChangeDetection::Timestamp).unwrap(),
            serde_json::json!("timestamp")
        );
        assert_eq!(
            serde_json::to_value(HardDeletes::Invalidate).unwrap(),
            serde_json::json!("invalidate")
        );
    }

    #[test]
    fn snapshot_full_check_ok() {
        let p = MaterializationPolicy {
            write_strategy: WriteStrategy::Snapshot,
            unique_keys: Some(vec!["customer_id".into()]),
            snapshot: Some(snap_base()),
            ..base()
        };
        validate_policy("sink", &p).unwrap();
    }

    #[test]
    fn snapshot_requires_unique_keys() {
        let p = MaterializationPolicy {
            write_strategy: WriteStrategy::Snapshot,
            snapshot: Some(snap_base()),
            ..base()
        };
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, MaterializationError::UniqueKeysRequired { .. }))
        );
    }

    #[test]
    fn snapshot_block_required_when_strategy_is_snapshot() {
        let p = MaterializationPolicy {
            write_strategy: WriteStrategy::Snapshot,
            unique_keys: Some(vec!["id".into()]),
            ..base()
        };
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, MaterializationError::SnapshotBlockRequired { .. }))
        );
    }

    #[test]
    fn snapshot_block_forbidden_when_strategy_is_not_snapshot() {
        let p = MaterializationPolicy {
            snapshot: Some(snap_base()),
            ..base()
        };
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, MaterializationError::SnapshotBlockForbidden { .. }))
        );
    }

    #[test]
    fn snapshot_check_requires_check_columns() {
        let mut snap = snap_base();
        snap.check_columns = None;
        let p = MaterializationPolicy {
            write_strategy: WriteStrategy::Snapshot,
            unique_keys: Some(vec!["id".into()]),
            snapshot: Some(snap),
            ..base()
        };
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, MaterializationError::SnapshotCheckColumnsRequired { .. }))
        );
    }

    #[test]
    fn snapshot_check_with_incremental_is_incoherent() {
        let p = MaterializationPolicy {
            read_mode: ReadMode::Incremental,
            write_strategy: WriteStrategy::Snapshot,
            unique_keys: Some(vec!["id".into()]),
            watermark: Some(Watermark {
                column: "updated_at".into(),
                watermark_type: WatermarkType::Timestamp,
            }),
            snapshot: Some(snap_base()),
            ..base()
        };
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(errs.iter().any(|e| matches!(
            e,
            MaterializationError::SnapshotCheckIncrementalIncoherent { .. }
        )));
    }

    #[test]
    fn snapshot_timestamp_requires_updated_at_column() {
        let snap = SnapshotPolicy {
            change_detection: ChangeDetection::Timestamp,
            check_columns: None,
            updated_at_column: None,
            hard_deletes: HardDeletes::Ignore,
        };
        let p = MaterializationPolicy {
            write_strategy: WriteStrategy::Snapshot,
            unique_keys: Some(vec!["id".into()]),
            snapshot: Some(snap),
            ..base()
        };
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, MaterializationError::SnapshotUpdatedAtRequired { .. }))
        );
    }

    #[test]
    fn snapshot_incremental_timestamp_watermark_must_match_updated_at() {
        let snap = SnapshotPolicy {
            change_detection: ChangeDetection::Timestamp,
            check_columns: None,
            updated_at_column: Some("modified_at".into()),
            hard_deletes: HardDeletes::Ignore,
        };
        let p = MaterializationPolicy {
            read_mode: ReadMode::Incremental,
            write_strategy: WriteStrategy::Snapshot,
            unique_keys: Some(vec!["id".into()]),
            watermark: Some(Watermark {
                column: "updated_at".into(),
                watermark_type: WatermarkType::Timestamp,
            }),
            snapshot: Some(snap),
            ..base()
        };
        let errs = validate_policy("sink", &p).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, MaterializationError::SnapshotWatermarkMismatch { .. }))
        );
    }

    #[test]
    fn snapshot_incremental_timestamp_matching_watermark_ok() {
        let snap = SnapshotPolicy {
            change_detection: ChangeDetection::Timestamp,
            check_columns: None,
            updated_at_column: Some("updated_at".into()),
            hard_deletes: HardDeletes::Ignore,
        };
        let p = MaterializationPolicy {
            read_mode: ReadMode::Incremental,
            write_strategy: WriteStrategy::Snapshot,
            unique_keys: Some(vec!["id".into()]),
            watermark: Some(Watermark {
                column: "updated_at".into(),
                watermark_type: WatermarkType::Timestamp,
            }),
            snapshot: Some(snap),
            ..base()
        };
        validate_policy("sink", &p).unwrap();
    }

    #[test]
    fn iso8601_duration_recognizer() {
        assert!(is_iso8601_duration("PT1H"));
        assert!(is_iso8601_duration("PT15M"));
        assert!(is_iso8601_duration("P1D"));
        assert!(is_iso8601_duration("P1DT2H"));
        assert!(!is_iso8601_duration("1H"));
        assert!(!is_iso8601_duration("P"));
        assert!(!is_iso8601_duration("PTH"));
    }
}
