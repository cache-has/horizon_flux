// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Generic stage-diff-merge planner for SCD2 snapshot materializations.
//!
//! Implements the pure, sink-agnostic core of the algorithm described in
//! `planning/28-snapshots-scd2.md` ("Execution Model"). Sinks
//! (postgres, duckdb-via-plugin, parquet) consume the [`SnapshotPlan`]
//! produced here and emit their own SQL / file rewrites; armillary-engine itself
//! does no I/O, no Arrow row processing, and no async work — it only:
//!
//! 1. Defines the canonical SCD2 metadata column set added to the target on
//!    first run ([`scd_metadata_columns`]).
//! 2. Generates surrogate keys for new versions ([`surrogate_key`]).
//! 3. Hashes business-column values for `change_detection: check`
//!    ([`check_hash`]).
//! 4. Classifies a join of (current target current versions) ⨝ (incoming
//!    staging rows) into [`RowClassification`]s and rolls them up into a
//!    [`SnapshotPlan`] + [`SnapshotMergeStats`] ([`plan_snapshot_merge`]).
//!
//! Hashing uses FNV-1a (64-bit) inline so the result is stable across
//! compilers, platforms, and rust releases — `std::collections::hash_map`'s
//! default hasher is explicitly *not* stable and would break round-trips
//! across runs.

use std::collections::HashMap;

use crate::materialization::{ChangeDetection, HardDeletes};

/// Column added to a snapshot target marking when this version became current.
/// Microsecond precision (see doc 28 "Open Questions" — soft timestamp precision).
pub const FLUX_VALID_FROM: &str = "armillary_valid_from";

/// Column added to a snapshot target marking when this version stopped being
/// current. `NULL` means "still current".
pub const FLUX_VALID_TO: &str = "armillary_valid_to";

/// Boolean convenience column equivalent to `armillary_valid_to IS NULL`.
pub const FLUX_IS_CURRENT: &str = "armillary_is_current";

/// Stable surrogate key for a single historical version of a row.
pub const FLUX_SCD_ID: &str = "armillary_scd_id";

/// Logical type of a snapshot metadata column. Sinks translate this into the
/// appropriate physical type for their storage backend (Postgres
/// `TIMESTAMPTZ`, DuckDB `TIMESTAMP`, Parquet `TIMESTAMP_MICROS`, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScdColumnType {
    /// Microsecond-precision timestamp with timezone.
    TimestampMicros,
    /// Boolean.
    Bool,
    /// Stable string surrogate key (hex-encoded FNV-1a).
    String,
}

/// One metadata column added to a snapshot target on first run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScdMetadataColumn {
    pub name: &'static str,
    pub logical_type: ScdColumnType,
    /// `false` for `armillary_valid_to` (NULL = still current); `true` everywhere else.
    pub not_null: bool,
}

/// The canonical set of metadata columns armillary adds to a snapshot target on
/// first run, in declaration order. Sinks translate `logical_type` into their
/// own DDL.
pub fn scd_metadata_columns() -> [ScdMetadataColumn; 4] {
    [
        ScdMetadataColumn {
            name: FLUX_SCD_ID,
            logical_type: ScdColumnType::String,
            not_null: true,
        },
        ScdMetadataColumn {
            name: FLUX_VALID_FROM,
            logical_type: ScdColumnType::TimestampMicros,
            not_null: true,
        },
        ScdMetadataColumn {
            name: FLUX_VALID_TO,
            logical_type: ScdColumnType::TimestampMicros,
            not_null: false,
        },
        ScdMetadataColumn {
            name: FLUX_IS_CURRENT,
            logical_type: ScdColumnType::Bool,
            not_null: true,
        },
    ]
}

/// FNV-1a 64-bit constants. Stable, deterministic, no_std-friendly.
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Stable 64-bit hash of a sequence of optional string fragments. Each
/// fragment is preceded by a one-byte tag distinguishing `None` from
/// `Some("")` so the hash of `[None, "x"]` differs from `["", "x"]`. A
/// 0xFE separator byte sits between fragments so concatenation collisions
/// (`["ab","c"]` vs `["a","bc"]`) cannot occur.
fn fnv1a_fields<'a, I>(fields: I) -> u64
where
    I: IntoIterator<Item = Option<&'a str>>,
{
    let mut hash = FNV_OFFSET;
    let mut first = true;
    for field in fields {
        if !first {
            hash ^= 0xFE;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        first = false;
        match field {
            None => {
                hash ^= 0x00;
                hash = hash.wrapping_mul(FNV_PRIME);
            }
            Some(s) => {
                hash ^= 0x01;
                hash = hash.wrapping_mul(FNV_PRIME);
                for byte in s.as_bytes() {
                    hash ^= *byte as u64;
                    hash = hash.wrapping_mul(FNV_PRIME);
                }
            }
        }
    }
    hash
}

/// Stable surrogate key for a single historical version of a logical row.
///
/// Computed as `fnv1a(unique_key_values || valid_from_micros)`, hex-encoded.
/// Sinks store this in [`FLUX_SCD_ID`]. Two rows produced by two different
/// runs with different `valid_from` timestamps therefore get different
/// surrogate keys, even if all other columns match — that's the point.
pub fn surrogate_key(unique_key_values: &[Option<&str>], valid_from_micros: i64) -> String {
    let micros_str = valid_from_micros.to_string();
    let mut owned: Vec<Option<&str>> = unique_key_values.to_vec();
    owned.push(Some(micros_str.as_str()));
    format!("{:016x}", fnv1a_fields(owned))
}

/// Stable change-detection hash for `change_detection: check`. Sinks call
/// this with the values of every column listed in `SnapshotPolicy::check_columns`
/// (or every business column if `check_columns: "*"`).
///
/// The result is what armillary compares between the target's current version
/// and the staged incoming row to decide "did this row change?".
pub fn check_hash(values: &[Option<&str>]) -> u64 {
    fnv1a_fields(values.iter().copied())
}

/// Result of comparing one logical row from the staging table against the
/// target's current versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowClassification {
    /// Row exists in both with matching change-detection signal — no-op.
    Unchanged,
    /// Row exists in both with differing change-detection signal — close
    /// the target's current version, insert the staging row as a new version.
    Changed,
    /// Row exists only in staging — insert as a brand-new version.
    New,
    /// Row exists only in the target's current versions (missing from
    /// staging this run). Handled per [`HardDeletes`].
    Gone,
}

/// Per-business-key classification produced by [`plan_snapshot_merge`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassifiedRow {
    pub unique_key: Vec<String>,
    pub classification: RowClassification,
}

/// Roll-up counts produced by [`plan_snapshot_merge`]. Maps directly onto
/// the snapshot accounting fields of `MaterializationReceipt`:
///
/// | receipt field    | snapshot meaning                                |
/// |------------------|-------------------------------------------------|
/// | `rows_inserted`  | new versions opened (= `new_versions + changed`)|
/// | `rows_updated`   | current versions closed (= `changed + invalidated_deletes`) |
/// | `rows_deleted`   | physically removed rows (`HardDeletes::Delete`) |
///
/// `unchanged` is informational — it doesn't appear on the receipt but is
/// useful for the `armillary snapshot diff` CLI subcommand.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotMergeStats {
    pub unchanged: u64,
    pub changed: u64,
    pub new_versions: u64,
    pub gone: u64,
    pub invalidated_deletes: u64,
    pub physical_deletes: u64,
}

impl SnapshotMergeStats {
    /// Row count to set on `MaterializationReceipt::rows_inserted`.
    pub fn receipt_rows_inserted(&self) -> u64 {
        self.new_versions + self.changed
    }

    /// Row count to set on `MaterializationReceipt::rows_updated`.
    pub fn receipt_rows_updated(&self) -> u64 {
        self.changed + self.invalidated_deletes
    }

    /// Row count to set on `MaterializationReceipt::rows_deleted`.
    pub fn receipt_rows_deleted(&self) -> u64 {
        self.physical_deletes
    }
}

/// Output of [`plan_snapshot_merge`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotPlan {
    pub rows: Vec<ClassifiedRow>,
    pub stats: SnapshotMergeStats,
}

/// One incoming row, as the sink's staging step has materialized it. The
/// `signal` is the value used to decide "did this row change?":
///
/// - For `change_detection: check`, sinks pass [`check_hash`] of the
///   tracked columns.
/// - For `change_detection: timestamp`, sinks pass the row's
///   `updated_at` value (Unix microseconds, or any monotonic encoding —
///   the planner only checks equality vs. the target).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedRow {
    pub unique_key: Vec<String>,
    pub signal: i64,
}

/// One row currently marked `armillary_is_current = true` in the target. The
/// `signal` must be encoded the same way the sink encodes [`StagedRow::signal`]
/// for the active [`ChangeDetection`] mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentTargetRow {
    pub unique_key: Vec<String>,
    pub signal: i64,
}

/// Pure stage-diff-merge planner. Given the target's current versions and
/// the incoming staged batch, classify every row and produce the rollup
/// counts a sink needs to drive its transactional MERGE.
///
/// `change_detection` selects the comparison: both `Check` and `Timestamp`
/// reduce to "is the staged signal equal to the current target signal?",
/// so the planner is identical for the two — the only difference is *what*
/// the sink put in the `signal` field. The parameter is kept on the
/// signature anyway so callers self-document and so future modes (e.g. an
/// always-replace mode) can be added without changing call sites.
pub fn plan_snapshot_merge(
    current: impl IntoIterator<Item = CurrentTargetRow>,
    staged: impl IntoIterator<Item = StagedRow>,
    _change_detection: ChangeDetection,
    hard_deletes: HardDeletes,
) -> SnapshotPlan {
    let mut current_by_key: HashMap<Vec<String>, i64> = HashMap::new();
    for row in current {
        current_by_key.insert(row.unique_key, row.signal);
    }

    let mut rows: Vec<ClassifiedRow> = Vec::new();
    let mut stats = SnapshotMergeStats::default();

    for s in staged {
        match current_by_key.remove(&s.unique_key) {
            None => {
                stats.new_versions += 1;
                rows.push(ClassifiedRow {
                    unique_key: s.unique_key,
                    classification: RowClassification::New,
                });
            }
            Some(target_signal) if target_signal == s.signal => {
                stats.unchanged += 1;
                rows.push(ClassifiedRow {
                    unique_key: s.unique_key,
                    classification: RowClassification::Unchanged,
                });
            }
            Some(_) => {
                stats.changed += 1;
                rows.push(ClassifiedRow {
                    unique_key: s.unique_key,
                    classification: RowClassification::Changed,
                });
            }
        }
    }

    // Anything still in current_by_key was present in the target but missing
    // from staging — those are "gone" rows, handled per `hard_deletes`.
    for (unique_key, _) in current_by_key {
        stats.gone += 1;
        match hard_deletes {
            HardDeletes::Ignore => {}
            HardDeletes::Invalidate => stats.invalidated_deletes += 1,
            HardDeletes::Delete => stats.physical_deletes += 1,
        }
        rows.push(ClassifiedRow {
            unique_key,
            classification: RowClassification::Gone,
        });
    }

    SnapshotPlan { rows, stats }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn metadata_columns_are_canonical_and_ordered() {
        let cols = scd_metadata_columns();
        assert_eq!(cols[0].name, FLUX_SCD_ID);
        assert_eq!(cols[1].name, FLUX_VALID_FROM);
        assert_eq!(cols[2].name, FLUX_VALID_TO);
        assert_eq!(cols[3].name, FLUX_IS_CURRENT);
        // valid_to is the only nullable one — current versions store NULL there.
        assert!(!cols[2].not_null);
        assert!(cols[0].not_null && cols[1].not_null && cols[3].not_null);
    }

    #[test]
    fn surrogate_key_is_stable_and_versioned() {
        let a = surrogate_key(&[Some("42")], 1_700_000_000_000_000);
        let a_again = surrogate_key(&[Some("42")], 1_700_000_000_000_000);
        let b = surrogate_key(&[Some("42")], 1_700_000_000_000_001);
        assert_eq!(a, a_again, "deterministic across calls");
        assert_ne!(a, b, "different valid_from → different surrogate key");
        assert_eq!(a.len(), 16, "16 hex chars = 64 bits");
    }

    #[test]
    fn surrogate_key_separates_field_boundaries() {
        // Without a separator, ["ab","c"] and ["a","bc"] would collide.
        let a = surrogate_key(&[Some("ab"), Some("c")], 0);
        let b = surrogate_key(&[Some("a"), Some("bc")], 0);
        assert_ne!(a, b);
    }

    #[test]
    fn check_hash_distinguishes_none_from_empty_string() {
        assert_ne!(check_hash(&[None]), check_hash(&[Some("")]));
    }

    #[test]
    fn check_hash_changes_when_any_tracked_column_changes() {
        let base = check_hash(&[Some("alice@example.com"), Some("pro"), Some("active")]);
        let email_changed = check_hash(&[Some("alice@new.com"), Some("pro"), Some("active")]);
        let plan_changed = check_hash(&[Some("alice@example.com"), Some("free"), Some("active")]);
        assert_ne!(base, email_changed);
        assert_ne!(base, plan_changed);
    }

    #[test]
    fn plan_classifies_new_unchanged_changed_and_gone() {
        let current = vec![
            CurrentTargetRow {
                unique_key: k(&["1"]),
                signal: 100,
            },
            CurrentTargetRow {
                unique_key: k(&["2"]),
                signal: 200,
            },
            CurrentTargetRow {
                unique_key: k(&["3"]),
                signal: 300,
            },
        ];
        let staged = vec![
            // 1: same signal → Unchanged
            StagedRow {
                unique_key: k(&["1"]),
                signal: 100,
            },
            // 2: signal differs → Changed
            StagedRow {
                unique_key: k(&["2"]),
                signal: 999,
            },
            // 4: not in target → New
            StagedRow {
                unique_key: k(&["4"]),
                signal: 400,
            },
            // 3 absent from staging → Gone
        ];

        let plan =
            plan_snapshot_merge(current, staged, ChangeDetection::Check, HardDeletes::Ignore);

        assert_eq!(plan.stats.unchanged, 1);
        assert_eq!(plan.stats.changed, 1);
        assert_eq!(plan.stats.new_versions, 1);
        assert_eq!(plan.stats.gone, 1);
        assert_eq!(plan.stats.invalidated_deletes, 0);
        assert_eq!(plan.stats.physical_deletes, 0);
    }

    #[test]
    fn hard_deletes_invalidate_increments_invalidated_count() {
        let current = vec![CurrentTargetRow {
            unique_key: k(&["7"]),
            signal: 1,
        }];
        let plan = plan_snapshot_merge(
            current,
            std::iter::empty::<StagedRow>(),
            ChangeDetection::Check,
            HardDeletes::Invalidate,
        );
        assert_eq!(plan.stats.gone, 1);
        assert_eq!(plan.stats.invalidated_deletes, 1);
        assert_eq!(plan.stats.physical_deletes, 0);
    }

    #[test]
    fn hard_deletes_delete_increments_physical_count() {
        let current = vec![CurrentTargetRow {
            unique_key: k(&["7"]),
            signal: 1,
        }];
        let plan = plan_snapshot_merge(
            current,
            std::iter::empty::<StagedRow>(),
            ChangeDetection::Check,
            HardDeletes::Delete,
        );
        assert_eq!(plan.stats.physical_deletes, 1);
    }

    #[test]
    fn receipt_rollup_matches_doc_28_definition() {
        // Stage built by hand: 2 new, 3 changed, 1 unchanged, 2 gone+invalidate.
        let stats = SnapshotMergeStats {
            unchanged: 1,
            changed: 3,
            new_versions: 2,
            gone: 2,
            invalidated_deletes: 2,
            physical_deletes: 0,
        };
        // doc 28: rows_inserted = new versions opened = new + changed
        assert_eq!(stats.receipt_rows_inserted(), 5);
        // doc 28: rows_updated = current versions closed = changed + invalidate-closed
        assert_eq!(stats.receipt_rows_updated(), 5);
        // doc 28: rows_deleted = hard-delete count
        assert_eq!(stats.receipt_rows_deleted(), 0);
    }

    #[test]
    fn timestamp_mode_uses_signal_equality() {
        // Same business key, same updated_at → unchanged.
        let plan = plan_snapshot_merge(
            vec![CurrentTargetRow {
                unique_key: k(&["c"]),
                signal: 1_700_000_000,
            }],
            vec![StagedRow {
                unique_key: k(&["c"]),
                signal: 1_700_000_000,
            }],
            ChangeDetection::Timestamp,
            HardDeletes::Ignore,
        );
        assert_eq!(plan.stats.unchanged, 1);
        assert_eq!(plan.stats.changed, 0);
    }

    #[test]
    fn plan_is_idempotent_when_run_twice_against_no_changes() {
        let target = vec![
            CurrentTargetRow {
                unique_key: k(&["a"]),
                signal: 1,
            },
            CurrentTargetRow {
                unique_key: k(&["b"]),
                signal: 2,
            },
        ];
        let staged = vec![
            StagedRow {
                unique_key: k(&["a"]),
                signal: 1,
            },
            StagedRow {
                unique_key: k(&["b"]),
                signal: 2,
            },
        ];
        let plan = plan_snapshot_merge(target, staged, ChangeDetection::Check, HardDeletes::Ignore);
        assert_eq!(plan.stats.unchanged, 2);
        assert_eq!(plan.stats.changed, 0);
        assert_eq!(plan.stats.new_versions, 0);
        assert_eq!(plan.stats.gone, 0);
        // Doc 28 success criterion: idempotent re-runs produce zero new versions.
        assert_eq!(plan.stats.receipt_rows_inserted(), 0);
        assert_eq!(plan.stats.receipt_rows_updated(), 0);
    }
}
