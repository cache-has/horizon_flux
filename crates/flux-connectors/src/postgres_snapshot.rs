// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! PostgreSQL SCD2 snapshot stage-diff-merge implementation.
//!
//! Implements `WriteStrategy::Snapshot` for the Postgres sink, per
//! `planning/28-snapshots-scd2.md`. Lives next to (rather than inside)
//! `postgres_sink.rs` so the snapshot SQL doesn't drown the rest of the
//! sink's existing strategies.
//!
//! ## Approach
//!
//! Native server-side stage-diff-merge: a `TEMP TABLE … ON COMMIT DROP`
//! is populated from the incoming Arrow batches via the same row-binding
//! path the other strategies use, then a small set of SQL statements does
//! the diff and the merge inside the existing per-write transaction. We
//! deliberately do not compute fingerprints in Rust here — Postgres'
//! `IS DISTINCT FROM` correctly handles every Arrow type the sink can
//! write and avoids cross-language hash drift between staging and target.
//!
//! `flux_engine::snapshot` (the pure planner) is still the canonical
//! algorithm — DuckDB / Parquet sinks reuse it because they cannot push
//! the diff into the storage engine. Both paths feed the same
//! [`flux_engine::SnapshotMergeStats`] roll-up so the
//! `MaterializationReceipt` accounting from doc 28 is consistent
//! sink-to-sink.

use arrow::datatypes::Schema;
use flux_datafusion::provider::ProviderError;
use flux_engine::materialization::{ChangeDetection, HardDeletes, SnapshotPolicy};
use flux_engine::snapshot::{
    FLUX_IS_CURRENT, FLUX_SCD_ID, FLUX_VALID_FROM, FLUX_VALID_TO, SnapshotMergeStats,
};

const STAGE_TABLE: &str = "_flux_snap_stage";

/// Validate that the snapshot policy can be executed against the incoming
/// schema. Most rules are already enforced upstream by
/// `flux_engine::materialization::validate_policy`; this is the runtime
/// "trust but verify" pass that surfaces a clear connector-side error
/// instead of a generic SQL failure.
pub(crate) fn validate_snapshot_runtime(
    schema: &Schema,
    unique_keys: &[String],
    policy: &SnapshotPolicy,
) -> Result<(), ProviderError> {
    if unique_keys.is_empty() {
        return Err("postgresql snapshot requires non-empty `unique_keys`".into());
    }
    for k in unique_keys {
        if schema.index_of(k).is_err() {
            return Err(format!(
                "postgresql snapshot unique_key '{k}' is not present in incoming schema"
            )
            .into());
        }
    }
    match policy.change_detection {
        ChangeDetection::Check => {
            let cols = policy
                .check_columns
                .as_ref()
                .ok_or("postgresql snapshot `check` requires `check_columns`")?;
            for c in cols.iter().filter(|c| c.as_str() != "*") {
                if schema.index_of(c).is_err() {
                    return Err(format!(
                        "postgresql snapshot check_column '{c}' is not present in incoming schema"
                    )
                    .into());
                }
            }
        }
        ChangeDetection::Timestamp => {
            let col = policy
                .updated_at_column
                .as_ref()
                .ok_or("postgresql snapshot `timestamp` requires `updated_at_column`")?;
            if schema.index_of(col).is_err() {
                return Err(format!(
                    "postgresql snapshot updated_at_column '{col}' is not present in incoming schema"
                )
                .into());
            }
        }
    }
    Ok(())
}

/// The set of columns whose values are compared (via `IS DISTINCT FROM`)
/// to decide "did this row change?". For check mode this is the configured
/// `check_columns` (with `"*"` expanding to all non-key business columns);
/// for timestamp mode it's just the `updated_at_column`.
pub(crate) fn comparison_columns(
    schema: &Schema,
    unique_keys: &[String],
    policy: &SnapshotPolicy,
) -> Vec<String> {
    match policy.change_detection {
        ChangeDetection::Check => {
            let cols = policy.check_columns.as_ref().expect("validated above");
            if cols.iter().any(|c| c == "*") {
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .filter(|n| !unique_keys.contains(n))
                    .collect()
            } else {
                cols.clone()
            }
        }
        ChangeDetection::Timestamp => {
            vec![policy.updated_at_column.clone().expect("validated above")]
        }
    }
}

/// Quote a SQL identifier the same way `postgres_sink::quote_ident` does.
/// Inlined here so this module is independent of `postgres_sink` internals.
fn q(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Build the `CREATE TABLE IF NOT EXISTS` for a snapshot target. Takes the
/// existing per-column DDL fragments produced by the regular create-table
/// path and appends the four SCD2 metadata columns from
/// `flux_engine::snapshot::scd_metadata_columns`.
///
/// The metadata columns must come last so the business-column ordering
/// matches the incoming Arrow schema (the row-binding loop relies on
/// positional `$N` placeholders).
pub(crate) fn build_create_snapshot_table_sql(
    table: &str,
    business_column_ddls: &[String],
    unique_keys: &[String],
) -> String {
    let mut cols: Vec<String> = business_column_ddls.to_vec();
    // Metadata columns. Postgres physical types are chosen to match
    // `ScdColumnType`: TEXT for the surrogate key (16 hex chars from
    // md5), TIMESTAMPTZ with microsecond precision for the validity
    // window, BOOLEAN for the convenience flag.
    cols.push(format!("{} TEXT NOT NULL", q(FLUX_SCD_ID)));
    cols.push(format!("{} TIMESTAMPTZ NOT NULL", q(FLUX_VALID_FROM)));
    cols.push(format!("{} TIMESTAMPTZ", q(FLUX_VALID_TO)));
    cols.push(format!("{} BOOLEAN NOT NULL", q(FLUX_IS_CURRENT)));

    // Index on the business key + flux_is_current accelerates every
    // diff round-trip. We can't put it inside CREATE TABLE; the sink
    // emits a follow-up CREATE INDEX after the table exists. The DDL
    // here only creates the table.
    let _ = unique_keys; // index creation handled in build_snapshot_index_sql
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        q(table),
        cols.join(", ")
    )
}

/// Build the helper index that makes the per-run diff cheap. Mirrors
/// the standard pattern: `(unique_keys, flux_is_current)` so the
/// "currently active version of key X" lookup is a single index scan.
pub(crate) fn build_snapshot_index_sql(table: &str, unique_keys: &[String]) -> String {
    let cols: Vec<String> = unique_keys.iter().map(|k| q(k)).collect();
    let idx_name = format!(
        "idx_{}_flux_current",
        table.replace('"', "").replace('.', "_")
    );
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} ({}, {})",
        q(&idx_name),
        q(table),
        cols.join(", "),
        q(FLUX_IS_CURRENT)
    )
}

/// `CREATE TEMP TABLE … ON COMMIT DROP` mirroring the incoming Arrow
/// schema. Used as the "stage" half of the stage-diff-merge.
pub(crate) fn build_create_stage_table_sql(business_column_ddls: &[String]) -> String {
    format!(
        "CREATE TEMP TABLE {} ({}) ON COMMIT DROP",
        q(STAGE_TABLE),
        business_column_ddls.join(", ")
    )
}

pub(crate) fn stage_table_name() -> &'static str {
    STAGE_TABLE
}

/// Single round-trip "what changed?" query. Returns three result columns:
///
/// - `changed_ids text[]` — `flux_scd_id` of every current target row
///   whose business key matches a stage row but whose comparison columns
///   differ. These get closed and a new version inserted.
/// - `gone_ids text[]` — `flux_scd_id` of every current target row whose
///   key is missing from the stage. Handled per `hard_deletes`.
/// - `new_count bigint` — number of stage rows whose key has no current
///   version in the target. These get inserted as brand-new versions.
///
/// `unchanged` is `total_stage_rows - new_count - len(changed_ids)`,
/// computed by the caller from the row count it already tracks.
pub(crate) fn build_diff_query(
    table: &str,
    unique_keys: &[String],
    comparison_cols: &[String],
) -> String {
    let key_join = unique_keys
        .iter()
        .map(|k| format!("t.{} = s.{}", q(k), q(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let key_join_left = unique_keys
        .iter()
        .map(|k| format!("s.{} = t.{}", q(k), q(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let distinct_cond = comparison_cols
        .iter()
        .map(|c| format!("t.{c} IS DISTINCT FROM s.{c}", c = q(c)))
        .collect::<Vec<_>>()
        .join(" OR ");
    let first_key = q(&unique_keys[0]);
    format!(
        "SELECT \
           COALESCE((SELECT array_agg(t.{scd_id}) \
                     FROM {target} t JOIN {stage} s ON {key_join} \
                     WHERE t.{is_current} AND ({distinct_cond})), ARRAY[]::text[]) AS changed_ids, \
           COALESCE((SELECT array_agg(t.{scd_id}) \
                     FROM {target} t LEFT JOIN {stage} s ON {key_join} \
                     WHERE t.{is_current} AND s.{first_key} IS NULL), ARRAY[]::text[]) AS gone_ids, \
           (SELECT COUNT(*) \
            FROM {stage} s LEFT JOIN {target} t ON {key_join_left} AND t.{is_current} \
            WHERE t.{first_key} IS NULL)::bigint AS new_count",
        scd_id = q(FLUX_SCD_ID),
        is_current = q(FLUX_IS_CURRENT),
        target = q(table),
        stage = q(STAGE_TABLE),
        key_join = key_join,
        key_join_left = key_join_left,
        distinct_cond = distinct_cond,
        first_key = first_key,
    )
}

/// `UPDATE target SET flux_valid_to = $1, flux_is_current = false WHERE flux_scd_id = ANY($2)`.
/// Closes a set of current versions in one shot. The caller passes the
/// union of `changed_ids` and (when `HardDeletes::Invalidate`) `gone_ids`
/// in `$2`.
pub(crate) fn build_close_versions_sql(table: &str) -> String {
    format!(
        "UPDATE {} SET {} = $1, {} = false WHERE {} = ANY($2)",
        q(table),
        q(FLUX_VALID_TO),
        q(FLUX_IS_CURRENT),
        q(FLUX_SCD_ID),
    )
}

/// Insert new versions for every stage row that is either *changed* or
/// *new*. Filter is the disjunction `(no current target row) OR
/// (current target row with at least one differing comparison column)`,
/// which is exactly the negation of "unchanged".
///
/// Surrogate key is `md5(now || pipe-joined unique key text)`. We use
/// Postgres-side md5 (rather than `flux_engine::snapshot::surrogate_key`)
/// so the value is generated atomically with the INSERT and we don't have
/// to bind one parameter per row. The flux-engine helper is still
/// canonical for non-SQL sinks (DuckDB, Parquet) where we materialize
/// surrogate keys in Rust before writing.
pub(crate) fn build_insert_new_versions_sql(
    table: &str,
    business_columns: &[String],
    unique_keys: &[String],
    comparison_cols: &[String],
) -> String {
    let business_quoted: Vec<String> = business_columns.iter().map(|c| q(c)).collect();
    let stage_select: Vec<String> = business_columns
        .iter()
        .map(|c| format!("s.{}", q(c)))
        .collect();
    let key_join = unique_keys
        .iter()
        .map(|k| format!("s.{} = t.{}", q(k), q(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let distinct_cond = comparison_cols
        .iter()
        .map(|c| format!("t.{c} IS DISTINCT FROM s.{c}", c = q(c)))
        .collect::<Vec<_>>()
        .join(" OR ");
    let first_key = q(&unique_keys[0]);

    // Build "concat_ws('|', $1::text, s.k1::text, s.k2::text)" for the surrogate key.
    // $1 = now (TIMESTAMPTZ), so casting to text gives a deterministic per-run prefix.
    // Cast through timestamptz first so the param's inferred type is
    // unambiguously timestamptz (otherwise PG can infer text from the
    // bare $1::text usage and refuse to bind a chrono::DateTime).
    let mut key_text_args = vec!["$1::timestamptz::text".to_string()];
    for k in unique_keys {
        key_text_args.push(format!("s.{}::text", q(k)));
    }
    let surrogate_expr = format!("md5(concat_ws('|', {}))", key_text_args.join(", "));

    format!(
        "INSERT INTO {target} ({biz_cols}, {scd_id}, {valid_from}, {valid_to}, {is_current}) \
         SELECT {stage_select}, {surrogate_expr}, $1::timestamptz, NULL, true \
         FROM {stage} s LEFT JOIN {target} t ON {key_join} AND t.{is_current} \
         WHERE t.{first_key} IS NULL OR ({distinct_cond})",
        target = q(table),
        biz_cols = business_quoted.join(", "),
        scd_id = q(FLUX_SCD_ID),
        valid_from = q(FLUX_VALID_FROM),
        valid_to = q(FLUX_VALID_TO),
        is_current = q(FLUX_IS_CURRENT),
        stage_select = stage_select.join(", "),
        surrogate_expr = surrogate_expr,
        stage = q(STAGE_TABLE),
        key_join = key_join,
        first_key = first_key,
        distinct_cond = distinct_cond,
    )
}

/// `DELETE FROM target WHERE (k1, k2) IN (... gone-key tuples ...)`.
/// Used only for `HardDeletes::Delete`, which removes every historical
/// version of a key — not just the current row. The caller binds the
/// gone unique-key tuples; we use a `VALUES` clause rather than `ANY` so
/// composite keys work without composite-array gymnastics.
pub(crate) fn build_hard_delete_sql(
    table: &str,
    unique_keys: &[String],
    n_tuples: usize,
) -> String {
    // Cast each key column to text on the LHS so the IN comparison works
    // regardless of whether the bound text values match the column's
    // physical type (BIGINT, INT, TEXT, etc.). The caller binds tuple
    // values as String — see postgres_sink.rs hard-delete branch.
    let key_cols = unique_keys
        .iter()
        .map(|k| format!("{}::text", q(k)))
        .collect::<Vec<_>>()
        .join(", ");
    let arity = unique_keys.len();
    let mut placeholders = Vec::with_capacity(n_tuples);
    let mut idx = 1usize;
    for _ in 0..n_tuples {
        let row: Vec<String> = (0..arity)
            .map(|_| {
                let s = format!("${idx}");
                idx += 1;
                s
            })
            .collect();
        placeholders.push(format!("({})", row.join(", ")));
    }
    format!(
        "DELETE FROM {} WHERE ({}) IN (VALUES {})",
        q(table),
        key_cols,
        placeholders.join(", ")
    )
}

/// Roll the raw counters from the diff query into the canonical
/// [`SnapshotMergeStats`]. The caller has already executed the close /
/// insert / delete statements based on these numbers; this is just the
/// receipt-population step.
pub(crate) fn stats_from_diff(
    total_stage_rows: u64,
    changed: u64,
    new_versions: u64,
    gone: u64,
    hard_deletes: HardDeletes,
) -> SnapshotMergeStats {
    let unchanged = total_stage_rows
        .saturating_sub(changed)
        .saturating_sub(new_versions);
    let (invalidated, physical) = match hard_deletes {
        HardDeletes::Ignore => (0, 0),
        HardDeletes::Invalidate => (gone, 0),
        HardDeletes::Delete => (0, gone),
    };
    SnapshotMergeStats {
        unchanged,
        changed,
        new_versions,
        gone,
        invalidated_deletes: invalidated,
        physical_deletes: physical,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
            Field::new("plan", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
        ])
    }

    fn check_policy(cols: Vec<&str>) -> SnapshotPolicy {
        SnapshotPolicy {
            change_detection: ChangeDetection::Check,
            check_columns: Some(cols.into_iter().map(String::from).collect()),
            updated_at_column: None,
            hard_deletes: HardDeletes::Ignore,
        }
    }

    #[test]
    fn comparison_columns_uses_check_columns_when_provided() {
        let cols = comparison_columns(
            &schema(),
            &["id".into()],
            &check_policy(vec!["email", "plan"]),
        );
        assert_eq!(cols, vec!["email", "plan"]);
    }

    #[test]
    fn comparison_columns_star_expands_to_non_key_business_cols() {
        let cols = comparison_columns(&schema(), &["id".into()], &check_policy(vec!["*"]));
        assert_eq!(cols, vec!["email", "plan", "status"]);
    }

    #[test]
    fn comparison_columns_timestamp_returns_updated_at() {
        let policy = SnapshotPolicy {
            change_detection: ChangeDetection::Timestamp,
            check_columns: None,
            updated_at_column: Some("updated_at".into()),
            hard_deletes: HardDeletes::Ignore,
        };
        let cols = comparison_columns(&schema(), &["id".into()], &policy);
        assert_eq!(cols, vec!["updated_at"]);
    }

    #[test]
    fn validate_runtime_rejects_missing_unique_key() {
        let err = validate_snapshot_runtime(
            &schema(),
            &["nonexistent".into()],
            &check_policy(vec!["email"]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn validate_runtime_rejects_missing_check_column() {
        let err = validate_snapshot_runtime(&schema(), &["id".into()], &check_policy(vec!["nope"]))
            .unwrap_err();
        assert!(err.to_string().contains("nope"));
    }

    #[test]
    fn create_snapshot_table_sql_has_metadata_at_end() {
        let business = vec!["\"id\" BIGINT".to_string(), "\"email\" TEXT".to_string()];
        let sql = build_create_snapshot_table_sql("customers", &business, &["id".into()]);
        // Business columns come first, metadata last (in canonical order).
        let id_at = sql.find("\"id\" BIGINT").unwrap();
        let email_at = sql.find("\"email\" TEXT").unwrap();
        let scd_id_at = sql.find("\"flux_scd_id\"").unwrap();
        let valid_from_at = sql.find("\"flux_valid_from\"").unwrap();
        let valid_to_at = sql.find("\"flux_valid_to\"").unwrap();
        let is_current_at = sql.find("\"flux_is_current\"").unwrap();
        assert!(id_at < email_at);
        assert!(email_at < scd_id_at);
        assert!(scd_id_at < valid_from_at);
        assert!(valid_from_at < valid_to_at);
        assert!(valid_to_at < is_current_at);
        // valid_to is the only nullable metadata column.
        assert!(sql.contains("\"flux_valid_to\" TIMESTAMPTZ,"));
        assert!(sql.contains("\"flux_scd_id\" TEXT NOT NULL"));
        assert!(sql.contains("\"flux_is_current\" BOOLEAN NOT NULL"));
    }

    #[test]
    fn diff_query_includes_all_three_buckets() {
        let sql = build_diff_query(
            "customers",
            &["id".into()],
            &["email".into(), "plan".into()],
        );
        assert!(sql.contains("changed_ids"));
        assert!(sql.contains("gone_ids"));
        assert!(sql.contains("new_count"));
        assert!(sql.contains("\"email\" IS DISTINCT FROM"));
        assert!(sql.contains("\"plan\" IS DISTINCT FROM"));
        assert!(sql.contains("\"flux_is_current\""));
    }

    #[test]
    fn close_versions_sql_uses_array_param() {
        let sql = build_close_versions_sql("customers");
        assert!(sql.contains("= ANY($2)"));
        assert!(sql.contains("\"flux_valid_to\" = $1"));
        assert!(sql.contains("\"flux_is_current\" = false"));
    }

    #[test]
    fn insert_new_versions_sql_uses_md5_surrogate_and_correct_filter() {
        let sql = build_insert_new_versions_sql(
            "customers",
            &["id".into(), "email".into(), "plan".into()],
            &["id".into()],
            &["email".into(), "plan".into()],
        );
        assert!(sql.contains("md5(concat_ws"));
        assert!(sql.contains("$1::timestamptz::text"));
        assert!(sql.contains("s.\"id\"::text"));
        // Filter: new (NULL key) OR changed (any distinct column).
        assert!(sql.contains("t.\"id\" IS NULL"));
        assert!(sql.contains("\"email\" IS DISTINCT FROM"));
        // SCD2 metadata columns are populated.
        assert!(sql.contains("\"flux_scd_id\""));
        assert!(sql.contains("\"flux_valid_from\""));
        assert!(sql.contains(", NULL, true"));
    }

    #[test]
    fn hard_delete_sql_uses_value_tuples_for_composite_keys() {
        let sql = build_hard_delete_sql("customers", &["tenant".into(), "id".into()], 2);
        // 4 placeholders: $1, $2 for first tuple; $3, $4 for second.
        assert!(sql.contains("($1, $2)"));
        assert!(sql.contains("($3, $4)"));
        assert!(sql.contains("(\"tenant\"::text, \"id\"::text) IN"));
    }

    #[test]
    fn stats_rollup_invalidate_sets_invalidated_count() {
        let stats = stats_from_diff(10, 2, 3, 4, HardDeletes::Invalidate);
        assert_eq!(stats.unchanged, 5);
        assert_eq!(stats.changed, 2);
        assert_eq!(stats.new_versions, 3);
        assert_eq!(stats.gone, 4);
        assert_eq!(stats.invalidated_deletes, 4);
        assert_eq!(stats.physical_deletes, 0);
        // Doc 28 receipt rules: rows_inserted = new + changed.
        assert_eq!(stats.receipt_rows_inserted(), 5);
        // rows_updated = changed + invalidated.
        assert_eq!(stats.receipt_rows_updated(), 6);
        assert_eq!(stats.receipt_rows_deleted(), 0);
    }

    #[test]
    fn stats_rollup_delete_sets_physical_count() {
        let stats = stats_from_diff(0, 0, 0, 7, HardDeletes::Delete);
        assert_eq!(stats.physical_deletes, 7);
        assert_eq!(stats.invalidated_deletes, 0);
        assert_eq!(stats.receipt_rows_deleted(), 7);
    }
}
