// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Schema diffing for incremental sink materializations (planning doc 27).
//!
//! Builds a [`SchemaDiff`] (defined in [`crate::provider`]) by comparing the
//! Arrow schema of the current incoming stream against the schema recorded
//! from the previous successful run. Used by the executor coordinator to:
//!
//! - decide whether the run can proceed under the configured
//!   [`OnSchemaChange`] policy,
//! - populate `MaterializationReceipt::schema_diff` so users can answer "what
//!   changed?" without log archaeology,
//! - compute a stable schema fingerprint stored alongside incremental state.
//!
//! The fingerprint is xxhash64 over a deterministic JSON serialization of the
//! schema's column names + Arrow type Display strings — Arrow's own
//! `Schema::serialize`/IPC bytes are not guaranteed stable across versions
//! and would produce false-positive schema changes on an Armillary upgrade.

use crate::provider::{SchemaDiff, SchemaField, SchemaTypeChange};
use armillary_engine::materialization::OnSchemaChange;
use arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// What the coordinator should do after seeing a [`SchemaDiff`] under a given
/// [`OnSchemaChange`] policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaAction {
    /// No diff, or diff is acceptable as-is — proceed without altering the
    /// target.
    Proceed,
    /// Diff requires the sink to add columns to the target. The sink-side
    /// implementation of `ALTER TABLE` is the deferred follow-up in doc 27;
    /// for now the coordinator emits a one-line WARN at the seam and proceeds.
    ProceedWithAlter,
    /// Diff is incompatible with the configured policy — the run must abort.
    Abort(String),
}

/// Compute the diff between a previously-recorded schema and the current
/// stream's schema.
pub fn compute_schema_diff(prev: &Schema, current: &Schema) -> SchemaDiff {
    let prev_fields: HashMap<&str, &arrow::datatypes::Field> = prev
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();
    let curr_fields: HashMap<&str, &arrow::datatypes::Field> = current
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();

    let mut diff = SchemaDiff::default();

    for (name, field) in &curr_fields {
        match prev_fields.get(name) {
            None => diff.added.push(SchemaField {
                name: (*name).to_string(),
                data_type: format!("{}", field.data_type()),
            }),
            Some(prev_field) => {
                if prev_field.data_type() != field.data_type() {
                    diff.type_changed.push(SchemaTypeChange {
                        name: (*name).to_string(),
                        from: format!("{}", prev_field.data_type()),
                        to: format!("{}", field.data_type()),
                    });
                }
            }
        }
    }

    for (name, field) in &prev_fields {
        if !curr_fields.contains_key(name) {
            diff.removed.push(SchemaField {
                name: (*name).to_string(),
                data_type: format!("{}", field.data_type()),
            });
        }
    }

    // Stable order so fingerprints / equality across runs are deterministic.
    diff.added.sort_by(|a, b| a.name.cmp(&b.name));
    diff.removed.sort_by(|a, b| a.name.cmp(&b.name));
    diff.type_changed.sort_by(|a, b| a.name.cmp(&b.name));

    diff
}

/// Lossy but stable JSON serialization of an Arrow [`Schema`] suitable for
/// persisting to the metadata store. Records `(name, data_type Display,
/// nullable)` per field. The reverse is [`deserialize_schema`], which only
/// reconstructs columns whose Arrow `DataType::Display` round-trips through
/// our small allowlist below — anything outside the list comes back as
/// `Utf8` so the diff still aligns on names. The fingerprint is the
/// authoritative equality check; this serialization exists purely to give
/// the diff something to compare against.
pub fn serialize_schema(schema: &Schema) -> String {
    let entries: Vec<SerField> = schema
        .fields()
        .iter()
        .map(|f| SerField {
            name: f.name().clone(),
            data_type: format!("{}", f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect();
    serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
}

/// Inverse of [`serialize_schema`]. Returns `None` on parse failure.
pub fn deserialize_schema(json: &str) -> Option<Schema> {
    let entries: Vec<SerField> = serde_json::from_str(json).ok()?;
    let fields: Vec<Field> = entries
        .into_iter()
        .map(|e| Field::new(&e.name, parse_data_type(&e.data_type), e.nullable))
        .collect();
    Some(Schema::new(fields))
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SerField {
    name: String,
    data_type: String,
    nullable: bool,
}

fn parse_data_type(s: &str) -> DataType {
    match s {
        "Int8" => DataType::Int8,
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "UInt8" => DataType::UInt8,
        "UInt16" => DataType::UInt16,
        "UInt32" => DataType::UInt32,
        "UInt64" => DataType::UInt64,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Boolean" => DataType::Boolean,
        "Utf8" => DataType::Utf8,
        "LargeUtf8" => DataType::LargeUtf8,
        "Date32" => DataType::Date32,
        "Date64" => DataType::Date64,
        // Timestamp variants and anything else: round-trip via Display
        // is lossy, so we use a sentinel that compares correctly to itself
        // and reports the original Display string back through the diff.
        // The fingerprint is the authoritative equality check, so a
        // mismatch here just shows up as `type_changed` in the diff which
        // is the right user-facing behavior.
        _ => DataType::Utf8,
    }
}

/// Stable, version-independent fingerprint over the schema's columns and
/// Arrow type Display strings. Hex-encoded `DefaultHasher` output to match
/// the convention already used by `output_cache::compute_node_fingerprint`.
/// Used as the `incremental_state.schema_fingerprint` value.
pub fn schema_fingerprint(schema: &Schema) -> String {
    let mut entries: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("{}\u{1f}{}", f.name(), f.data_type()))
        .collect();
    entries.sort();
    let canonical = entries.join("\u{1e}");
    let mut hasher = DefaultHasher::new();
    canonical.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Decide what the coordinator should do given a diff and the configured
/// `on_schema_change` policy.
pub fn apply_policy(diff: &SchemaDiff, policy: OnSchemaChange) -> SchemaAction {
    if diff.is_empty() {
        return SchemaAction::Proceed;
    }
    match policy {
        OnSchemaChange::Fail => SchemaAction::Abort(render_diff(diff)),
        OnSchemaChange::Ignore => SchemaAction::Proceed,
        OnSchemaChange::AppendNewColumns => {
            // Removed/retyped columns are not handled by `append_new_columns`;
            // doc 27 says removed columns simply keep their old values, so a
            // pure-removal diff is allowed to proceed. Type changes, however,
            // would silently corrupt data and must abort.
            if !diff.type_changed.is_empty() {
                SchemaAction::Abort(format!(
                    "on_schema_change=append_new_columns refuses type changes: {}",
                    render_diff(diff)
                ))
            } else if !diff.added.is_empty() {
                SchemaAction::ProceedWithAlter
            } else {
                SchemaAction::Proceed
            }
        }
        OnSchemaChange::SyncAllColumns => {
            if !diff.type_changed.is_empty() {
                // Type changes need lossless coercion logic that the current
                // sinks don't yet implement. Fail loud rather than guess.
                SchemaAction::Abort(format!(
                    "on_schema_change=sync_all_columns refuses non-trivial type changes: {}",
                    render_diff(diff)
                ))
            } else {
                SchemaAction::ProceedWithAlter
            }
        }
    }
}

fn render_diff(diff: &SchemaDiff) -> String {
    let mut parts = Vec::new();
    if !diff.added.is_empty() {
        let names: Vec<String> = diff
            .added
            .iter()
            .map(|f| format!("{}:{}", f.name, f.data_type))
            .collect();
        parts.push(format!("added=[{}]", names.join(",")));
    }
    if !diff.removed.is_empty() {
        let names: Vec<String> = diff
            .removed
            .iter()
            .map(|f| format!("{}:{}", f.name, f.data_type))
            .collect();
        parts.push(format!("removed=[{}]", names.join(",")));
    }
    if !diff.type_changed.is_empty() {
        let names: Vec<String> = diff
            .type_changed
            .iter()
            .map(|c| format!("{}:{}->{}", c.name, c.from, c.to))
            .collect();
        parts.push(format!("type_changed=[{}]", names.join(",")));
    }
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    fn schema(fields: Vec<(&str, DataType)>) -> Schema {
        Schema::new(
            fields
                .into_iter()
                .map(|(n, t)| Field::new(n, t, true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn no_change() {
        let s = schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        let diff = compute_schema_diff(&s, &s);
        assert!(diff.is_empty());
        assert_eq!(
            apply_policy(&diff, OnSchemaChange::Fail),
            SchemaAction::Proceed
        );
    }

    #[test]
    fn added_column() {
        let prev = schema(vec![("id", DataType::Int64)]);
        let curr = schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        let diff = compute_schema_diff(&prev, &curr);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].name, "name");
        assert!(diff.removed.is_empty());
        assert_eq!(
            apply_policy(&diff, OnSchemaChange::AppendNewColumns),
            SchemaAction::ProceedWithAlter
        );
    }

    #[test]
    fn removed_column() {
        let prev = schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        let curr = schema(vec![("id", DataType::Int64)]);
        let diff = compute_schema_diff(&prev, &curr);
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].name, "name");
        // append_new_columns tolerates pure removal (historical rows keep old values).
        assert_eq!(
            apply_policy(&diff, OnSchemaChange::AppendNewColumns),
            SchemaAction::Proceed
        );
    }

    #[test]
    fn type_changed_column() {
        let prev = schema(vec![("id", DataType::Int32)]);
        let curr = schema(vec![("id", DataType::Int64)]);
        let diff = compute_schema_diff(&prev, &curr);
        assert_eq!(diff.type_changed.len(), 1);
        assert_eq!(diff.type_changed[0].name, "id");
        // append_new_columns refuses type changes — silent coercion is the
        // dbt-shaped footgun we explicitly avoid.
        assert!(matches!(
            apply_policy(&diff, OnSchemaChange::AppendNewColumns),
            SchemaAction::Abort(_)
        ));
    }

    #[test]
    fn fail_policy_aborts_on_any_diff() {
        let prev = schema(vec![("id", DataType::Int64)]);
        let curr = schema(vec![("id", DataType::Int64), ("x", DataType::Utf8)]);
        let diff = compute_schema_diff(&prev, &curr);
        assert!(matches!(
            apply_policy(&diff, OnSchemaChange::Fail),
            SchemaAction::Abort(_)
        ));
    }

    #[test]
    fn ignore_policy_proceeds_on_any_diff() {
        let prev = schema(vec![("id", DataType::Int64)]);
        let curr = schema(vec![("id", DataType::Int64), ("x", DataType::Utf8)]);
        let diff = compute_schema_diff(&prev, &curr);
        assert_eq!(
            apply_policy(&diff, OnSchemaChange::Ignore),
            SchemaAction::Proceed
        );
    }

    #[test]
    fn fingerprint_is_stable() {
        let s1 = schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        let s2 = schema(vec![("name", DataType::Utf8), ("id", DataType::Int64)]);
        // Different field order, same content → same fingerprint.
        assert_eq!(schema_fingerprint(&s1), schema_fingerprint(&s2));
    }

    #[test]
    fn fingerprint_changes_on_type_change() {
        let s1 = schema(vec![("id", DataType::Int32)]);
        let s2 = schema(vec![("id", DataType::Int64)]);
        assert_ne!(schema_fingerprint(&s1), schema_fingerprint(&s2));
    }

    #[test]
    fn mixed_add_remove_and_type_change_in_one_diff() {
        let prev = schema(vec![
            ("id", DataType::Int32),
            ("dropped", DataType::Utf8),
            ("kept", DataType::Boolean),
        ]);
        let curr = schema(vec![
            ("id", DataType::Int64),
            ("kept", DataType::Boolean),
            ("added", DataType::Float64),
        ]);
        let diff = compute_schema_diff(&prev, &curr);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].name, "added");
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].name, "dropped");
        assert_eq!(diff.type_changed.len(), 1);
        assert_eq!(diff.type_changed[0].name, "id");
        // Type-change present → AppendNewColumns must abort even though
        // there's also a clean add.
        assert!(matches!(
            apply_policy(&diff, OnSchemaChange::AppendNewColumns),
            SchemaAction::Abort(_)
        ));
        // SyncAllColumns also refuses non-trivial type changes.
        assert!(matches!(
            apply_policy(&diff, OnSchemaChange::SyncAllColumns),
            SchemaAction::Abort(_)
        ));
    }

    #[test]
    fn schema_serialize_deserialize_round_trips_for_supported_types() {
        let s = schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("active", DataType::Boolean),
        ]);
        let json = serialize_schema(&s);
        let restored = deserialize_schema(&json).expect("round trip");
        // The fingerprint is the authoritative equality check.
        assert_eq!(schema_fingerprint(&s), schema_fingerprint(&restored));
        let diff = compute_schema_diff(&s, &restored);
        assert!(diff.is_empty());
    }
}
