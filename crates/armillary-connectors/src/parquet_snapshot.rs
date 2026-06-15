// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Parquet sink implementation of `WriteStrategy::Snapshot` (SCD2).
//!
//! This is a full read-modify-write rewrite: each run reads the existing
//! target Parquet file (if any), classifies every business key against the
//! incoming staged batch using the pure planner from
//! [`armillary_engine::snapshot`], and writes a new Parquet file containing the
//! union of (historical rows) ∪ (closed-current versions) ∪ (still-current
//! Unchanged rows) ∪ (newly-opened versions). The new file is written to a
//! sibling `.tmp` path and atomically renamed over the target.
//!
//! Cloud paths are rejected here — read-modify-write across object stores
//! has no atomic-rename primitive and no easy locking, so a v1 implementation
//! cannot promise correct snapshot semantics under concurrent writers.
//! Larger-than-memory tables are not handled either; doc 28's "chunked
//! rewrite via DataFusion" path is deferred. Both limitations are documented
//! in the deferred-tasks section of `planning/28-snapshots-scd2.md`.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use armillary_datafusion::provider::{
    MaterializationContext, MaterializationReceipt, ProviderError, WriteStats,
};
use armillary_engine::materialization::{
    ChangeDetection, HardDeletes, MaterializationPolicy, SnapshotPolicy,
};
use armillary_engine::snapshot::{
    CurrentTargetRow, FLUX_IS_CURRENT, FLUX_SCD_ID, FLUX_VALID_FROM, FLUX_VALID_TO,
    RowClassification, SnapshotMergeStats, StagedRow, check_hash, plan_snapshot_merge,
    surrogate_key,
};
use arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::cloud_store;
use crate::config::FileConfig;

const SCD_META_NAMES: [&str; 4] = [FLUX_SCD_ID, FLUX_VALID_FROM, FLUX_VALID_TO, FLUX_IS_CURRENT];

/// Top-level entry point. Called from `file_sink::write` when
/// `format == Parquet && write_strategy == Snapshot`.
pub(crate) async fn write_parquet_snapshot(
    file_config: &FileConfig,
    data: Vec<RecordBatch>,
    ctx: &MaterializationContext,
    materialization: Option<&MaterializationPolicy>,
    start: Instant,
) -> Result<MaterializationReceipt, ProviderError> {
    // ---- 1. Resolve policy + path ----
    let policy =
        materialization.ok_or("parquet snapshot strategy requires a `materialization` block")?;
    let snapshot = policy
        .snapshot
        .as_ref()
        .ok_or("parquet snapshot strategy requires a `snapshot:` block")?;
    let unique_keys: Vec<String> = policy.unique_keys.clone().unwrap_or_default();
    if unique_keys.is_empty() {
        return Err("parquet snapshot strategy requires non-empty `unique_keys`".into());
    }

    let path_str = file_config
        .path
        .to_str()
        .ok_or_else(|| format!("path is not valid UTF-8: {}", file_config.path.display()))?;
    if cloud_store::is_cloud_url(path_str) {
        return Err(
            "parquet snapshot strategy is not supported on cloud object stores in v1 \
             (no atomic rename / locking primitive); use a local path or a database sink"
                .into(),
        );
    }

    let path: PathBuf = if file_config.path.is_relative() {
        std::env::current_dir()
            .map_err(|e| format!("failed to get current directory: {e}"))?
            .join(&file_config.path)
    } else {
        file_config.path.clone()
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create directory '{}': {e}", parent.display()))?;
    }

    // ---- 2. Determine current run timestamp (microseconds since epoch) ----
    let now_micros: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system clock before unix epoch: {e}"))?
        .as_micros() as i64;

    // ---- 3. Combine staged batches into a single RecordBatch ----
    let staged_schema = if data.is_empty() {
        // No incoming rows: still need to honor `hard_deletes` against the
        // existing target. We need a schema; if a target file already
        // exists we'll lift the business schema from it. If neither exists,
        // there is literally nothing to do.
        if !path.exists() {
            let stats = WriteStats {
                rows_written: 0,
                bytes_written: 0,
                duration: start.elapsed(),
            };
            return Ok(MaterializationReceipt::from_write_stats(&stats, ctx));
        }
        Arc::new(business_schema_from_target(&path)?)
    } else {
        let s = data[0].schema();
        // Strip any pre-existing scd metadata cols if a misconfigured caller
        // hands us batches with them already attached.
        Arc::new(strip_metadata_fields(&s))
    };

    let staged_batch = if data.is_empty() {
        RecordBatch::new_empty(Arc::clone(&staged_schema))
    } else {
        let stripped: Vec<RecordBatch> = data
            .iter()
            .map(|b| project_business_columns(b, &staged_schema))
            .collect::<Result<_, _>>()?;
        concat_batches(&staged_schema, &stripped)
            .map_err(|e| format!("failed to concatenate staged batches: {e}"))?
    };

    validate_columns_present(&staged_schema, &unique_keys, "unique_keys")?;
    if let ChangeDetection::Check = snapshot.change_detection {
        let cols = snapshot
            .check_columns
            .as_ref()
            .ok_or("snapshot.check_columns is required for change_detection: check")?;
        validate_columns_present(&staged_schema, cols, "snapshot.check_columns")?;
    }
    if let ChangeDetection::Timestamp = snapshot.change_detection {
        let col = snapshot
            .updated_at_column
            .as_ref()
            .ok_or("snapshot.updated_at_column is required for change_detection: timestamp")?;
        validate_columns_present(
            &staged_schema,
            std::slice::from_ref(col),
            "snapshot.updated_at_column",
        )?;
    }

    // ---- 4. Read existing target (if any) and split into current vs historical ----
    let existing = if path.exists() {
        Some(read_existing_target(&path, &staged_schema)?)
    } else {
        None
    };

    let (current_batch, historical_batch) = match &existing {
        None => (None, None),
        Some(target) => {
            let is_current_idx = target.schema().index_of(FLUX_IS_CURRENT).map_err(|_| {
                format!(
                    "existing target '{}' has no `{FLUX_IS_CURRENT}` column; \
                         it does not look like a armillary snapshot table",
                    path.display()
                )
            })?;
            let is_current = target
                .column(is_current_idx)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| format!("`{FLUX_IS_CURRENT}` column is not boolean"))?
                .clone();

            let mut cur_idx: Vec<u32> = Vec::new();
            let mut hist_idx: Vec<u32> = Vec::new();
            for i in 0..target.num_rows() {
                if is_current.is_valid(i) && is_current.value(i) {
                    cur_idx.push(i as u32);
                } else {
                    hist_idx.push(i as u32);
                }
            }
            (
                Some(take_rows(target, &cur_idx)?),
                Some(take_rows(target, &hist_idx)?),
            )
        }
    };

    // ---- 5. Build StagedRow / CurrentTargetRow lists for the planner ----
    let staged_rows = build_signal_rows(&staged_batch, &unique_keys, snapshot)?
        .into_iter()
        .map(|(key, signal)| StagedRow {
            unique_key: key,
            signal,
        })
        .collect::<Vec<_>>();

    let current_rows: Vec<CurrentTargetRow> = match &current_batch {
        Some(b) => build_signal_rows(b, &unique_keys, snapshot)?
            .into_iter()
            .map(|(key, signal)| CurrentTargetRow {
                unique_key: key,
                signal,
            })
            .collect(),
        None => Vec::new(),
    };

    let plan = plan_snapshot_merge(
        current_rows,
        staged_rows.clone(),
        snapshot.change_detection,
        snapshot.hard_deletes,
    );

    // ---- 6. Index classifications by key for batch construction ----
    use std::collections::HashMap;
    let mut class_by_key: HashMap<Vec<String>, RowClassification> = HashMap::new();
    for row in &plan.rows {
        class_by_key.insert(row.unique_key.clone(), row.classification);
    }

    // ---- 7. Build target schema (business cols + 4 metadata cols) ----
    let target_schema = Arc::new(target_schema_with_metadata(&staged_schema));

    // ---- 8. Compose output batches ----
    let mut output_batches: Vec<RecordBatch> = Vec::new();

    // 8a. Historical rows: keep all, except (Hard-Delete + key in plan-Gone-Delete) drop ALL versions of that key.
    if let Some(hist) = &historical_batch
        && hist.num_rows() > 0
    {
        let keep = if matches!(snapshot.hard_deletes, HardDeletes::Delete) {
            // Drop every historical version of any key whose current version was Gone.
            let gone_keys: std::collections::HashSet<Vec<String>> = plan
                .rows
                .iter()
                .filter(|r| r.classification == RowClassification::Gone)
                .map(|r| r.unique_key.clone())
                .collect();
            let key_strings = stringify_keys(hist, &unique_keys)?;
            let mut idx: Vec<u32> = Vec::new();
            for (i, k) in key_strings.iter().enumerate() {
                if !gone_keys.contains(k) {
                    idx.push(i as u32);
                }
            }
            take_rows(hist, &idx)?
        } else {
            hist.clone()
        };
        if keep.num_rows() > 0 {
            output_batches.push(reorder_to_schema(&keep, &target_schema)?);
        }
    }

    // 8b. Current rows: split by classification.
    if let Some(cur) = &current_batch
        && cur.num_rows() > 0
    {
        let key_strings = stringify_keys(cur, &unique_keys)?;
        let mut keep_unchanged: Vec<u32> = Vec::new();
        let mut close_changed: Vec<u32> = Vec::new();
        let mut close_gone_invalidate: Vec<u32> = Vec::new();
        // Gone+Ignore => keep_unchanged. Gone+Delete => skip entirely.
        for (i, key) in key_strings.iter().enumerate() {
            match class_by_key.get(key) {
                Some(RowClassification::Unchanged) => keep_unchanged.push(i as u32),
                Some(RowClassification::Changed) => close_changed.push(i as u32),
                Some(RowClassification::Gone) => match snapshot.hard_deletes {
                    HardDeletes::Ignore => keep_unchanged.push(i as u32),
                    HardDeletes::Invalidate => close_gone_invalidate.push(i as u32),
                    HardDeletes::Delete => {} // drop
                },
                Some(RowClassification::New) | None => {
                    // Defensive: a current-target row whose key didn't show
                    // up in the plan shouldn't happen. Treat as unchanged.
                    keep_unchanged.push(i as u32);
                }
            }
        }

        if !keep_unchanged.is_empty() {
            let b = take_rows(cur, &keep_unchanged)?;
            output_batches.push(reorder_to_schema(&b, &target_schema)?);
        }
        let mut to_close: Vec<u32> = close_changed;
        to_close.extend(close_gone_invalidate);
        if !to_close.is_empty() {
            let b = take_rows(cur, &to_close)?;
            let closed = override_close_metadata(&b, now_micros, &target_schema)?;
            output_batches.push(closed);
        }
    }

    // 8c. New + Changed staged rows: emit as new versions.
    let new_version_idx: Vec<u32> = staged_rows
        .iter()
        .enumerate()
        .filter_map(|(i, sr)| match class_by_key.get(&sr.unique_key) {
            Some(RowClassification::New) | Some(RowClassification::Changed) => Some(i as u32),
            _ => None,
        })
        .collect();
    if !new_version_idx.is_empty() {
        let b = take_rows(&staged_batch, &new_version_idx)?;
        let opened = build_new_versions(&b, &unique_keys, now_micros, &target_schema)?;
        output_batches.push(opened);
    }

    // ---- 9. Write atomically to <path>.tmp then rename ----
    let tmp_path = path.with_extension(match path.extension() {
        Some(ext) => format!("{}.tmp", ext.to_string_lossy()),
        None => "tmp".to_string(),
    });
    let bytes_written = write_parquet_atomic(
        &tmp_path,
        &path,
        &target_schema,
        &output_batches,
        file_config,
    )?;

    // ---- 10. Build receipt ----
    let total_rows: u64 = output_batches.iter().map(|b| b.num_rows() as u64).sum();
    let stats = WriteStats {
        rows_written: total_rows,
        bytes_written,
        duration: start.elapsed(),
    };
    let mut receipt = MaterializationReceipt::from_write_stats(&stats, ctx);
    receipt.rows_inserted = plan.stats.receipt_rows_inserted();
    receipt.rows_updated = plan.stats.receipt_rows_updated();
    receipt.rows_deleted = plan.stats.receipt_rows_deleted();
    let _: SnapshotMergeStats = plan.stats; // reaffirm we used it
    Ok(receipt)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn validate_columns_present(
    schema: &Schema,
    cols: &[String],
    label: &str,
) -> Result<(), ProviderError> {
    for c in cols {
        if schema.index_of(c).is_err() {
            return Err(format!(
                "snapshot {label} references column '{c}' which is not in the staged schema"
            )
            .into());
        }
    }
    Ok(())
}

fn strip_metadata_fields(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .filter(|f| !SCD_META_NAMES.contains(&f.name().as_str()))
        .cloned()
        .collect::<Vec<_>>();
    Schema::new(fields)
}

/// Project an incoming batch down to the staged business schema (drops any
/// metadata columns the caller might have included).
fn project_business_columns(
    batch: &RecordBatch,
    target: &Schema,
) -> Result<RecordBatch, ProviderError> {
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for f in target.fields() {
        let idx = batch
            .schema()
            .index_of(f.name())
            .map_err(|_| format!("staged batch missing column '{}'", f.name()))?;
        cols.push(batch.column(idx).clone());
    }
    RecordBatch::try_new(Arc::new(target.clone()), cols)
        .map_err(|e| format!("failed to project staged batch: {e}").into())
}

fn business_schema_from_target(path: &Path) -> Result<Schema, ProviderError> {
    let file = fs::File::open(path)
        .map_err(|e| format!("failed to open existing target '{}': {e}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("failed to read parquet metadata: {e}"))?;
    let schema = builder.schema().clone();
    Ok(strip_metadata_fields(&schema))
}

fn read_existing_target(
    path: &Path,
    expected_business: &Schema,
) -> Result<RecordBatch, ProviderError> {
    let file = fs::File::open(path)
        .map_err(|e| format!("failed to open existing target '{}': {e}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("failed to read parquet metadata: {e}"))?;
    let target_schema = builder.schema().clone();
    // Sanity check that all expected business columns exist on the target.
    for f in expected_business.fields() {
        if target_schema.index_of(f.name()).is_err() {
            return Err(format!(
                "existing snapshot target '{}' missing business column '{}' present \
                 in incoming staged data",
                path.display(),
                f.name()
            )
            .into());
        }
    }
    let reader = builder
        .build()
        .map_err(|e| format!("failed to build parquet reader: {e}"))?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for b in reader {
        batches.push(b.map_err(|e| format!("failed to read parquet batch: {e}"))?);
    }
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(target_schema));
    }
    concat_batches(&target_schema, &batches)
        .map_err(|e| format!("failed to concat target batches: {e}").into())
}

fn take_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch, ProviderError> {
    let idx_array = UInt32Array::from(indices.to_vec());
    let cols: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c, &idx_array, None))
        .collect::<Result<_, _>>()
        .map_err(|e| format!("take failed: {e}"))?;
    RecordBatch::try_new(batch.schema(), cols)
        .map_err(|e| format!("failed to assemble taken batch: {e}").into())
}

/// Stringify each row's unique-key columns into a `Vec<String>` per row.
/// `null` is encoded as the literal string `"\0NULL"` so it cannot collide
/// with a user value.
fn stringify_keys(
    batch: &RecordBatch,
    unique_keys: &[String],
) -> Result<Vec<Vec<String>>, ProviderError> {
    let opts = FormatOptions::default().with_null("\0NULL");
    let mut formatters: Vec<ArrayFormatter> = Vec::with_capacity(unique_keys.len());
    for k in unique_keys {
        let idx = batch
            .schema()
            .index_of(k)
            .map_err(|_| format!("unique key '{k}' missing from batch schema"))?;
        formatters.push(
            ArrayFormatter::try_new(batch.column(idx).as_ref(), &opts)
                .map_err(|e| format!("cannot format key column '{k}': {e}"))?,
        );
    }
    let mut out: Vec<Vec<String>> = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let mut row: Vec<String> = Vec::with_capacity(unique_keys.len());
        for f in &formatters {
            row.push(f.value(i).to_string());
        }
        out.push(row);
    }
    Ok(out)
}

/// Build (key, signal) pairs for every row in `batch`, where `signal` is the
/// change-detection comparison value defined by [`armillary_engine::snapshot`].
fn build_signal_rows(
    batch: &RecordBatch,
    unique_keys: &[String],
    snapshot: &SnapshotPolicy,
) -> Result<Vec<(Vec<String>, i64)>, ProviderError> {
    let keys = stringify_keys(batch, unique_keys)?;
    let signals: Vec<i64> = match snapshot.change_detection {
        ChangeDetection::Check => {
            let cols = snapshot
                .check_columns
                .as_ref()
                .ok_or("snapshot.check_columns is required for change_detection: check")?;
            let opts = FormatOptions::default().with_null("\0NULL");
            let mut formatters: Vec<ArrayFormatter> = Vec::with_capacity(cols.len());
            for c in cols {
                let idx = batch
                    .schema()
                    .index_of(c)
                    .map_err(|_| format!("check column '{c}' missing from batch schema"))?;
                formatters.push(
                    ArrayFormatter::try_new(batch.column(idx).as_ref(), &opts)
                        .map_err(|e| format!("cannot format check column '{c}': {e}"))?,
                );
            }
            (0..batch.num_rows())
                .map(|i| {
                    let strs: Vec<String> =
                        formatters.iter().map(|f| f.value(i).to_string()).collect();
                    let refs: Vec<Option<&str>> = strs.iter().map(|s| Some(s.as_str())).collect();
                    check_hash(&refs) as i64
                })
                .collect()
        }
        ChangeDetection::Timestamp => {
            let col = snapshot
                .updated_at_column
                .as_ref()
                .ok_or("snapshot.updated_at_column required for change_detection: timestamp")?;
            let idx = batch
                .schema()
                .index_of(col)
                .map_err(|_| format!("updated_at column '{col}' missing from batch schema"))?;
            let array = batch.column(idx);
            timestamp_signals(array.as_ref())?
        }
    };
    Ok(keys.into_iter().zip(signals).collect())
}

fn timestamp_signals(array: &dyn Array) -> Result<Vec<i64>, ProviderError> {
    use arrow::array::{Int64Array, TimestampNanosecondArray};
    if let Some(a) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok((0..a.len())
            .map(|i| if a.is_null(i) { i64::MIN } else { a.value(i) })
            .collect());
    }
    if let Some(a) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok((0..a.len())
            .map(|i| {
                if a.is_null(i) {
                    i64::MIN
                } else {
                    a.value(i) / 1_000
                }
            })
            .collect());
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok((0..a.len())
            .map(|i| if a.is_null(i) { i64::MIN } else { a.value(i) })
            .collect());
    }
    Err(format!(
        "snapshot updated_at column must be Timestamp(Micro|Nano) or Int64, got {:?}",
        array.data_type()
    )
    .into())
}

fn target_schema_with_metadata(business: &Schema) -> Schema {
    let mut fields: Vec<Field> = business
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(FLUX_SCD_ID, DataType::Utf8, false));
    fields.push(Field::new(
        FLUX_VALID_FROM,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    ));
    fields.push(Field::new(
        FLUX_VALID_TO,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    ));
    fields.push(Field::new(FLUX_IS_CURRENT, DataType::Boolean, false));
    Schema::new(fields)
}

/// Reorder/project a batch's columns to match `target` (column-name-based),
/// erroring if any required column is missing. Used to coerce historical and
/// current target slices into the canonical target schema before concat.
fn reorder_to_schema(
    batch: &RecordBatch,
    target: &Arc<Schema>,
) -> Result<RecordBatch, ProviderError> {
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for f in target.fields() {
        let idx = batch.schema().index_of(f.name()).map_err(|_| {
            format!(
                "batch missing column '{}' needed for target schema",
                f.name()
            )
        })?;
        cols.push(batch.column(idx).clone());
    }
    RecordBatch::try_new(Arc::clone(target), cols)
        .map_err(|e| format!("failed to reorder batch to target schema: {e}").into())
}

/// Take a slice of current target rows and overwrite their `armillary_valid_to`
/// and `armillary_is_current` to mark them closed at `now_micros`. Other columns
/// (including `armillary_scd_id` and the original `armillary_valid_from`) are preserved.
fn override_close_metadata(
    batch: &RecordBatch,
    now_micros: i64,
    target_schema: &Arc<Schema>,
) -> Result<RecordBatch, ProviderError> {
    let n = batch.num_rows();
    let valid_to = TimestampMicrosecondArray::from(vec![Some(now_micros); n]).with_timezone("UTC");
    let is_current = BooleanArray::from(vec![false; n]);

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());
    for f in target_schema.fields() {
        let name = f.name().as_str();
        if name == FLUX_VALID_TO {
            cols.push(Arc::new(valid_to.clone()) as ArrayRef);
        } else if name == FLUX_IS_CURRENT {
            cols.push(Arc::new(is_current.clone()) as ArrayRef);
        } else {
            let idx = batch.schema().index_of(name).map_err(|_| {
                format!("current-target slice missing column '{name}' for close-metadata override")
            })?;
            cols.push(batch.column(idx).clone());
        }
    }
    RecordBatch::try_new(Arc::clone(target_schema), cols)
        .map_err(|e| format!("failed to assemble closed-version batch: {e}").into())
}

/// Build new-version rows from a slice of staged business rows, attaching
/// freshly-generated metadata columns.
fn build_new_versions(
    staged: &RecordBatch,
    unique_keys: &[String],
    now_micros: i64,
    target_schema: &Arc<Schema>,
) -> Result<RecordBatch, ProviderError> {
    let n = staged.num_rows();

    let key_strings = stringify_keys(staged, unique_keys)?;
    let scd_ids: Vec<String> = key_strings
        .iter()
        .map(|k| {
            let refs: Vec<Option<&str>> = k.iter().map(|s| Some(s.as_str())).collect();
            surrogate_key(&refs, now_micros)
        })
        .collect();

    let scd_id = StringArray::from(scd_ids);
    let valid_from =
        TimestampMicrosecondArray::from(vec![Some(now_micros); n]).with_timezone("UTC");
    let valid_to: TimestampMicrosecondArray =
        TimestampMicrosecondArray::from(vec![None as Option<i64>; n]).with_timezone("UTC");
    let is_current = BooleanArray::from(vec![true; n]);

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());
    for f in target_schema.fields() {
        let name = f.name().as_str();
        if name == FLUX_SCD_ID {
            cols.push(Arc::new(scd_id.clone()) as ArrayRef);
        } else if name == FLUX_VALID_FROM {
            cols.push(Arc::new(valid_from.clone()) as ArrayRef);
        } else if name == FLUX_VALID_TO {
            cols.push(Arc::new(valid_to.clone()) as ArrayRef);
        } else if name == FLUX_IS_CURRENT {
            cols.push(Arc::new(is_current.clone()) as ArrayRef);
        } else {
            let idx = staged
                .schema()
                .index_of(name)
                .map_err(|_| format!("staged batch missing business column '{name}'"))?;
            cols.push(staged.column(idx).clone());
        }
    }
    RecordBatch::try_new(Arc::clone(target_schema), cols)
        .map_err(|e| format!("failed to assemble new-version batch: {e}").into())
}

fn write_parquet_atomic(
    tmp_path: &Path,
    final_path: &Path,
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    file_config: &FileConfig,
) -> Result<u64, ProviderError> {
    let compression = match &file_config.options.compression {
        Some(codec) => crate::file_sink::parse_compression(codec)?,
        None => Compression::SNAPPY,
    };
    let mut props_builder = WriterProperties::builder().set_compression(compression);
    if let Some(rg_size) = file_config.options.row_group_size {
        props_builder = props_builder.set_max_row_group_row_count(Some(rg_size));
    }
    let props = props_builder.build();

    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create directory '{}': {e}", parent.display()))?;
    }
    let file = fs::File::create(tmp_path)
        .map_err(|e| format!("failed to create '{}': {e}", tmp_path.display()))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(schema), Some(props))
        .map_err(|e| format!("failed to create parquet writer: {e}"))?;
    for b in batches {
        writer
            .write(b)
            .map_err(|e| format!("failed to write parquet batch: {e}"))?;
    }
    writer
        .close()
        .map_err(|e| format!("failed to close parquet writer: {e}"))?;

    fs::rename(tmp_path, final_path).map_err(|e| {
        format!(
            "failed to atomically rename '{}' -> '{}': {e}",
            tmp_path.display(),
            final_path.display()
        )
    })?;

    let bytes = fs::metadata(final_path).map(|m| m.len()).unwrap_or(0);
    Ok(bytes)
}
