// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! File-based sink connector (CSV and Parquet).
//!
//! Writes Arrow record batches to local files or cloud object stores in CSV or
//! Parquet format. Supports configurable options for each format and
//! overwrite/append modes.
//!
//! For cloud URLs (`s3://`, `gs://`, `az://`), data is written to an in-memory
//! buffer and uploaded via [`ObjectStore::put`]. The `object_store` crate
//! handles multipart upload for large payloads automatically.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufWriter, Cursor, Write};
use std::time::Instant;

use armillary_datafusion::provider::{
    MaterializationContext, MaterializationReceipt, PipelineSink, ProviderError, WriteOptions,
    WriteStats,
};
use armillary_engine::materialization::WriteStrategy;
use armillary_engine::node::SinkConfig;
use arrow::array::{Array, UInt32Array};
use arrow::compute::take;
use arrow::csv::WriterBuilder as CsvWriterBuilder;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use async_trait::async_trait;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::cloud_store;
use crate::config::{FileConfig, FileFormat, WriteMode};

/// Sink connector for CSV and Parquet files (local and cloud).
///
/// Supports:
/// - CSV with configurable delimiter, header, quote char
/// - Parquet with configurable compression (snappy, zstd, gzip, none) and row group size
/// - Overwrite or append mode (append only for local CSV)
/// - Cloud URLs: `s3://`, `gs://`, `az://`
/// - Automatic parent directory creation (local paths)
/// - Multipart upload for large cloud files (handled by `object_store`)
pub struct FileSink;

impl FileSink {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FileSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PipelineSink for FileSink {
    async fn write(
        &self,
        config: &SinkConfig,
        data: Vec<RecordBatch>,
        _options: &WriteOptions,
        ctx: &MaterializationContext,
    ) -> Result<MaterializationReceipt, ProviderError> {
        let start = Instant::now();

        let mut file_config: FileConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid file sink config: {e}"))?;

        // Resolve the materialization write strategy against this sink's
        // capabilities. Strategies that the file sink can't honor (merge,
        // delete_insert, insert_overwrite) are rejected here with a clear
        // error rather than silently downgraded. `TruncateInsert` is
        // honored by forcing the underlying file write to overwrite mode,
        // overriding any `FileConfig.options.write_mode` the user set.
        check_strategy_supported(&file_config.format, ctx.write_strategy)?;
        if matches!(ctx.write_strategy, WriteStrategy::TruncateInsert) {
            file_config.options.write_mode = Some(WriteMode::Overwrite);
        }

        // InsertOverwrite (Parquet only — gated by check_strategy_supported)
        // takes a separate hive-partitioned write path that touches only the
        // partitions present in the incoming data.
        if matches!(ctx.write_strategy, WriteStrategy::InsertOverwrite) {
            return write_parquet_insert_overwrite(&file_config, data, ctx, start).await;
        }

        // Snapshot (Parquet only — gated by check_strategy_supported) takes a
        // full read-modify-write SCD2 path. See `parquet_snapshot.rs`.
        if matches!(ctx.write_strategy, WriteStrategy::Snapshot) {
            return crate::parquet_snapshot::write_parquet_snapshot(
                &file_config,
                data,
                ctx,
                config.materialization.as_ref(),
                start,
            )
            .await;
        }

        if data.is_empty() {
            let stats = WriteStats {
                rows_written: 0,
                bytes_written: 0,
                duration: start.elapsed(),
            };
            return Ok(MaterializationReceipt::from_write_stats(&stats, ctx));
        }

        let path_str = file_config
            .path
            .to_str()
            .ok_or_else(|| format!("path is not valid UTF-8: {}", file_config.path.display()))?;

        if cloud_store::is_cloud_url(path_str) {
            let (rows_written, bytes_written) = write_cloud(path_str, &data, &file_config).await?;
            let stats = WriteStats {
                rows_written,
                bytes_written,
                duration: start.elapsed(),
            };
            Ok(MaterializationReceipt::from_write_stats(&stats, ctx))
        } else {
            // Local path — existing behavior.
            let path = if file_config.path.is_relative() {
                std::env::current_dir()
                    .map_err(|e| format!("failed to get current directory: {e}"))?
                    .join(&file_config.path)
            } else {
                file_config.path.clone()
            };

            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    format!("failed to create directory '{}': {e}", parent.display())
                })?;
            }

            debug!(path = %path.display(), format = ?file_config.format, "writing local file sink");

            let (rows_written, bytes_written) = match file_config.format {
                FileFormat::Csv => write_csv(&path, &data, &file_config)?,
                FileFormat::Parquet => write_parquet(&path, &data, &file_config)?,
            };

            let stats = WriteStats {
                rows_written,
                bytes_written,
                duration: start.elapsed(),
            };
            Ok(MaterializationReceipt::from_write_stats(&stats, ctx))
        }
    }

    fn validate_config(&self, config: &SinkConfig) -> Result<(), ProviderError> {
        let file_config: FileConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid file sink config: {e}"))?;

        // Reject materialization strategies the file sink can't implement.
        // Validation runs once at pipeline import time, so users see the
        // error before any data is written.
        if let Some(policy) = &config.materialization {
            check_strategy_supported(&file_config.format, policy.write_strategy)?;
        }

        // Path must not be empty.
        if file_config.path.as_os_str().is_empty() {
            return Err("file sink path must not be empty".into());
        }

        let path_str = file_config.path.to_string_lossy();

        // Glob patterns are not valid for sink paths.
        if path_str.contains('*') || path_str.contains('?') || path_str.contains('[') {
            return Err("file sink path must not contain glob patterns".into());
        }

        // Cloud sinks do not support append mode.
        if cloud_store::is_cloud_url(&path_str)
            && matches!(file_config.options.write_mode, Some(WriteMode::Append))
        {
            return Err(
                "append mode is not supported for cloud storage sinks; use overwrite".into(),
            );
        }

        // Validate compression value if specified for Parquet.
        if let FileFormat::Parquet = file_config.format {
            if let Some(ref codec) = file_config.options.compression {
                parse_compression(codec)?;
            }
        }

        Ok(())
    }
}

/// Check whether a [`WriteStrategy`] is supported by the file sink for the
/// given file [`FileFormat`]. CSV supports only `Append`; Parquet additionally
/// supports `TruncateInsert` (mapped to overwrite). Every other strategy
/// returns an error explaining the limitation and pointing at the right
/// alternative.
///
/// `InsertOverwrite` (partition-replace) is intentionally still rejected here
/// — it's tracked as a follow-up in `planning/27-incremental-materializations.md`
/// because it requires partition-column infrastructure on `FileConfig`.
fn check_strategy_supported(
    format: &FileFormat,
    strategy: WriteStrategy,
) -> Result<(), ProviderError> {
    match (format, strategy) {
        (_, WriteStrategy::Append) => Ok(()),
        (FileFormat::Parquet, WriteStrategy::TruncateInsert) => Ok(()),
        (FileFormat::Csv, WriteStrategy::TruncateInsert) => Err(
            "csv file sink does not support write_strategy=truncate_insert; use append \
             (the file is rewritten on each run anyway unless write_mode=append is set)"
                .into(),
        ),
        (_, WriteStrategy::Merge) => Err(
            "file sinks do not support write_strategy=merge; use a database sink \
             (postgres) for upsert semantics"
                .into(),
        ),
        (_, WriteStrategy::DeleteInsert) => Err(
            "file sinks do not support write_strategy=delete_insert; use a database sink \
             (postgres) for delete+insert semantics"
                .into(),
        ),
        (FileFormat::Parquet, WriteStrategy::InsertOverwrite) => Ok(()),
        (FileFormat::Csv, WriteStrategy::InsertOverwrite) => Err(
            "csv file sink does not support write_strategy=insert_overwrite; \
             use parquet for partitioned hive-style outputs"
                .into(),
        ),
        (FileFormat::Parquet, WriteStrategy::Snapshot) => Ok(()),
        (FileFormat::Csv, WriteStrategy::Snapshot) => Err(
            "csv file sink cannot represent SCD2 history (no schema for \
             armillary_valid_from/armillary_valid_to/armillary_is_current/armillary_scd_id metadata); \
             use parquet or a database sink (postgres/duckdb) for snapshot \
             write_strategy"
                .into(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Cloud write
// ---------------------------------------------------------------------------

/// Write record batches to a cloud object store.
///
/// Serializes data to an in-memory buffer, then uploads via `object_store::put`.
async fn write_cloud(
    path_str: &str,
    data: &[RecordBatch],
    file_config: &FileConfig,
) -> Result<(u64, u64), ProviderError> {
    if matches!(file_config.options.write_mode, Some(WriteMode::Append)) {
        return Err("append mode is not supported for cloud storage sinks; use overwrite".into());
    }

    let cloud_url = cloud_store::parse_cloud_url(path_str)?;
    let store = cloud_store::build_object_store(&cloud_url, &file_config.storage_options)?;

    debug!(
        path = %path_str,
        format = ?file_config.format,
        "writing cloud file sink"
    );

    let bytes = match file_config.format {
        FileFormat::Csv => write_csv_to_bytes(data, file_config)?,
        FileFormat::Parquet => write_parquet_to_bytes(data, file_config)?,
    };

    let object_path = ObjectPath::parse(&cloud_url.object_path)
        .map_err(|e| format!("invalid object path '{}': {e}", cloud_url.object_path))?;

    let bytes_len = bytes.len() as u64;
    let payload = PutPayload::from(bytes);
    store
        .put(&object_path, payload)
        .await
        .map_err(|e| format!("failed to upload to '{}': {e}", path_str))?;

    let rows_written: u64 = data.iter().map(|b| b.num_rows() as u64).sum();
    Ok((rows_written, bytes_len))
}

/// Serialize record batches to CSV bytes in memory.
fn write_csv_to_bytes(data: &[RecordBatch], config: &FileConfig) -> Result<Vec<u8>, ProviderError> {
    let has_header = config.options.has_header.unwrap_or(true);
    let mut buf = Vec::new();

    for (i, batch) in data.iter().enumerate() {
        let mut builder = CsvWriterBuilder::new();

        if let Some(delim) = config.options.delimiter {
            builder = builder.with_delimiter(delim as u8);
        }
        if let Some(quote) = config.options.quote_char {
            builder = builder.with_quote(quote as u8);
        }

        let write_header = has_header && i == 0;
        builder = builder.with_header(write_header);

        let mut writer = builder.build(&mut buf);
        writer
            .write(batch)
            .map_err(|e| format!("failed to write CSV batch: {e}"))?;
    }

    Ok(buf)
}

/// Serialize record batches to Parquet bytes in memory.
fn write_parquet_to_bytes(
    data: &[RecordBatch],
    config: &FileConfig,
) -> Result<Vec<u8>, ProviderError> {
    let schema = data[0].schema();

    let compression = match &config.options.compression {
        Some(codec) => parse_compression(codec)?,
        None => Compression::SNAPPY,
    };

    let mut props_builder = WriterProperties::builder().set_compression(compression);
    if let Some(rg_size) = config.options.row_group_size {
        props_builder = props_builder.set_max_row_group_row_count(Some(rg_size));
    }
    let props = props_builder.build();

    let mut buf = Cursor::new(Vec::new());
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))
        .map_err(|e| format!("failed to create Parquet writer: {e}"))?;

    for batch in data {
        writer
            .write(batch)
            .map_err(|e| format!("failed to write Parquet batch: {e}"))?;
    }

    writer
        .close()
        .map_err(|e| format!("failed to close Parquet writer: {e}"))?;

    Ok(buf.into_inner())
}

// ---------------------------------------------------------------------------
// InsertOverwrite (Parquet) — hive-partitioned per-partition replace
// ---------------------------------------------------------------------------

/// Top-level entry point for `WriteStrategy::InsertOverwrite` against a Parquet
/// target. Treats `file_config.path` as a directory containing a hive-style
/// dataset (`<root>/<col>=<value>/...`). Groups incoming batches by the
/// configured `partition_column`, then for each touched partition value
/// atomically replaces that partition's contents. Untouched partitions are
/// left alone — that is the entire point of this strategy versus
/// `truncate_insert`.
///
/// Local writes use a staging-dir + atomic-rename pattern. Cloud writes
/// list-then-delete-then-put; object stores don't offer atomic rename so a
/// brief inconsistency window during replace is unavoidable for now.
async fn write_parquet_insert_overwrite(
    file_config: &FileConfig,
    data: Vec<RecordBatch>,
    ctx: &MaterializationContext,
    start: Instant,
) -> Result<MaterializationReceipt, ProviderError> {
    let partition_col = ctx.partition_column.as_deref().ok_or_else(|| {
        "write_strategy=insert_overwrite requires materialization.partition_column \
         on the sink node"
            .to_string()
    })?;

    // Empty input: nothing to write, nothing to delete. Be explicit about
    // this so users don't get a surprise empty-target.
    if data.is_empty() {
        let stats = WriteStats {
            rows_written: 0,
            bytes_written: 0,
            duration: start.elapsed(),
        };
        return Ok(MaterializationReceipt::from_write_stats(&stats, ctx));
    }

    // Group batches by distinct partition value. Returns rows with the
    // partition column projected out — hive layout encodes the value in the
    // path, so duplicating it inside the file is just dead bytes.
    let groups = partition_batches_by_column(&data, partition_col)?;
    if groups.is_empty() {
        let stats = WriteStats {
            rows_written: 0,
            bytes_written: 0,
            duration: start.elapsed(),
        };
        return Ok(MaterializationReceipt::from_write_stats(&stats, ctx));
    }

    let path_str = file_config
        .path
        .to_str()
        .ok_or_else(|| format!("path is not valid UTF-8: {}", file_config.path.display()))?;

    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;

    if cloud_store::is_cloud_url(path_str) {
        let cloud_url = cloud_store::parse_cloud_url(path_str)?;
        let store = cloud_store::build_object_store(&cloud_url, &file_config.storage_options)?;
        let root = cloud_url.object_path.trim_end_matches('/').to_string();

        for (value, batches) in &groups {
            let encoded = hive_encode(value);
            let prefix = if root.is_empty() {
                format!("{partition_col}={encoded}")
            } else {
                format!("{root}/{partition_col}={encoded}")
            };
            let bytes = write_parquet_to_bytes(batches, file_config)?;
            let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

            // Replace partition: list everything under the prefix, delete
            // it, then put the new file. The dataset prefix lives at
            // `<prefix>/`, so add the trailing slash to avoid clobbering a
            // sibling partition whose name starts with the same string
            // (`year=2026` vs `year=20260101`).
            replace_cloud_partition(store.as_ref(), &format!("{prefix}/")).await?;

            let object_path = ObjectPath::parse(format!("{prefix}/data.parquet"))
                .map_err(|e| format!("invalid object path '{prefix}/data.parquet': {e}"))?;
            let bytes_len = bytes.len() as u64;
            store
                .put(&object_path, PutPayload::from(bytes))
                .await
                .map_err(|e| format!("failed to upload partition '{prefix}': {e}"))?;

            total_rows += rows;
            total_bytes += bytes_len;
        }
    } else {
        let root = if file_config.path.is_relative() {
            std::env::current_dir()
                .map_err(|e| format!("failed to get current directory: {e}"))?
                .join(&file_config.path)
        } else {
            file_config.path.clone()
        };

        // The root directory must exist (or be creatable) — partitions sit
        // under it. If `root` exists today as a *file* (e.g. an old
        // single-file parquet from before the partitioning upgrade) the
        // user is mid-migration and we refuse loudly rather than blow away
        // their file.
        if root.exists() && !root.is_dir() {
            return Err(format!(
                "insert_overwrite target '{}' exists as a file, not a directory; \
                 remove it (or migrate to a directory layout) before re-running",
                root.display()
            )
            .into());
        }
        fs::create_dir_all(&root)
            .map_err(|e| format!("failed to create directory '{}': {e}", root.display()))?;

        for (value, batches) in &groups {
            let encoded = hive_encode(value);
            let partition_dir = root.join(format!("{partition_col}={encoded}"));
            let staging_dir = root.join(format!(".staging-{partition_col}={encoded}"));

            // Stage the new partition contents alongside the live one and
            // swap once the write is done. If a previous run crashed
            // mid-staging, clean it up first so we never accidentally use
            // stale data.
            if staging_dir.exists() {
                fs::remove_dir_all(&staging_dir).map_err(|e| {
                    format!(
                        "failed to clean stale staging dir '{}': {e}",
                        staging_dir.display()
                    )
                })?;
            }
            fs::create_dir_all(&staging_dir).map_err(|e| {
                format!(
                    "failed to create staging dir '{}': {e}",
                    staging_dir.display()
                )
            })?;

            let staging_file = staging_dir.join("data.parquet");
            let (rows, bytes) = match write_parquet(&staging_file, batches, file_config) {
                Ok(v) => v,
                Err(e) => {
                    let _ = fs::remove_dir_all(&staging_dir);
                    return Err(e);
                }
            };

            // Atomic replace: remove the old partition then rename staging
            // into place. `fs::rename` is atomic on POSIX when src and dst
            // are on the same filesystem, which they always are here
            // because both live under `root`.
            if partition_dir.exists() {
                fs::remove_dir_all(&partition_dir).map_err(|e| {
                    format!(
                        "failed to remove old partition '{}': {e}",
                        partition_dir.display()
                    )
                })?;
            }
            fs::rename(&staging_dir, &partition_dir).map_err(|e| {
                format!(
                    "failed to swap staging dir into '{}': {e}",
                    partition_dir.display()
                )
            })?;

            total_rows += rows;
            total_bytes += bytes;
        }
    }

    let stats = WriteStats {
        rows_written: total_rows,
        bytes_written: total_bytes,
        duration: start.elapsed(),
    };
    let mut receipt = MaterializationReceipt::from_write_stats(&stats, ctx);
    // Every row in an insert_overwrite write is an insert against the
    // newly-replaced partition. We can't cheaply count the rows we deleted
    // (would require scanning the old partition), so leave rows_deleted at
    // the receipt's "zero or unmeasured" sentinel.
    receipt.rows_inserted = total_rows;
    Ok(receipt)
}

/// Group `batches` by the distinct values of `column_name`, returning a map
/// from the formatted partition value to the projected sub-batches with the
/// partition column dropped.
///
/// Uses `BTreeMap` so partition iteration order is deterministic — useful for
/// readable logs and stable test assertions.
fn partition_batches_by_column(
    batches: &[RecordBatch],
    column_name: &str,
) -> Result<BTreeMap<String, Vec<RecordBatch>>, ProviderError> {
    if batches.is_empty() {
        return Ok(BTreeMap::new());
    }

    let schema = batches[0].schema();
    let col_idx = schema.index_of(column_name).map_err(|_| {
        format!(
            "partition_column '{column_name}' not found in sink input schema (have: {:?})",
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        )
    })?;

    // Build the projected schema (everything except the partition column)
    // once, since every batch shares it.
    let projected_indices: Vec<usize> = (0..schema.fields().len())
        .filter(|i| *i != col_idx)
        .collect();
    let projected_schema = Arc::new(Schema::new(
        projected_indices
            .iter()
            .map(|i| schema.field(*i).clone())
            .collect::<Vec<_>>(),
    ));
    let format_options = FormatOptions::default();
    let mut groups: BTreeMap<String, Vec<RecordBatch>> = BTreeMap::new();

    for batch in batches {
        if batch.schema().index_of(column_name).is_err() {
            return Err(format!(
                "partition_column '{column_name}' missing from a batch midway through the \
                 sink stream — schema is unstable, refusing to write"
            )
            .into());
        }
        let array = batch.column(col_idx);

        // Hive can't represent a null partition value cleanly. Refuse it
        // loudly rather than silently dropping rows or writing them to a
        // `__HIVE_DEFAULT_PARTITION__` directory the file source won't read.
        if array.null_count() > 0 {
            return Err(format!(
                "partition_column '{column_name}' contains {} null value(s); \
                 nulls cannot be hive-encoded — filter or coalesce them upstream",
                array.null_count()
            )
            .into());
        }

        let formatter = ArrayFormatter::try_new(array.as_ref(), &format_options)
            .map_err(|e| format!("failed to format partition column '{column_name}': {e}"))?;

        // Bucket row indices by formatted partition value.
        let mut buckets: BTreeMap<String, Vec<u32>> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let value = formatter.value(row).to_string();
            buckets.entry(value).or_default().push(row as u32);
        }

        for (value, indices) in buckets {
            // Take the rows for this partition out of the batch, then
            // project away the partition column.
            let idx_array = UInt32Array::from(indices);
            let mut projected_columns = Vec::with_capacity(projected_indices.len());
            for &col in &projected_indices {
                let taken = take(batch.column(col).as_ref(), &idx_array, None).map_err(|e| {
                    format!("failed to project partition '{value}' for column index {col}: {e}")
                })?;
                projected_columns.push(taken);
            }
            let sub_batch = RecordBatch::try_new(projected_schema.clone(), projected_columns)
                .map_err(|e| format!("failed to build sub-batch for partition '{value}': {e}"))?;
            groups.entry(value).or_default().push(sub_batch);
        }
    }

    Ok(groups)
}

/// Hive-encode a partition value for use as a directory segment. Hive's own
/// rules percent-encode `/`, `=`, control chars, etc. We implement the
/// minimum subset that keeps round-tripping safe with DataFusion's hive
/// partition reader: replace `/`, `=`, and `%` with their percent-encoded
/// forms. Everything else passes through.
fn hive_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '%' => out.push_str("%25"),
            '/' => out.push_str("%2F"),
            '=' => out.push_str("%3D"),
            _ => out.push(ch),
        }
    }
    out
}

/// List every object under `prefix` and delete it. Used to clear a partition
/// before writing its replacement on cloud sinks. Errors are surfaced
/// directly — a partial-delete leaves the partition in a known-bad state
/// that the next run will overwrite, but the user gets a loud error so they
/// can investigate.
async fn replace_cloud_partition(
    store: &dyn ObjectStore,
    prefix: &str,
) -> Result<(), ProviderError> {
    // Use `list_with_delimiter` (single round-trip, returns a Vec instead of
    // a stream) so we don't have to pull `futures-util` in just to iterate a
    // BoxStream. This is non-recursive — fine for our layout, which writes
    // only `partition_dir/data.parquet` with no nesting. If a previous
    // version of armillary ever wrote nested files under a partition, they'll
    // be left behind; document the limitation alongside the writer.
    let prefix_trimmed = prefix.trim_end_matches('/');
    let prefix_path =
        ObjectPath::parse(prefix_trimmed).map_err(|e| format!("invalid prefix '{prefix}': {e}"))?;
    let listing = store
        .list_with_delimiter(Some(&prefix_path))
        .await
        .map_err(|e| format!("failed to list partition '{prefix}': {e}"))?;
    let to_delete: Vec<ObjectPath> = listing.objects.into_iter().map(|o| o.location).collect();
    if to_delete.is_empty() {
        return Ok(());
    }
    debug!(
        prefix = %prefix,
        count = to_delete.len(),
        "clearing existing partition objects"
    );
    for path in to_delete {
        if let Err(e) = store.delete(&path).await {
            warn!(path = %path, error = %e, "failed to delete object during partition replace");
            return Err(format!("failed to delete '{path}' during partition replace: {e}").into());
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Local write
// ---------------------------------------------------------------------------

/// Write record batches as CSV to a local file.
fn write_csv(
    path: &std::path::Path,
    data: &[RecordBatch],
    config: &FileConfig,
) -> Result<(u64, u64), ProviderError> {
    let append = matches!(config.options.write_mode, Some(WriteMode::Append)) && path.exists();

    let file = if append {
        fs::OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|e| format!("failed to open '{}' for append: {e}", path.display()))?
    } else {
        fs::File::create(path).map_err(|e| format!("failed to create '{}': {e}", path.display()))?
    };
    let mut buf = BufWriter::new(file);

    let has_header = config.options.has_header.unwrap_or(true);

    let mut rows_written: u64 = 0;
    for (i, batch) in data.iter().enumerate() {
        let mut builder = CsvWriterBuilder::new();

        if let Some(delim) = config.options.delimiter {
            builder = builder.with_delimiter(delim as u8);
        }
        if let Some(quote) = config.options.quote_char {
            builder = builder.with_quote(quote as u8);
        }

        // Write header only for the first batch (and not when appending).
        let write_header = has_header && i == 0 && !append;
        builder = builder.with_header(write_header);

        let mut writer = builder.build(&mut buf);
        writer
            .write(batch)
            .map_err(|e| format!("failed to write CSV batch: {e}"))?;

        rows_written += batch.num_rows() as u64;
    }

    buf.flush()
        .map_err(|e| format!("failed to flush CSV output: {e}"))?;

    let bytes_written = fs::metadata(path).map(|m| m.len()).unwrap_or(0);

    Ok((rows_written, bytes_written))
}

/// Write record batches as Parquet to a local file.
fn write_parquet(
    path: &std::path::Path,
    data: &[RecordBatch],
    config: &FileConfig,
) -> Result<(u64, u64), ProviderError> {
    if matches!(config.options.write_mode, Some(WriteMode::Append)) && path.exists() {
        return Err(
            "parquet append mode is not supported; use overwrite or write to a new file".into(),
        );
    }

    let schema = data[0].schema();

    let compression = match &config.options.compression {
        Some(codec) => parse_compression(codec)?,
        None => Compression::SNAPPY,
    };

    let mut props_builder = WriterProperties::builder().set_compression(compression);

    if let Some(rg_size) = config.options.row_group_size {
        props_builder = props_builder.set_max_row_group_row_count(Some(rg_size));
    }

    let props = props_builder.build();

    let file = fs::File::create(path)
        .map_err(|e| format!("failed to create '{}': {e}", path.display()))?;

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| format!("failed to create Parquet writer: {e}"))?;

    let mut rows_written: u64 = 0;
    for batch in data {
        writer
            .write(batch)
            .map_err(|e| format!("failed to write Parquet batch: {e}"))?;
        rows_written += batch.num_rows() as u64;
    }

    let _metadata = writer
        .close()
        .map_err(|e| format!("failed to close Parquet writer: {e}"))?;

    let bytes_written = fs::metadata(path).map(|m| m.len()).unwrap_or(0);

    Ok((rows_written, bytes_written))
}

/// Parse a compression codec string into a Parquet `Compression` variant.
pub(crate) fn parse_compression(codec: &str) -> Result<Compression, ProviderError> {
    match codec.to_lowercase().as_str() {
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(Default::default())),
        "zstd" => Ok(Compression::ZSTD(Default::default())),
        "lz4" => Ok(Compression::LZ4),
        "none" | "uncompressed" => Ok(Compression::UNCOMPRESSED),
        other => Err(format!(
            "unsupported parquet compression codec '{other}'; expected one of: snappy, gzip, zstd, lz4, none"
        )
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_sink_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FileSink>();
    }

    #[test]
    fn check_strategy_csv_only_allows_append() {
        assert!(check_strategy_supported(&FileFormat::Csv, WriteStrategy::Append).is_ok());
        for s in [
            WriteStrategy::Merge,
            WriteStrategy::DeleteInsert,
            WriteStrategy::TruncateInsert,
            WriteStrategy::InsertOverwrite,
            WriteStrategy::Snapshot,
        ] {
            assert!(
                check_strategy_supported(&FileFormat::Csv, s).is_err(),
                "csv must reject {s:?}"
            );
        }
        // Snapshot rejection message must point users at the alternative.
        let err = check_strategy_supported(&FileFormat::Csv, WriteStrategy::Snapshot)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("SCD2") && err.contains("parquet"),
            "csv snapshot error should mention SCD2 and parquet alternative, got: {err}"
        );
    }

    #[test]
    fn check_strategy_parquet_allows_append_truncate_and_insert_overwrite() {
        for s in [
            WriteStrategy::Append,
            WriteStrategy::TruncateInsert,
            WriteStrategy::InsertOverwrite,
            WriteStrategy::Snapshot,
        ] {
            assert!(
                check_strategy_supported(&FileFormat::Parquet, s).is_ok(),
                "parquet must accept {s:?}"
            );
        }
        for s in [WriteStrategy::Merge, WriteStrategy::DeleteInsert] {
            assert!(
                check_strategy_supported(&FileFormat::Parquet, s).is_err(),
                "parquet must reject {s:?}"
            );
        }
    }

    #[test]
    fn hive_encode_escapes_special_chars() {
        assert_eq!(hive_encode("2026-04-08"), "2026-04-08");
        assert_eq!(hive_encode("a/b"), "a%2Fb");
        assert_eq!(hive_encode("k=v"), "k%3Dv");
        assert_eq!(hive_encode("100%"), "100%25");
    }

    #[tokio::test]
    async fn parquet_truncate_insert_overwrites_even_with_append_mode() {
        use crate::config::{FileConfig, FileOptions};
        use armillary_datafusion::provider::MaterializationContext;
        use armillary_engine::materialization::{MaterializationPolicy, ReadMode};
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("out.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();

        // Pre-existing file: with WriteStrategy::Append + write_mode=Append,
        // parquet would error. With TruncateInsert, the sink must override
        // and overwrite cleanly.
        std::fs::write(&path, b"stale").unwrap();

        let file_config = FileConfig {
            path: path.clone(),
            format: FileFormat::Parquet,
            options: FileOptions {
                write_mode: Some(WriteMode::Append),
                ..Default::default()
            },
            table_partition_cols: None,
            storage_options: Default::default(),
        };

        let policy = MaterializationPolicy {
            write_strategy: WriteStrategy::TruncateInsert,
            ..MaterializationPolicy::default()
        };
        // (Default ReadMode is Full, default OnSchemaChange/FirstRun, no
        // watermark/unique_keys/partition_column — exactly the shape we want.)
        let _ = ReadMode::Full;

        let sink_config = SinkConfig {
            connector: "file".to_string(),
            materialization: Some(policy),
            config: serde_json::to_value(&file_config).unwrap(),
        };

        let sink = FileSink::new();
        let ctx = MaterializationContext::from_policy(sink_config.materialization.as_ref());
        let receipt = sink
            .write(&sink_config, vec![batch], &WriteOptions::default(), &ctx)
            .await
            .unwrap();
        assert_eq!(receipt.rows_written, 2);
    }
}
