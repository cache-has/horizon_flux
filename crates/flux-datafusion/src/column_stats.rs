// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-column statistics computed from Arrow RecordBatches for preview display.

use arrow::array::{Array, AsArray, BooleanArray};
use arrow::compute;
use arrow::datatypes::{DataType, Float64Type};
use arrow::record_batch::RecordBatch;
use serde::Serialize;

/// Statistics for a single column, variant depends on the column's data type.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ColumnStats {
    Numeric {
        min: Option<f64>,
        max: Option<f64>,
        mean: Option<f64>,
        null_count: u64,
    },
    String {
        min_length: Option<u64>,
        max_length: Option<u64>,
        unique_count: u64,
        null_count: u64,
    },
    Boolean {
        true_count: u64,
        false_count: u64,
        null_count: u64,
    },
    Other {
        null_count: u64,
    },
}

/// Compute per-column statistics for a set of record batches.
///
/// Returns a `Vec<ColumnStats>` with one entry per column in schema order.
/// If `batches` is empty, returns an empty vec.
pub fn compute_column_stats(batches: &[RecordBatch]) -> Vec<ColumnStats> {
    if batches.is_empty() {
        return vec![];
    }

    let schema = batches[0].schema();
    let num_cols = schema.fields().len();
    let mut results = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let field = schema.field(col_idx);
        let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(col_idx).as_ref()).collect();

        let stats = match field.data_type() {
            dt if is_numeric(dt) => compute_numeric_stats(&arrays),
            DataType::Utf8 | DataType::LargeUtf8 => compute_string_stats(&arrays),
            DataType::Boolean => compute_boolean_stats(&arrays),
            _ => compute_other_stats(&arrays),
        };
        results.push(stats);
    }

    results
}

fn is_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn compute_numeric_stats(arrays: &[&dyn Array]) -> ColumnStats {
    let mut null_count: u64 = 0;
    let mut overall_min: Option<f64> = None;
    let mut overall_max: Option<f64> = None;
    let mut sum: f64 = 0.0;
    let mut value_count: u64 = 0;

    for &arr in arrays {
        null_count += arr.null_count() as u64;

        // Cast to Float64 for uniform statistics computation.
        let f64_arr = match compute::cast(arr, &DataType::Float64) {
            Ok(a) => a,
            Err(_) => continue,
        };
        let f64_arr = f64_arr.as_primitive::<Float64Type>();

        if let Some(min_val) = compute::min(f64_arr) {
            overall_min = Some(match overall_min {
                Some(prev) => prev.min(min_val),
                None => min_val,
            });
        }
        if let Some(max_val) = compute::max(f64_arr) {
            overall_max = Some(match overall_max {
                Some(prev) => prev.max(max_val),
                None => max_val,
            });
        }

        // Sum for mean calculation.
        for i in 0..f64_arr.len() {
            if !f64_arr.is_null(i) {
                sum += f64_arr.value(i);
                value_count += 1;
            }
        }
    }

    let mean = if value_count > 0 {
        Some(sum / value_count as f64)
    } else {
        None
    };

    ColumnStats::Numeric {
        min: overall_min,
        max: overall_max,
        mean,
        null_count,
    }
}

fn compute_string_stats(arrays: &[&dyn Array]) -> ColumnStats {
    let mut null_count: u64 = 0;
    let mut min_len: Option<u64> = None;
    let mut max_len: Option<u64> = None;
    let mut unique_set = std::collections::HashSet::new();

    for &arr in arrays {
        null_count += arr.null_count() as u64;

        // Try Utf8 first, then LargeUtf8.
        if let Some(str_arr) = arr.as_any().downcast_ref::<arrow::array::StringArray>() {
            for i in 0..str_arr.len() {
                if !str_arr.is_null(i) {
                    let val = str_arr.value(i);
                    let len = val.len() as u64;
                    min_len = Some(min_len.map_or(len, |prev: u64| prev.min(len)));
                    max_len = Some(max_len.map_or(len, |prev: u64| prev.max(len)));
                    unique_set.insert(val.to_owned());
                }
            }
        } else if let Some(str_arr) = arr
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
        {
            for i in 0..str_arr.len() {
                if !str_arr.is_null(i) {
                    let val = str_arr.value(i);
                    let len = val.len() as u64;
                    min_len = Some(min_len.map_or(len, |prev: u64| prev.min(len)));
                    max_len = Some(max_len.map_or(len, |prev: u64| prev.max(len)));
                    unique_set.insert(val.to_owned());
                }
            }
        }
    }

    ColumnStats::String {
        min_length: min_len,
        max_length: max_len,
        unique_count: unique_set.len() as u64,
        null_count,
    }
}

fn compute_boolean_stats(arrays: &[&dyn Array]) -> ColumnStats {
    let mut true_count: u64 = 0;
    let mut false_count: u64 = 0;
    let mut null_count: u64 = 0;

    for &arr in arrays {
        null_count += arr.null_count() as u64;
        if let Some(bool_arr) = arr.as_any().downcast_ref::<BooleanArray>() {
            for i in 0..bool_arr.len() {
                if !bool_arr.is_null(i) {
                    if bool_arr.value(i) {
                        true_count += 1;
                    } else {
                        false_count += 1;
                    }
                }
            }
        }
    }

    ColumnStats::Boolean {
        true_count,
        false_count,
        null_count,
    }
}

fn compute_other_stats(arrays: &[&dyn Array]) -> ColumnStats {
    let null_count: u64 = arrays.iter().map(|a| a.null_count() as u64).sum();
    ColumnStats::Other { null_count }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("score", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0),
                    Some(20.0),
                    None,
                    Some(30.0),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("hello"),
                    Some("hi"),
                    Some("world"),
                    None,
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                    None,
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn empty_batches_returns_empty() {
        assert!(compute_column_stats(&[]).is_empty());
    }

    #[test]
    fn numeric_stats_correct() {
        let batch = make_batch();
        let stats = compute_column_stats(&[batch]);

        // id column (Int32, no nulls)
        match &stats[0] {
            ColumnStats::Numeric {
                min,
                max,
                mean,
                null_count,
            } => {
                assert_eq!(*min, Some(1.0));
                assert_eq!(*max, Some(4.0));
                assert!((mean.unwrap() - 2.5).abs() < 1e-10);
                assert_eq!(*null_count, 0);
            }
            other => panic!("expected Numeric, got {other:?}"),
        }

        // score column (Float64, 1 null)
        match &stats[1] {
            ColumnStats::Numeric {
                min,
                max,
                mean,
                null_count,
            } => {
                assert_eq!(*min, Some(10.0));
                assert_eq!(*max, Some(30.0));
                assert!((mean.unwrap() - 20.0).abs() < 1e-10);
                assert_eq!(*null_count, 1);
            }
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn string_stats_correct() {
        let batch = make_batch();
        let stats = compute_column_stats(&[batch]);

        match &stats[2] {
            ColumnStats::String {
                min_length,
                max_length,
                unique_count,
                null_count,
            } => {
                assert_eq!(*min_length, Some(2)); // "hi"
                assert_eq!(*max_length, Some(5)); // "hello" / "world"
                assert_eq!(*unique_count, 3);
                assert_eq!(*null_count, 1);
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn boolean_stats_correct() {
        let batch = make_batch();
        let stats = compute_column_stats(&[batch]);

        match &stats[3] {
            ColumnStats::Boolean {
                true_count,
                false_count,
                null_count,
            } => {
                assert_eq!(*true_count, 2);
                assert_eq!(*false_count, 1);
                assert_eq!(*null_count, 1);
            }
            other => panic!("expected Boolean, got {other:?}"),
        }
    }
}
