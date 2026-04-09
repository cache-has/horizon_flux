// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Watermark coercion, filter construction, and max-watermark capture for
//! incremental sink materializations (planning doc 27).
//!
//! Three responsibilities live here so the executor coordinator can stay
//! focused on orchestration:
//!
//! 1. **Parse** a stored canonical watermark string into a DataFusion
//!    [`ScalarValue`] of the right Arrow type.
//! 2. **Build** the DataFusion [`Expr`] (`col > scalar`) the executor wraps
//!    around the source `DataFrame` so the optimizer can push it down into
//!    the connector's `TableProvider::scan(filters)`.
//! 3. **Capture** the new max watermark from the streamed `RecordBatch`es as
//!    they fly past the sink, folded across batches in a single pass.
//!
//! All three reuse the canonical serialization rules from doc 27's
//! "Watermark Type Coercion Rules" section. A round-trip
//! `serialize → deserialize → serialize` is a fixed point — enforced below
//! by unit test.

use crate::error::IncrementalStateError;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Int64Type, TimeUnit, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use flux_engine::materialization::WatermarkType;

/// Errors raised while parsing, building, or capturing a watermark.
///
/// All variants are deliberately loud — silent fallback to "no filter"
/// would re-introduce the dbt papercut where users believe their pipeline
/// is incremental but the system is quietly doing a full scan.
#[derive(Debug, thiserror::Error)]
pub enum WatermarkError {
    #[error(
        "incremental sink `{node_id}`: watermark column `{column}` is missing from source `{source_id}` schema"
    )]
    ColumnMissingFromSource {
        node_id: String,
        source_id: String,
        column: String,
    },

    #[error(
        "incremental sink `{node_id}`: watermark `{column}` declared as `{declared}` but source column type is `{actual}`"
    )]
    TypeMismatch {
        node_id: String,
        column: String,
        declared: String,
        actual: String,
    },

    #[error(
        "incremental sink `{node_id}`: stored watermark `{value}` is not a valid `{wtype}`: {reason}"
    )]
    UnparseableStoredValue {
        node_id: String,
        wtype: String,
        value: String,
        reason: String,
    },

    #[error("invalid ISO 8601 duration `{0}`")]
    InvalidLookback(String),
}

impl From<WatermarkError> for IncrementalStateError {
    fn from(err: WatermarkError) -> Self {
        IncrementalStateError::Database(err.to_string())
    }
}

/// Parsed lookback duration. Days/hours/minutes/seconds only — calendar
/// units (years, months) are rejected because "subtract a month from a
/// watermark" has no unambiguous meaning at this layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LookbackDuration {
    pub seconds: i64,
}

impl LookbackDuration {
    pub fn zero() -> Self {
        Self { seconds: 0 }
    }
}

/// Parse an ISO 8601 duration string of the shape `P[nD]T[nH][nM][nS]`.
///
/// Accepts e.g. `PT0S`, `PT15M`, `PT1H`, `P1D`, `P1DT2H`. Rejects calendar
/// units (Y/M/W) — those are ambiguous for watermark arithmetic and doc 27's
/// validator already constrains lookback to timestamp watermarks where
/// fixed-length subtraction is well-defined.
pub fn parse_lookback(s: &str) -> Result<LookbackDuration, WatermarkError> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'P' {
        return Err(WatermarkError::InvalidLookback(s.to_string()));
    }
    let mut seconds: i64 = 0;
    let mut i = 1usize;
    let mut in_time = false;
    let mut current = String::new();
    let mut saw_unit = false;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match c {
            'T' => {
                if in_time || !current.is_empty() {
                    return Err(WatermarkError::InvalidLookback(s.to_string()));
                }
                in_time = true;
            }
            '0'..='9' => current.push(c),
            'D' if !in_time => {
                let n: i64 = current
                    .parse()
                    .map_err(|_| WatermarkError::InvalidLookback(s.to_string()))?;
                seconds += n * 86_400;
                current.clear();
                saw_unit = true;
            }
            'H' if in_time => {
                let n: i64 = current
                    .parse()
                    .map_err(|_| WatermarkError::InvalidLookback(s.to_string()))?;
                seconds += n * 3_600;
                current.clear();
                saw_unit = true;
            }
            'M' if in_time => {
                let n: i64 = current
                    .parse()
                    .map_err(|_| WatermarkError::InvalidLookback(s.to_string()))?;
                seconds += n * 60;
                current.clear();
                saw_unit = true;
            }
            'S' if in_time => {
                let n: i64 = current
                    .parse()
                    .map_err(|_| WatermarkError::InvalidLookback(s.to_string()))?;
                seconds += n;
                current.clear();
                saw_unit = true;
            }
            _ => return Err(WatermarkError::InvalidLookback(s.to_string())),
        }
        i += 1;
    }
    if !current.is_empty() || !saw_unit {
        return Err(WatermarkError::InvalidLookback(s.to_string()));
    }
    Ok(LookbackDuration { seconds })
}

/// Convert a stored canonical watermark string to a DataFusion [`ScalarValue`]
/// matching its declared type.
///
/// - `Timestamp` → RFC 3339 nanoseconds → `ScalarValue::TimestampNanosecond(_, Some("UTC"))`
/// - `Int64` → decimal → `ScalarValue::Int64`
/// - `String` → verbatim → `ScalarValue::Utf8`
pub fn stored_to_scalar(
    node_id: &str,
    wtype: WatermarkType,
    value: &str,
    lookback: LookbackDuration,
) -> Result<ScalarValue, WatermarkError> {
    match wtype {
        WatermarkType::Timestamp => {
            let dt = DateTime::parse_from_rfc3339(value).map_err(|e| {
                WatermarkError::UnparseableStoredValue {
                    node_id: node_id.to_string(),
                    wtype: "timestamp".into(),
                    value: value.to_string(),
                    reason: e.to_string(),
                }
            })?;
            let dt: DateTime<Utc> = dt.with_timezone(&Utc);
            let dt = dt - chrono::Duration::seconds(lookback.seconds);
            let nanos =
                dt.timestamp_nanos_opt()
                    .ok_or_else(|| WatermarkError::UnparseableStoredValue {
                        node_id: node_id.to_string(),
                        wtype: "timestamp".into(),
                        value: value.to_string(),
                        reason: "value out of nanosecond range".into(),
                    })?;
            Ok(ScalarValue::TimestampNanosecond(
                Some(nanos),
                Some("UTC".into()),
            ))
        }
        WatermarkType::Int64 => {
            // Lookback is rejected for int64 at policy validation time;
            // ignored here defensively.
            let n: i64 = value.parse().map_err(|e: std::num::ParseIntError| {
                WatermarkError::UnparseableStoredValue {
                    node_id: node_id.to_string(),
                    wtype: "int64".into(),
                    value: value.to_string(),
                    reason: e.to_string(),
                }
            })?;
            Ok(ScalarValue::Int64(Some(n)))
        }
        WatermarkType::String => Ok(ScalarValue::Utf8(Some(value.to_string()))),
    }
}

/// Build the DataFusion `col > scalar` filter expression the coordinator
/// wraps around the source DataFrame.
pub fn build_filter_expr(column: &str, scalar: ScalarValue) -> Expr {
    col(column).gt(lit(scalar))
}

/// Format a [`ScalarValue`] back into the canonical stored string form.
/// Used after capturing a new max from a stream so the round-trip is a
/// fixed point.
pub fn scalar_to_stored(wtype: WatermarkType, scalar: &ScalarValue) -> Option<String> {
    match (wtype, scalar) {
        (WatermarkType::Timestamp, ScalarValue::TimestampNanosecond(Some(n), _)) => {
            let dt = DateTime::<Utc>::from_timestamp_nanos(*n);
            Some(dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string())
        }
        (WatermarkType::Int64, ScalarValue::Int64(Some(n))) => Some(n.to_string()),
        (WatermarkType::String, ScalarValue::Utf8(Some(s))) => Some(s.clone()),
        _ => None,
    }
}

/// Fold the maximum watermark value across an iterator of record batches.
///
/// Returns `None` when no non-null values were observed (the column is
/// either missing, all-null, or the input is empty). Coordinators treat
/// `None` as "no advancement" — they keep the previous stored watermark.
pub fn fold_max_watermark(
    batches: &[RecordBatch],
    column: &str,
    wtype: WatermarkType,
) -> Result<Option<ScalarValue>, IncrementalStateError> {
    let mut current: Option<ScalarValue> = None;
    for batch in batches {
        let Some((_, idx)) = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == column)
            .map(|(idx, f)| (f.clone(), idx))
        else {
            continue;
        };
        let array = batch.column(idx);
        let max = array_max(array, wtype)?;
        current = match (current, max) {
            (None, m) => m,
            (Some(c), None) => Some(c),
            (Some(c), Some(m)) => Some(if scalar_gt(&m, &c) { m } else { c }),
        };
    }
    Ok(current)
}

fn scalar_gt(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (
            ScalarValue::TimestampNanosecond(Some(x), _),
            ScalarValue::TimestampNanosecond(Some(y), _),
        ) => x > y,
        (ScalarValue::Int64(Some(x)), ScalarValue::Int64(Some(y))) => x > y,
        (ScalarValue::Utf8(Some(x)), ScalarValue::Utf8(Some(y))) => x > y,
        _ => false,
    }
}

fn array_max(
    array: &dyn Array,
    wtype: WatermarkType,
) -> Result<Option<ScalarValue>, IncrementalStateError> {
    if array.is_empty() {
        return Ok(None);
    }
    match wtype {
        WatermarkType::Timestamp => {
            // Cast whatever timestamp/date variant we have to TimestampNanosecond(UTC)
            // so a single max kernel covers the lot. Naive timestamps inherit
            // UTC per doc 27.
            let target = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
            let casted = arrow::compute::cast(array, &target).map_err(|e| {
                IncrementalStateError::Database(format!("watermark cast to ts(ns,UTC) failed: {e}"))
            })?;
            let typed = casted.as_primitive::<TimestampNanosecondType>();
            let mut max: Option<i64> = None;
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    continue;
                }
                let v = typed.value(i);
                max = Some(max.map_or(v, |m| m.max(v)));
            }
            Ok(max.map(|n| ScalarValue::TimestampNanosecond(Some(n), Some("UTC".into()))))
        }
        WatermarkType::Int64 => {
            let casted = arrow::compute::cast(array, &DataType::Int64).map_err(|e| {
                IncrementalStateError::Database(format!("watermark cast to i64 failed: {e}"))
            })?;
            let typed = casted.as_primitive::<Int64Type>();
            let mut max: Option<i64> = None;
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    continue;
                }
                let v = typed.value(i);
                max = Some(max.map_or(v, |m| m.max(v)));
            }
            Ok(max.map(|n| ScalarValue::Int64(Some(n))))
        }
        WatermarkType::String => {
            let casted = arrow::compute::cast(array, &DataType::Utf8).map_err(|e| {
                IncrementalStateError::Database(format!("watermark cast to utf8 failed: {e}"))
            })?;
            let typed = casted.as_string::<i32>();
            let mut max: Option<String> = None;
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    continue;
                }
                let v = typed.value(i);
                max = Some(match max {
                    Some(m) if m.as_str() >= v => m,
                    _ => v.to_string(),
                });
            }
            Ok(max.map(|s| ScalarValue::Utf8(Some(s))))
        }
    }
}

/// Cheap structural compatibility check between an Arrow column and a
/// declared [`WatermarkType`]. Used by the pre-pass to fail before any I/O
/// when a source column type drifts away from what the sink declared.
pub fn watermark_type_matches(arrow_type: &DataType, wtype: WatermarkType) -> bool {
    match wtype {
        WatermarkType::Timestamp => matches!(
            arrow_type,
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
        ),
        WatermarkType::Int64 => matches!(
            arrow_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
        ),
        WatermarkType::String => matches!(arrow_type, DataType::Utf8 | DataType::LargeUtf8),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn parse_lookback_zero() {
        assert_eq!(parse_lookback("PT0S").unwrap().seconds, 0);
    }

    #[test]
    fn parse_lookback_basic() {
        assert_eq!(parse_lookback("PT15M").unwrap().seconds, 900);
        assert_eq!(parse_lookback("PT1H").unwrap().seconds, 3_600);
        assert_eq!(parse_lookback("P1D").unwrap().seconds, 86_400);
        assert_eq!(parse_lookback("P1DT2H").unwrap().seconds, 86_400 + 7_200);
    }

    #[test]
    fn parse_lookback_rejects_garbage() {
        assert!(parse_lookback("1H").is_err());
        assert!(parse_lookback("PT").is_err());
        assert!(parse_lookback("PT1Y").is_err()); // calendar units rejected
    }

    #[test]
    fn timestamp_round_trip_is_fixed_point() {
        let s = "2026-04-08T12:34:56.123456789Z";
        let scalar = stored_to_scalar(
            "sink",
            WatermarkType::Timestamp,
            s,
            LookbackDuration::zero(),
        )
        .unwrap();
        let back = scalar_to_stored(WatermarkType::Timestamp, &scalar).unwrap();
        // Re-parse the round-trip output to verify a second pass is also a fixed point.
        let scalar2 = stored_to_scalar(
            "sink",
            WatermarkType::Timestamp,
            &back,
            LookbackDuration::zero(),
        )
        .unwrap();
        let back2 = scalar_to_stored(WatermarkType::Timestamp, &scalar2).unwrap();
        assert_eq!(back, back2);
    }

    #[test]
    fn int64_round_trip() {
        let scalar = stored_to_scalar(
            "sink",
            WatermarkType::Int64,
            "12345",
            LookbackDuration::zero(),
        )
        .unwrap();
        assert_eq!(
            scalar_to_stored(WatermarkType::Int64, &scalar).unwrap(),
            "12345"
        );
    }

    #[test]
    fn string_round_trip() {
        let scalar = stored_to_scalar(
            "sink",
            WatermarkType::String,
            "ULID-XYZ",
            LookbackDuration::zero(),
        )
        .unwrap();
        assert_eq!(
            scalar_to_stored(WatermarkType::String, &scalar).unwrap(),
            "ULID-XYZ"
        );
    }

    #[test]
    fn lookback_subtracts_from_timestamp() {
        let scalar = stored_to_scalar(
            "sink",
            WatermarkType::Timestamp,
            "2026-04-08T12:00:00.000000000Z",
            LookbackDuration { seconds: 3_600 },
        )
        .unwrap();
        let back = scalar_to_stored(WatermarkType::Timestamp, &scalar).unwrap();
        assert!(back.starts_with("2026-04-08T11:00:00"));
    }

    #[test]
    fn unparseable_timestamp_errors_loud() {
        let err = stored_to_scalar(
            "sink",
            WatermarkType::Timestamp,
            "not-a-date",
            LookbackDuration::zero(),
        )
        .unwrap_err();
        assert!(matches!(err, WatermarkError::UnparseableStoredValue { .. }));
    }

    #[test]
    fn fold_max_int64() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 5, 3]))],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![2, 9, 4]))],
        )
        .unwrap();
        let max = fold_max_watermark(&[b1, b2], "id", WatermarkType::Int64).unwrap();
        assert_eq!(max, Some(ScalarValue::Int64(Some(9))));
    }

    #[test]
    fn fold_max_timestamp() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        )]));
        let arr =
            TimestampNanosecondArray::from(vec![100i64, 300, 200]).with_timezone(Arc::from("UTC"));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let max = fold_max_watermark(&[batch], "ts", WatermarkType::Timestamp).unwrap();
        assert_eq!(
            max,
            Some(ScalarValue::TimestampNanosecond(
                Some(300),
                Some("UTC".into())
            ))
        );
    }

    #[test]
    fn fold_max_string() {
        let schema = Arc::new(Schema::new(vec![Field::new("ulid", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["aa", "zz", "mm"]))],
        )
        .unwrap();
        let max = fold_max_watermark(&[batch], "ulid", WatermarkType::String).unwrap();
        assert_eq!(max, Some(ScalarValue::Utf8(Some("zz".to_string()))));
    }

    #[test]
    fn fold_max_missing_column_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        let max = fold_max_watermark(&[batch], "id", WatermarkType::Int64).unwrap();
        assert_eq!(max, None);
    }

    #[test]
    fn type_matches_table() {
        assert!(watermark_type_matches(
            &DataType::Int64,
            WatermarkType::Int64
        ));
        assert!(watermark_type_matches(
            &DataType::Int32,
            WatermarkType::Int64
        ));
        assert!(watermark_type_matches(
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            WatermarkType::Timestamp
        ));
        assert!(watermark_type_matches(
            &DataType::Date32,
            WatermarkType::Timestamp
        ));
        assert!(watermark_type_matches(
            &DataType::Utf8,
            WatermarkType::String
        ));
        assert!(!watermark_type_matches(
            &DataType::Utf8,
            WatermarkType::Int64
        ));
        assert!(!watermark_type_matches(
            &DataType::Int64,
            WatermarkType::Timestamp
        ));
    }
}
