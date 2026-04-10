// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Backfill types, range expansion, and variable mapping (planning doc 33).
//!
//! A backfill is a sequence of pipeline runs, each with a distinct set of
//! variable values derived from a range definition. This module contains the
//! pure data model and expansion logic — no async, no execution.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// IDs & statuses
// ---------------------------------------------------------------------------

/// Unique identifier for a backfill.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BackfillId(pub String);

impl BackfillId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

impl Default for BackfillId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for BackfillId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Top-level status of a backfill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillStatus {
    Pending,
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl BackfillStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "cancelled" => Some(Self::Cancelled),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// Status of a single backfill iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IterationStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Skipped,
}

impl IterationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "skipped" => Some(Self::Skipped),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Range definitions
// ---------------------------------------------------------------------------

/// Granularity for date range iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DateGranularity {
    Hour,
    Day,
    Week,
    Month,
}

/// How to generate iterations for a backfill.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RangeDefinition {
    /// Iterate over a date range with configurable granularity.
    DateRange {
        /// Inclusive start date (ISO 8601: `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS`).
        start: String,
        /// Exclusive end date.
        end: String,
        granularity: DateGranularity,
        /// Maps `$iteration.start` and `$iteration.end` to pipeline variables.
        variable_mapping: HashMap<String, String>,
    },
    /// Iterate over an explicit list of values.
    List {
        values: Vec<String>,
        /// Maps `$iteration.value` to pipeline variables.
        variable_mapping: HashMap<String, String>,
    },
    /// Iterate over rows returned by a SQL query (escape hatch).
    Sql {
        connection: String,
        query: String,
        /// Maps `$iteration.<column>` to pipeline variables.
        variable_mapping: HashMap<String, String>,
    },
}

// ---------------------------------------------------------------------------
// Expanded iteration (before execution)
// ---------------------------------------------------------------------------

/// A single expanded iteration ready for execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpandedIteration {
    /// Zero-based index within the backfill.
    pub index: u32,
    /// Canonical key for resume deduplication (e.g. `"2024-01-15"` for dates).
    pub key: String,
    /// Resolved variable overrides for this iteration's pipeline run.
    pub variables: HashMap<String, Value>,
}

// ---------------------------------------------------------------------------
// Backfill & iteration records (persisted)
// ---------------------------------------------------------------------------

/// A backfill record as stored in the metadata store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Backfill {
    pub id: BackfillId,
    pub pipeline_id: String,
    pub environment: String,
    pub range_definition: RangeDefinition,
    pub concurrency: u32,
    pub fail_fast: bool,
    pub full_refresh: bool,
    pub status: BackfillStatus,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub created_by: Option<String>,
}

/// A single iteration record as stored in the metadata store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillIteration {
    pub backfill_id: BackfillId,
    pub iteration_index: u32,
    pub iteration_key: String,
    pub variables: HashMap<String, Value>,
    pub status: IterationStatus,
    pub run_id: Option<String>,
    pub error: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}

/// Aggregated progress summary for a backfill.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillProgress {
    pub total: u32,
    pub succeeded: u32,
    pub failed: u32,
    pub running: u32,
    pub pending: u32,
    pub skipped: u32,
}

// ---------------------------------------------------------------------------
// Range expansion
// ---------------------------------------------------------------------------

/// Errors from range expansion or variable mapping.
#[derive(Debug, thiserror::Error)]
pub enum RangeError {
    #[error("invalid date `{0}`: {1}")]
    InvalidDate(String, String),

    #[error("start date `{start}` must be before end date `{end}`")]
    EmptyRange { start: String, end: String },

    #[error("variable mapping references unknown placeholder `{0}`")]
    UnknownPlaceholder(String),

    #[error("SQL range expansion is not supported in this context")]
    SqlNotSupported,
}

/// Expand a non-SQL range definition into a list of iterations.
///
/// SQL ranges require a database connection and are expanded by the
/// coordinator at runtime — this function returns [`RangeError::SqlNotSupported`]
/// for SQL ranges.
pub fn expand_range(range: &RangeDefinition) -> Result<Vec<ExpandedIteration>, RangeError> {
    match range {
        RangeDefinition::DateRange {
            start,
            end,
            granularity,
            variable_mapping,
        } => expand_date_range(start, end, *granularity, variable_mapping),
        RangeDefinition::List {
            values,
            variable_mapping,
        } => expand_list(values, variable_mapping),
        RangeDefinition::Sql { .. } => Err(RangeError::SqlNotSupported),
    }
}

fn expand_date_range(
    start: &str,
    end: &str,
    granularity: DateGranularity,
    variable_mapping: &HashMap<String, String>,
) -> Result<Vec<ExpandedIteration>, RangeError> {
    use chrono::{NaiveDate, NaiveDateTime};

    // Parse as date or datetime.
    let parse_dt = |s: &str| -> Result<NaiveDateTime, RangeError> {
        if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            Ok(d.and_hms_opt(0, 0, 0).unwrap())
        } else {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                .map_err(|e| RangeError::InvalidDate(s.to_string(), e.to_string()))
        }
    };

    let start_dt = parse_dt(start)?;
    let end_dt = parse_dt(end)?;

    if start_dt >= end_dt {
        return Err(RangeError::EmptyRange {
            start: start.to_string(),
            end: end.to_string(),
        });
    }

    let mut iterations = Vec::new();
    let mut cursor = start_dt;
    let mut index = 0u32;

    while cursor < end_dt {
        let next = advance(cursor, granularity);
        // Clamp to end boundary.
        let iter_end = if next > end_dt { end_dt } else { next };

        let iter_start_str = format_dt(cursor, granularity);
        let iter_end_str = format_dt(iter_end, granularity);
        let key = iter_start_str.clone();

        let variables = apply_date_mapping(variable_mapping, &iter_start_str, &iter_end_str)?;

        iterations.push(ExpandedIteration {
            index,
            key,
            variables,
        });

        cursor = next;
        index += 1;
    }

    Ok(iterations)
}

/// Advance a datetime by one granularity step.
fn advance(dt: chrono::NaiveDateTime, granularity: DateGranularity) -> chrono::NaiveDateTime {
    use chrono::{Datelike, Days, NaiveDate, TimeDelta};

    match granularity {
        DateGranularity::Hour => dt + TimeDelta::hours(1),
        DateGranularity::Day => dt + Days::new(1),
        DateGranularity::Week => dt + Days::new(7),
        DateGranularity::Month => {
            let (y, m) = if dt.month() == 12 {
                (dt.year() + 1, 1)
            } else {
                (dt.year(), dt.month() + 1)
            };
            NaiveDate::from_ymd_opt(y, m, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        }
    }
}

/// Format a datetime for display/key, adapted to granularity.
fn format_dt(dt: chrono::NaiveDateTime, granularity: DateGranularity) -> String {
    match granularity {
        DateGranularity::Hour => dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
        _ => dt.format("%Y-%m-%d").to_string(),
    }
}

fn apply_date_mapping(
    mapping: &HashMap<String, String>,
    iter_start: &str,
    iter_end: &str,
) -> Result<HashMap<String, Value>, RangeError> {
    let mut vars = HashMap::new();
    for (var_name, placeholder) in mapping {
        let value = match placeholder.as_str() {
            "$iteration.start" => iter_start.to_string(),
            "$iteration.end" => iter_end.to_string(),
            other => return Err(RangeError::UnknownPlaceholder(other.to_string())),
        };
        vars.insert(var_name.clone(), Value::String(value));
    }
    Ok(vars)
}

fn expand_list(
    values: &[String],
    variable_mapping: &HashMap<String, String>,
) -> Result<Vec<ExpandedIteration>, RangeError> {
    let mut iterations = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let mut vars = HashMap::new();
        for (var_name, placeholder) in variable_mapping {
            let resolved = match placeholder.as_str() {
                "$iteration.value" => value.clone(),
                other => return Err(RangeError::UnknownPlaceholder(other.to_string())),
            };
            vars.insert(var_name.clone(), Value::String(resolved));
        }
        iterations.push(ExpandedIteration {
            index: index as u32,
            key: value.clone(),
            variables: vars,
        });
    }
    Ok(iterations)
}

/// Expand a SQL range from pre-fetched rows. Each row is a map of column
/// name → string value. Called by the coordinator after executing the query.
pub fn expand_sql_rows(
    rows: &[HashMap<String, String>],
    variable_mapping: &HashMap<String, String>,
) -> Result<Vec<ExpandedIteration>, RangeError> {
    let mut iterations = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let mut vars = HashMap::new();
        for (var_name, placeholder) in variable_mapping {
            let col = placeholder
                .strip_prefix("$iteration.")
                .ok_or_else(|| RangeError::UnknownPlaceholder(placeholder.clone()))?;
            let value = row.get(col).cloned().unwrap_or_default();
            vars.insert(var_name.clone(), Value::String(value));
        }
        // Use first column value as key, or index if empty row.
        let key = row
            .values()
            .next()
            .cloned()
            .unwrap_or_else(|| index.to_string());
        iterations.push(ExpandedIteration {
            index: index as u32,
            key,
            variables: vars,
        });
    }
    Ok(iterations)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_range_daily_31_days() {
        let mapping = HashMap::from([("run_date".into(), "$iteration.start".into())]);
        let range = RangeDefinition::DateRange {
            start: "2024-01-01".into(),
            end: "2024-02-01".into(),
            granularity: DateGranularity::Day,
            variable_mapping: mapping,
        };
        let iters = expand_range(&range).unwrap();
        assert_eq!(iters.len(), 31);
        assert_eq!(iters[0].key, "2024-01-01");
        assert_eq!(iters[30].key, "2024-01-31");
        assert_eq!(
            iters[0].variables["run_date"],
            Value::String("2024-01-01".into())
        );
    }

    #[test]
    fn date_range_hourly() {
        let mapping = HashMap::from([
            ("start".into(), "$iteration.start".into()),
            ("end".into(), "$iteration.end".into()),
        ]);
        let range = RangeDefinition::DateRange {
            start: "2024-01-01T00:00:00".into(),
            end: "2024-01-01T03:00:00".into(),
            granularity: DateGranularity::Hour,
            variable_mapping: mapping,
        };
        let iters = expand_range(&range).unwrap();
        assert_eq!(iters.len(), 3);
        assert_eq!(iters[0].key, "2024-01-01T00:00:00");
        assert_eq!(iters[1].key, "2024-01-01T01:00:00");
        assert_eq!(iters[2].key, "2024-01-01T02:00:00");
        assert_eq!(
            iters[2].variables["end"],
            Value::String("2024-01-01T03:00:00".into())
        );
    }

    #[test]
    fn date_range_weekly() {
        let mapping = HashMap::from([("week_start".into(), "$iteration.start".into())]);
        let range = RangeDefinition::DateRange {
            start: "2024-01-01".into(),
            end: "2024-01-22".into(),
            granularity: DateGranularity::Week,
            variable_mapping: mapping,
        };
        let iters = expand_range(&range).unwrap();
        assert_eq!(iters.len(), 3);
        assert_eq!(iters[0].key, "2024-01-01");
        assert_eq!(iters[1].key, "2024-01-08");
        assert_eq!(iters[2].key, "2024-01-15");
    }

    #[test]
    fn date_range_monthly() {
        let mapping = HashMap::from([("month_start".into(), "$iteration.start".into())]);
        let range = RangeDefinition::DateRange {
            start: "2024-01-01".into(),
            end: "2024-04-01".into(),
            granularity: DateGranularity::Month,
            variable_mapping: mapping,
        };
        let iters = expand_range(&range).unwrap();
        assert_eq!(iters.len(), 3);
        assert_eq!(iters[0].key, "2024-01-01");
        assert_eq!(iters[1].key, "2024-02-01");
        assert_eq!(iters[2].key, "2024-03-01");
    }

    #[test]
    fn date_range_monthly_across_year_boundary() {
        let mapping = HashMap::from([("m".into(), "$iteration.start".into())]);
        let range = RangeDefinition::DateRange {
            start: "2024-11-01".into(),
            end: "2025-02-01".into(),
            granularity: DateGranularity::Month,
            variable_mapping: mapping,
        };
        let iters = expand_range(&range).unwrap();
        assert_eq!(iters.len(), 3);
        assert_eq!(iters[0].key, "2024-11-01");
        assert_eq!(iters[1].key, "2024-12-01");
        assert_eq!(iters[2].key, "2025-01-01");
    }

    #[test]
    fn date_range_empty_is_error() {
        let mapping = HashMap::new();
        let range = RangeDefinition::DateRange {
            start: "2024-02-01".into(),
            end: "2024-01-01".into(),
            granularity: DateGranularity::Day,
            variable_mapping: mapping,
        };
        assert!(expand_range(&range).is_err());
    }

    #[test]
    fn list_range() {
        let mapping = HashMap::from([("region".into(), "$iteration.value".into())]);
        let range = RangeDefinition::List {
            values: vec!["US".into(), "EU".into(), "APAC".into()],
            variable_mapping: mapping,
        };
        let iters = expand_range(&range).unwrap();
        assert_eq!(iters.len(), 3);
        assert_eq!(iters[0].key, "US");
        assert_eq!(iters[1].variables["region"], Value::String("EU".into()));
    }

    #[test]
    fn sql_rows_expansion() {
        let rows = vec![
            HashMap::from([("tenant_id".into(), "t1".into())]),
            HashMap::from([("tenant_id".into(), "t2".into())]),
        ];
        let mapping = HashMap::from([("tid".into(), "$iteration.tenant_id".into())]);
        let iters = expand_sql_rows(&rows, &mapping).unwrap();
        assert_eq!(iters.len(), 2);
        assert_eq!(iters[0].variables["tid"], Value::String("t1".into()));
        assert_eq!(iters[1].key, "t2");
    }

    #[test]
    fn unknown_placeholder_is_error() {
        let mapping = HashMap::from([("x".into(), "$iteration.bogus".into())]);
        let range = RangeDefinition::List {
            values: vec!["a".into()],
            variable_mapping: mapping,
        };
        assert!(expand_range(&range).is_err());
    }

    #[test]
    fn serde_roundtrip_date_range() {
        let range = RangeDefinition::DateRange {
            start: "2024-01-01".into(),
            end: "2024-02-01".into(),
            granularity: DateGranularity::Day,
            variable_mapping: HashMap::from([("d".into(), "$iteration.start".into())]),
        };
        let json = serde_json::to_string(&range).unwrap();
        let back: RangeDefinition = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, RangeDefinition::DateRange { .. }));
    }

    #[test]
    fn serde_roundtrip_backfill() {
        let bf = Backfill {
            id: BackfillId("test-id".into()),
            pipeline_id: "my-pipe".into(),
            environment: "prod".into(),
            range_definition: RangeDefinition::List {
                values: vec!["a".into()],
                variable_mapping: HashMap::new(),
            },
            concurrency: 4,
            fail_fast: true,
            full_refresh: true,
            status: BackfillStatus::Pending,
            created_at: "2024-01-01T00:00:00Z".into(),
            started_at: None,
            completed_at: None,
            created_by: Some("user".into()),
        };
        let json = serde_json::to_string(&bf).unwrap();
        let back: Backfill = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id.0, "test-id");
        assert_eq!(back.concurrency, 4);
    }
}
