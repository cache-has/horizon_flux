// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Freshness SLA types for the resource catalog (planning doc 37, sub-feature 3).
//!
//! An SLA declares a maximum acceptable age for a resource. The evaluator
//! periodically checks each resource with an SLA against its last successful
//! producing run and records the result.

use serde::{Deserialize, Serialize};

/// SLA configuration declared in a resource annotation YAML file.
///
/// ```yaml
/// sla:
///   freshness:
///     max_age: PT6H
///     warn_at: PT4H
///     scope: last_successful_run
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaConfig {
    pub freshness: FreshnessConfig,
}

/// Freshness thresholds within an SLA.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessConfig {
    /// Maximum acceptable age as an ISO 8601 duration (e.g. `PT6H`).
    pub max_age: String,
    /// Warning threshold as an ISO 8601 duration (e.g. `PT4H`). Optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warn_at: Option<String>,
    /// How freshness is computed.
    #[serde(default)]
    pub scope: SlaScope,
}

/// How the SLA evaluator computes resource age.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SlaScope {
    /// Age = now − last successful producing run's end time.
    #[default]
    LastSuccessfulRun,
    /// Age = now − next expected run time (derived from the trigger's schedule).
    DeclaredSchedule,
}

/// The result of evaluating one resource's SLA at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaEvaluation {
    /// Resource fingerprint.
    pub fingerprint: String,
    /// When this evaluation was performed (ISO 8601).
    pub evaluated_at: String,
    /// Computed status.
    pub status: SlaStatus,
    /// Age of the resource at evaluation time (ISO 8601 duration).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
    /// The max_age threshold from the SLA config.
    pub max_age: String,
    /// The warn_at threshold from the SLA config, if set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warn_at: Option<String>,
    /// Pipeline name that produces this resource (if known).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_pipeline: Option<String>,
    /// Timestamp of the last successful run (ISO 8601), if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<String>,
}

/// SLA compliance status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlaStatus {
    /// Resource is within its freshness window.
    Ok,
    /// Resource age has crossed the warning threshold but not the breach threshold.
    Warning,
    /// Resource age has exceeded the maximum allowed age.
    Breach,
    /// No producing run has ever succeeded (cannot evaluate).
    Unknown,
}

impl SlaStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warning => "warning",
            Self::Breach => "breach",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for SlaStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Parse an ISO 8601 duration string into a [`chrono::Duration`].
///
/// Supports a subset of ISO 8601: `PnDTnHnMnS` forms commonly used for
/// freshness windows (e.g. `PT6H`, `P1DT12H`, `PT30M`).
pub fn parse_iso_duration(s: &str) -> Result<chrono::Duration, String> {
    let s = s.trim();
    if !s.starts_with('P') {
        return Err(format!("ISO 8601 duration must start with 'P': {s}"));
    }
    let rest = &s[1..];

    let (date_part, time_part) = if let Some(t_pos) = rest.find('T') {
        (&rest[..t_pos], &rest[t_pos + 1..])
    } else {
        (rest, "")
    };

    let mut total_secs: i64 = 0;

    // Parse date part (days only for now).
    if !date_part.is_empty() {
        let days = parse_component(date_part, 'D')
            .map_err(|e| format!("invalid days in duration '{s}': {e}"))?;
        total_secs += days * 86400;
    }

    // Parse time part.
    if !time_part.is_empty() {
        let mut remaining = time_part;

        if let Some(h_pos) = remaining.find('H') {
            let hours: i64 = remaining[..h_pos]
                .parse()
                .map_err(|_| format!("invalid hours in duration '{s}'"))?;
            total_secs += hours * 3600;
            remaining = &remaining[h_pos + 1..];
        }
        if let Some(m_pos) = remaining.find('M') {
            let minutes: i64 = remaining[..m_pos]
                .parse()
                .map_err(|_| format!("invalid minutes in duration '{s}'"))?;
            total_secs += minutes * 60;
            remaining = &remaining[m_pos + 1..];
        }
        if let Some(s_pos) = remaining.find('S') {
            let secs: i64 = remaining[..s_pos]
                .parse()
                .map_err(|_| format!("invalid seconds in duration '{s}'"))?;
            total_secs += secs;
        }
    }

    Ok(chrono::Duration::seconds(total_secs))
}

/// Format a [`chrono::Duration`] as an ISO 8601 duration string.
pub fn format_iso_duration(d: &chrono::Duration) -> String {
    let total_secs = d.num_seconds().unsigned_abs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    let mut result = String::from("PT");
    if hours > 0 {
        result.push_str(&format!("{hours}H"));
    }
    if minutes > 0 {
        result.push_str(&format!("{minutes}M"));
    }
    if secs > 0 || result == "PT" {
        result.push_str(&format!("{secs}S"));
    }
    result
}

fn parse_component(s: &str, marker: char) -> Result<i64, String> {
    if let Some(pos) = s.find(marker) {
        s[..pos]
            .parse()
            .map_err(|_| format!("invalid number before '{marker}'"))
    } else if s.is_empty() {
        Ok(0)
    } else {
        Err(format!("expected '{marker}' in '{s}'"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pt6h() {
        let d = parse_iso_duration("PT6H").unwrap();
        assert_eq!(d.num_hours(), 6);
    }

    #[test]
    fn parse_p1dt12h() {
        let d = parse_iso_duration("P1DT12H").unwrap();
        assert_eq!(d.num_hours(), 36);
    }

    #[test]
    fn parse_pt30m() {
        let d = parse_iso_duration("PT30M").unwrap();
        assert_eq!(d.num_minutes(), 30);
    }

    #[test]
    fn parse_pt1h30m15s() {
        let d = parse_iso_duration("PT1H30M15S").unwrap();
        assert_eq!(d.num_seconds(), 5415);
    }

    #[test]
    fn format_roundtrip() {
        let d = chrono::Duration::seconds(5415);
        assert_eq!(format_iso_duration(&d), "PT1H30M15S");
    }

    #[test]
    fn sla_config_deserialize() {
        let yaml = r#"
freshness:
  max_age: PT6H
  warn_at: PT4H
  scope: last_successful_run
"#;
        let config: SlaConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.freshness.max_age, "PT6H");
        assert_eq!(config.freshness.warn_at.as_deref(), Some("PT4H"));
        assert_eq!(config.freshness.scope, SlaScope::LastSuccessfulRun);
    }

    #[test]
    fn sla_status_serialize() {
        assert_eq!(
            serde_json::to_string(&SlaStatus::Breach).unwrap(),
            "\"breach\""
        );
    }
}
