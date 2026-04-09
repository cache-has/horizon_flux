// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! ISO 8601 duration parsing and interval-based scheduling.
//!
//! Supports a practical subset of ISO 8601 durations: `P[nD][T[nH][nM][nS]]`.
//! Year and month durations are intentionally unsupported because they have
//! variable lengths that depend on calendar context. Users who need "every month"
//! should use a cron expression instead.

use crate::error::SchedulerError;
use chrono::{DateTime, Duration, Utc};

/// A parsed ISO 8601 duration that can be applied as a chrono `Duration`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Iso8601Duration {
    days: i64,
    hours: i64,
    minutes: i64,
    seconds: i64,
}

impl Iso8601Duration {
    /// Parse an ISO 8601 duration string like `PT30M`, `P1DT12H`, `PT1H30M`.
    ///
    /// Rejects year/month components (`P1Y`, `P2M`) — use cron for those.
    pub fn parse(input: &str) -> Result<Self, SchedulerError> {
        let s = input.trim();
        if !s.starts_with('P') {
            return Err(SchedulerError::InvalidInterval(format!(
                "must start with 'P': {input}"
            )));
        }

        let rest = &s[1..];
        if rest.is_empty() {
            return Err(SchedulerError::InvalidInterval(format!(
                "empty duration: {input}"
            )));
        }

        let (date_part, time_part) = match rest.find('T') {
            Some(pos) => (&rest[..pos], Some(&rest[pos + 1..])),
            None => (rest, None),
        };

        let mut days = 0i64;
        let mut hours = 0i64;
        let mut minutes = 0i64;
        let mut seconds = 0i64;

        // Parse date part (only D supported)
        if !date_part.is_empty() {
            if date_part.contains('Y') || date_part.contains('M') {
                return Err(SchedulerError::InvalidInterval(format!(
                    "year/month durations are not supported (use cron instead): {input}"
                )));
            }
            days = parse_component(date_part, 'D').map_err(|_| {
                SchedulerError::InvalidInterval(format!("invalid date component: {input}"))
            })?;
        }

        // Parse time part (H, M, S)
        if let Some(tp) = time_part {
            if tp.is_empty() {
                return Err(SchedulerError::InvalidInterval(format!(
                    "empty time component after T: {input}"
                )));
            }
            let mut remaining = tp;

            if let Some(pos) = remaining.find('H') {
                hours = remaining[..pos].parse().map_err(|_| {
                    SchedulerError::InvalidInterval(format!("invalid hours: {input}"))
                })?;
                remaining = &remaining[pos + 1..];
            }
            if let Some(pos) = remaining.find('M') {
                minutes = remaining[..pos].parse().map_err(|_| {
                    SchedulerError::InvalidInterval(format!("invalid minutes: {input}"))
                })?;
                remaining = &remaining[pos + 1..];
            }
            if let Some(pos) = remaining.find('S') {
                seconds = remaining[..pos].parse().map_err(|_| {
                    SchedulerError::InvalidInterval(format!("invalid seconds: {input}"))
                })?;
                remaining = &remaining[pos + 1..];
            }
            if !remaining.is_empty() {
                return Err(SchedulerError::InvalidInterval(format!(
                    "unexpected trailing characters in time part: {input}"
                )));
            }
        }

        if days == 0 && hours == 0 && minutes == 0 && seconds == 0 {
            return Err(SchedulerError::InvalidInterval(format!(
                "zero-length duration: {input}"
            )));
        }

        Ok(Self {
            days,
            hours,
            minutes,
            seconds,
        })
    }

    /// Convert to a chrono `Duration`.
    pub fn to_chrono_duration(&self) -> Duration {
        Duration::days(self.days)
            + Duration::hours(self.hours)
            + Duration::minutes(self.minutes)
            + Duration::seconds(self.seconds)
    }

    /// Compute the next fire time after `after`, given `start_at` as the
    /// interval anchor. The next fire is the smallest `start_at + n * interval`
    /// that is strictly after `after`.
    pub fn next_after(&self, start_at: DateTime<Utc>, after: DateTime<Utc>) -> DateTime<Utc> {
        let interval = self.to_chrono_duration();
        if interval.num_milliseconds() <= 0 {
            // Shouldn't happen since parse rejects zero, but be safe.
            return after + Duration::seconds(1);
        }

        if after < start_at {
            return start_at;
        }

        let elapsed = after - start_at;
        let interval_ms = interval.num_milliseconds();
        let elapsed_ms = elapsed.num_milliseconds();

        // Number of complete intervals that have passed
        let n = elapsed_ms / interval_ms;
        start_at + interval * (n as i32 + 1)
    }
}

/// Parse a single numeric component ending with the given suffix char.
/// E.g., parse_component("7D", 'D') -> Ok(7)
fn parse_component(s: &str, suffix: char) -> Result<i64, ()> {
    if s.ends_with(suffix) {
        s[..s.len() - 1].parse().map_err(|_| ())
    } else if s.chars().all(|c| c.is_ascii_digit()) {
        // No suffix found — this is fine for date part with no 'D'
        // but we shouldn't silently swallow it
        Err(())
    } else {
        Err(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn parse_pt30m() {
        let d = Iso8601Duration::parse("PT30M").unwrap();
        assert_eq!(d.to_chrono_duration(), Duration::minutes(30));
    }

    #[test]
    fn parse_pt1h() {
        let d = Iso8601Duration::parse("PT1H").unwrap();
        assert_eq!(d.to_chrono_duration(), Duration::hours(1));
    }

    #[test]
    fn parse_p1d() {
        let d = Iso8601Duration::parse("P1D").unwrap();
        assert_eq!(d.to_chrono_duration(), Duration::days(1));
    }

    #[test]
    fn parse_p1dt12h30m() {
        let d = Iso8601Duration::parse("P1DT12H30M").unwrap();
        assert_eq!(
            d.to_chrono_duration(),
            Duration::days(1) + Duration::hours(12) + Duration::minutes(30)
        );
    }

    #[test]
    fn parse_pt1h30m15s() {
        let d = Iso8601Duration::parse("PT1H30M15S").unwrap();
        assert_eq!(
            d.to_chrono_duration(),
            Duration::hours(1) + Duration::minutes(30) + Duration::seconds(15)
        );
    }

    #[test]
    fn reject_year_month() {
        assert!(Iso8601Duration::parse("P1Y").is_err());
        assert!(Iso8601Duration::parse("P2M").is_err());
        assert!(Iso8601Duration::parse("P1Y2M3D").is_err());
    }

    #[test]
    fn reject_zero_duration() {
        assert!(Iso8601Duration::parse("PT0S").is_err());
    }

    #[test]
    fn reject_empty() {
        assert!(Iso8601Duration::parse("P").is_err());
        assert!(Iso8601Duration::parse("PT").is_err());
    }

    #[test]
    fn next_after_basic() {
        let d = Iso8601Duration::parse("PT30M").unwrap();
        let start = Utc.with_ymd_and_hms(2026, 4, 9, 0, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 4, 9, 1, 15, 0).unwrap();
        let next = d.next_after(start, now);
        // 1:15 is between the 2nd (1:00) and 3rd (1:30) interval, so next is 1:30
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 4, 9, 1, 30, 0).unwrap());
    }

    #[test]
    fn next_after_before_start() {
        let d = Iso8601Duration::parse("PT1H").unwrap();
        let start = Utc.with_ymd_and_hms(2026, 4, 9, 10, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 4, 9, 8, 0, 0).unwrap();
        let next = d.next_after(start, now);
        assert_eq!(next, start);
    }
}
