// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Cron expression parsing and next-fire-time calculation.

use crate::error::SchedulerError;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use std::str::FromStr;

/// Parsed cron schedule that can compute next fire times.
#[derive(Debug, Clone)]
pub struct CronSchedule {
    cron: Cron,
    timezone: Tz,
}

impl CronSchedule {
    /// Parse a standard 5-field cron expression with a timezone name.
    ///
    /// ```text
    /// ┌───────────── minute (0–59)
    /// │ ┌───────────── hour (0–23)
    /// │ │ ┌───────────── day of month (1–31)
    /// │ │ │ ┌───────────── month (1–12)
    /// │ │ │ │ ┌───────────── day of week (0–6, Sun=0)
    /// │ │ │ │ │
    /// * * * * *
    /// ```
    pub fn parse(expression: &str, timezone: &str) -> Result<Self, SchedulerError> {
        let tz: Tz = timezone
            .parse()
            .map_err(|_| SchedulerError::InvalidCron(format!("invalid timezone: {timezone}")))?;

        let cron = Cron::from_str(expression)
            .map_err(|e| SchedulerError::InvalidCron(format!("{expression}: {e}")))?;

        Ok(Self { cron, timezone: tz })
    }

    /// Compute the next fire time strictly after `after` (UTC).
    ///
    /// Returns `None` if the cron expression has no future occurrences (shouldn't
    /// happen for standard patterns, but is theoretically possible for very
    /// constrained expressions like `0 0 30 2 *`).
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let local_after = after.with_timezone(&self.timezone);
        // find_next_occurrence returns the next matching time strictly after the input
        self.cron
            .find_next_occurrence(&local_after, false)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn every_hour_next_fire() {
        let sched = CronSchedule::parse("0 * * * *", "UTC").unwrap();
        let now = Utc.with_ymd_and_hms(2026, 4, 9, 10, 30, 0).unwrap();
        let next = sched.next_after(now).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 4, 9, 11, 0, 0).unwrap());
    }

    #[test]
    fn every_6_hours() {
        let sched = CronSchedule::parse("0 */6 * * *", "UTC").unwrap();
        let now = Utc.with_ymd_and_hms(2026, 4, 9, 5, 0, 0).unwrap();
        let next = sched.next_after(now).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 4, 9, 6, 0, 0).unwrap());
    }

    #[test]
    fn timezone_aware() {
        // "0 9 * * *" in America/Chicago (UTC-5 or UTC-6 depending on DST)
        let sched = CronSchedule::parse("0 9 * * *", "America/Chicago").unwrap();
        // April 9 2026 at 13:00 UTC = 8:00 AM CDT (UTC-5 in April)
        let now = Utc.with_ymd_and_hms(2026, 4, 9, 13, 0, 0).unwrap();
        let next = sched.next_after(now).unwrap();
        // Next 9:00 AM CDT = 14:00 UTC
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 4, 9, 14, 0, 0).unwrap());
    }

    #[test]
    fn invalid_expression_rejected() {
        let result = CronSchedule::parse("not a cron", "UTC");
        assert!(result.is_err());
    }

    #[test]
    fn invalid_timezone_rejected() {
        let result = CronSchedule::parse("0 * * * *", "Mars/Olympus_Mons");
        assert!(result.is_err());
    }
}
