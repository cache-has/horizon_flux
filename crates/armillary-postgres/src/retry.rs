// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Retry logic for transient PostgreSQL connection failures.
//!
//! Only connection *acquisition* (`pool.get()`) is retried — not query
//! execution. This avoids double-executing non-idempotent writes while
//! covering the most common transient failure (stale/dropped connections,
//! pool exhaustion under load).

use deadpool_postgres::Pool;
use std::time::Duration;

/// Maximum number of retry attempts after the initial failure.
const MAX_RETRIES: u32 = 3;

/// Initial backoff duration. Doubled on each retry.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Upper bound on backoff to prevent excessive waits.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Acquire a client from the pool, retrying on transient failures.
///
/// Uses exponential backoff: 100ms → 200ms → 400ms (capped at 5s).
/// Returns the last error if all attempts are exhausted.
pub(crate) async fn get_client(
    pool: &Pool,
) -> Result<deadpool_postgres::Client, deadpool_postgres::PoolError> {
    let mut last_err = None;
    let mut backoff = INITIAL_BACKOFF;

    for attempt in 0..=MAX_RETRIES {
        match pool.get().await {
            Ok(client) => return Ok(client),
            Err(e) => {
                if attempt < MAX_RETRIES {
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_retries = MAX_RETRIES,
                        backoff_ms = backoff.as_millis() as u64,
                        "transient pool error, retrying: {e}"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
                last_err = Some(e);
            }
        }
    }

    Err(last_err.expect("loop ran at least once"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_caps_at_max() {
        let mut b = INITIAL_BACKOFF;
        for _ in 0..20 {
            b = (b * 2).min(MAX_BACKOFF);
        }
        assert!(b <= MAX_BACKOFF);
    }

    #[tokio::test]
    async fn retries_on_bad_pool() {
        // A pool pointed at a non-existent host will fail to get a client.
        // Verify that get_client retries and eventually returns an error
        // (not a panic).
        let pool = crate::create_pool("postgresql://localhost:19999/nonexistent")
            .expect("pool creation succeeds (lazy connect)");

        let start = std::time::Instant::now();
        let result = get_client(&pool).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        // With 3 retries and exponential backoff (100+200+400=700ms minimum),
        // it should take at least a few hundred milliseconds.
        assert!(
            elapsed >= Duration::from_millis(300),
            "expected retries with backoff, but elapsed was {elapsed:?}"
        );
    }
}
