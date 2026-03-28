// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Sample configuration for preview execution.

use serde::{Deserialize, Serialize};

/// How to sample source data for preview execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum SampleConfig {
    /// Take the first N rows from each source.
    FirstN { count: usize },
    /// Take a reproducible random sample.
    Random { count: usize, seed: u64 },
    /// Use the full dataset (no sampling).
    Full,
}

impl Default for SampleConfig {
    fn default() -> Self {
        Self::FirstN { count: 100 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_first_100() {
        assert!(matches!(SampleConfig::default(), SampleConfig::FirstN { count: 100 }));
    }

    #[test]
    fn serde_roundtrip() {
        let configs = vec![
            SampleConfig::FirstN { count: 50 },
            SampleConfig::Random { count: 200, seed: 42 },
            SampleConfig::Full,
        ];
        for cfg in configs {
            let json = serde_json::to_string(&cfg).unwrap();
            let back: SampleConfig = serde_json::from_str(&json).unwrap();
            assert_eq!(format!("{:?}", cfg), format!("{:?}", back));
        }
    }
}
