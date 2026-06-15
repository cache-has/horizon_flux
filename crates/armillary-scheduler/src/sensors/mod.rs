// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Sensor implementations for event-driven triggers.
//!
//! Sensors detect external conditions (new files, webhook requests, pipeline
//! completions) and translate them into trigger firings.

pub mod file_arrival;
