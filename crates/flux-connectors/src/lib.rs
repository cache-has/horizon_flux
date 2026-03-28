// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Concrete source and sink connector implementations for Horizon Flux.
//!
//! This crate provides:
//! - [`ConnectorConfig`]: Typed, serializable configuration for each connector
//! - [`ConnectorRegistry`]: Factory that creates connectors from config and
//!   populates a [`ProviderRegistry`]

pub mod config;
pub mod registry;

pub use config::ConnectorConfig;
pub use registry::ConnectorRegistry;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
