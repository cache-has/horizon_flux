// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Resolve `{{ secret:name }}` references in configuration strings.
//!
//! At pipeline execution time, connector config values that contain
//! `{{ secret:name }}` patterns are resolved against the secret store
//! using the active environment for scoped lookup.

use crate::error::SecretError;
use crate::store::SecretStore;

/// Resolve all `{{ secret:... }}` references in a string.
///
/// Each `{{ secret:name }}` is replaced with the decrypted secret value
/// (UTF-8). Resolution uses environment fallback: environment-specific
/// first, then default (unscoped).
pub fn resolve_secrets(
    input: &str,
    store: &SecretStore,
    environment: Option<&str>,
) -> Result<String, SecretError> {
    let mut result = String::with_capacity(input.len());
    let mut remaining = input;

    while let Some(start) = remaining.find("{{") {
        result.push_str(&remaining[..start]);

        let after_open = &remaining[start + 2..];
        let Some(end) = after_open.find("}}") else {
            // No closing braces — treat as literal text.
            result.push_str(&remaining[start..]);
            return Ok(result);
        };

        let inner = after_open[..end].trim();

        if let Some(secret_name) = inner.strip_prefix("secret:") {
            let secret_name = secret_name.trim();
            if secret_name.is_empty() {
                return Err(SecretError::InvalidReference(
                    "empty secret name in {{ secret: }}".to_string(),
                ));
            }

            let value = store.resolve(secret_name, environment)?;
            let value_str = String::from_utf8(value).map_err(|_| {
                SecretError::Decryption(format!("secret '{secret_name}' is not valid UTF-8"))
            })?;
            result.push_str(&value_str);
        } else {
            // Not a secret reference — preserve as-is (could be a variable).
            result.push_str(&remaining[start..start + 2 + end + 2]);
        }

        remaining = &after_open[end + 2..];
    }

    result.push_str(remaining);
    Ok(result)
}

/// Check whether a string contains any `{{ secret:... }}` references.
pub fn has_secret_refs(input: &str) -> bool {
    let mut remaining = input;
    while let Some(start) = remaining.find("{{") {
        let after = &remaining[start + 2..];
        if let Some(end) = after.find("}}") {
            let inner = after[..end].trim();
            if inner.starts_with("secret:") {
                return true;
            }
            remaining = &after[end + 2..];
        } else {
            break;
        }
    }
    false
}

/// Resolve all `{{ secret:... }}` references in a JSON value (recursively).
///
/// Only string values are resolved; other types are returned as-is.
pub fn resolve_json_secrets(
    value: &serde_json::Value,
    store: &SecretStore,
    environment: Option<&str>,
) -> Result<serde_json::Value, SecretError> {
    match value {
        serde_json::Value::String(s) => {
            if has_secret_refs(s) {
                Ok(serde_json::Value::String(resolve_secrets(
                    s,
                    store,
                    environment,
                )?))
            } else {
                Ok(value.clone())
            }
        }
        serde_json::Value::Object(map) => {
            let mut resolved = serde_json::Map::new();
            for (k, v) in map {
                resolved.insert(k.clone(), resolve_json_secrets(v, store, environment)?);
            }
            Ok(serde_json::Value::Object(resolved))
        }
        serde_json::Value::Array(arr) => {
            let resolved: Result<Vec<_>, _> = arr
                .iter()
                .map(|v| resolve_json_secrets(v, store, environment))
                .collect();
            Ok(serde_json::Value::Array(resolved?))
        }
        _ => Ok(value.clone()),
    }
}
