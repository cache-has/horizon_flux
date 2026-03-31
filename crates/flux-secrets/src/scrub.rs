// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Runtime scrubbing of secret values from error messages and stack traces.
//!
//! When a connector fails after secret resolution (e.g. a PostgreSQL driver
//! error that includes the connection string), the error message may contain
//! plaintext secret values. This module provides utilities to redact those
//! values before the message reaches logs, the database, WebSocket consumers,
//! or API responses.

use regex::Regex;
use std::sync::LazyLock;

/// Regex matching `://user:password@` in URI-style connection strings.
static URI_PASSWORD_RE: LazyLock<Regex> = LazyLock::new(|| {
    // Captures: scheme://user:PASSWORD@
    // Group 1 = scheme + "://", Group 2 = user + ":", Group 3 = password, Group 4 = "@..."
    Regex::new(r"(\w+://)([^:@/]+:)([^@]+)(@)").expect("valid regex")
});

/// Regex matching `password=value` in key-value connection strings.
static KV_PASSWORD_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(password\s*=\s*)(\S+)").expect("valid regex"));

/// Scrub known secret values from arbitrary text.
///
/// Replaces each known value with `[REDACTED]`, longest first to avoid
/// partial matches (e.g. if one secret is a prefix of another). Also applies
/// connection-string password redaction as a defense-in-depth measure.
pub fn scrub_secrets(text: &str, known_values: &[String]) -> String {
    if text.is_empty() {
        return String::new();
    }

    let mut result = text.to_string();

    // Sort by length descending so longer secrets are replaced first.
    let mut sorted: Vec<&str> = known_values
        .iter()
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .collect();
    sorted.sort_by_key(|s| std::cmp::Reverse(s.len()));

    for value in sorted {
        result = result.replace(value, "[REDACTED]");
    }

    // Defense-in-depth: also redact connection string password patterns that
    // might not have been caught by value-based scrubbing.
    scrub_connection_strings_in_place(&mut result);

    result
}

/// Redact passwords from connection-string-like patterns embedded in
/// arbitrary text (error messages, stack traces, etc.).
///
/// Handles both URI-style (`postgresql://user:pass@host`) and key-value
/// style (`password=secret`) patterns.
pub fn scrub_connection_strings(text: &str) -> String {
    let mut result = text.to_string();
    scrub_connection_strings_in_place(&mut result);
    result
}

fn scrub_connection_strings_in_place(text: &mut String) {
    // URI style: scheme://user:PASSWORD@host
    let replaced = URI_PASSWORD_RE.replace_all(text, "${1}${2}[REDACTED]${4}");
    if replaced != *text {
        *text = replaced.into_owned();
    }

    // Key-value style: password=VALUE
    let replaced = KV_PASSWORD_RE.replace_all(text, "${1}[REDACTED]");
    if replaced != *text {
        *text = replaced.into_owned();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrub_known_values() {
        let text = "connection to host failed: postgresql://user:s3cret@localhost:5432/db";
        let known = vec!["s3cret".to_string()];
        let result = scrub_secrets(text, &known);
        assert!(
            !result.contains("s3cret"),
            "secret value should be redacted"
        );
        assert!(result.contains("[REDACTED]"));
    }

    #[test]
    fn scrub_multiple_values_longest_first() {
        let text = "key=abc123, token=abc";
        let known = vec!["abc".to_string(), "abc123".to_string()];
        let result = scrub_secrets(text, &known);
        assert!(!result.contains("abc123"));
        assert!(!result.contains("abc"));
        // "abc123" should be replaced as one unit, not partially
        assert!(result.contains("key=[REDACTED]"));
    }

    #[test]
    fn scrub_empty_values_ignored() {
        let text = "hello world";
        let known = vec!["".to_string(), "world".to_string()];
        let result = scrub_secrets(text, &known);
        assert_eq!(result, "hello [REDACTED]");
    }

    #[test]
    fn scrub_uri_password_in_error() {
        let text = "connection refused: postgresql://admin:hunter2@db.example.com:5432/prod";
        let result = scrub_connection_strings(text);
        assert!(!result.contains("hunter2"), "password should be redacted");
        assert!(result.contains("admin:[REDACTED]@db.example.com"));
    }

    #[test]
    fn scrub_kv_password_in_error() {
        let text = "failed to connect: host=localhost password=s3cret dbname=mydb";
        let result = scrub_connection_strings(text);
        assert!(!result.contains("s3cret"));
        assert!(result.contains("password=[REDACTED]"));
    }

    #[test]
    fn scrub_no_password_unchanged() {
        let text = "table not found: users";
        let result = scrub_connection_strings(text);
        assert_eq!(result, text);
    }

    #[test]
    fn scrub_empty_input() {
        assert_eq!(scrub_secrets("", &[]), "");
        assert_eq!(scrub_connection_strings(""), "");
    }

    #[test]
    fn scrub_combined_value_and_connection_string() {
        // Secret value already replaced, but connection string pattern also present
        let text = "error: mysql://root:mypass@localhost/db returned: access denied";
        let known = vec!["mypass".to_string()];
        let result = scrub_secrets(text, &known);
        assert!(!result.contains("mypass"));
    }
}
