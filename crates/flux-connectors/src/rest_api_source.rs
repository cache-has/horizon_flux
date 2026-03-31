// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! REST API source connector.
//!
//! Implements a DataFusion [`TableProvider`] that fetches data from HTTP
//! endpoints with support for authentication, pagination, response format
//! parsing (JSON, NDJSON, CSV), rate limiting, and retry logic.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, NullArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use flux_datafusion::provider::{ProviderError, SourceConnector};
use flux_engine::node::SourceConfig;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tracing::debug;

use crate::config::{PaginationConfig, ResponseFormat, RestApiAuth, RestApiConfig};

// ---------------------------------------------------------------------------
// SourceConnector implementation
// ---------------------------------------------------------------------------

/// Source connector for REST API endpoints.
///
/// Creates a [`RestApiTableProvider`] that fetches data from HTTP endpoints,
/// parses responses in various formats (JSON, NDJSON, CSV), supports
/// pagination, authentication, rate limiting, and retry logic.
pub struct RestApiSource;

impl RestApiSource {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RestApiSource {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceConnector for RestApiSource {
    fn create_table_provider(
        &self,
        config: &SourceConfig,
    ) -> Result<Arc<dyn TableProvider>, ProviderError> {
        let api_config: RestApiConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid rest_api source config: {e}"))?;

        if api_config.url.is_empty() {
            return Err("rest_api source requires a non-empty 'url'".into());
        }

        let method = api_config.method.to_uppercase();
        if !matches!(method.as_str(), "GET" | "POST" | "PUT" | "PATCH" | "DELETE") {
            return Err(format!("unsupported HTTP method: {method}").into());
        }

        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| "rest_api source requires a tokio runtime")?;

        let (schema, batches) =
            tokio::task::block_in_place(|| rt.block_on(fetch_all_data(&api_config)))?;

        debug!(
            rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            columns = schema.fields().len(),
            url = %api_config.url,
            "fetched rest api data"
        );

        let mem_table = MemTable::try_new(schema, vec![batches])
            .map_err(|e| format!("failed to create table from REST API data: {e}"))?;

        Ok(Arc::new(mem_table))
    }
}

// ---------------------------------------------------------------------------
// HTTP client helpers
// ---------------------------------------------------------------------------

/// Build a reqwest client with appropriate headers and auth.
fn build_client(config: &RestApiConfig) -> Result<reqwest::Client, ProviderError> {
    let mut headers = HeaderMap::new();

    for (key, value) in &config.headers {
        let name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|e| format!("invalid header name '{key}': {e}"))?;
        let val = HeaderValue::from_str(value)
            .map_err(|e| format!("invalid header value for '{key}': {e}"))?;
        headers.insert(name, val);
    }

    // Apply authentication headers.
    if let Some(auth) = &config.auth {
        match auth {
            RestApiAuth::Bearer { token } => {
                headers.insert(
                    reqwest::header::AUTHORIZATION,
                    HeaderValue::from_str(&format!("Bearer {token}"))
                        .map_err(|e| format!("invalid bearer token: {e}"))?,
                );
            }
            RestApiAuth::ApiKey { header, value } => {
                let name = HeaderName::from_bytes(header.as_bytes())
                    .map_err(|e| format!("invalid API key header '{header}': {e}"))?;
                let val = HeaderValue::from_str(value)
                    .map_err(|e| format!("invalid API key value: {e}"))?;
                headers.insert(name, val);
            }
            RestApiAuth::Basic { .. } => {
                // Basic auth is applied per-request via reqwest's .basic_auth()
            }
        }
    }

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| format!("failed to build HTTP client: {e}"))?;

    Ok(client)
}

/// Build a request for the given URL with method and optional basic auth.
fn build_request(
    client: &reqwest::Client,
    config: &RestApiConfig,
    url: &str,
) -> Result<reqwest::RequestBuilder, ProviderError> {
    let method: reqwest::Method = config
        .method
        .to_uppercase()
        .parse()
        .map_err(|e| format!("invalid HTTP method '{}': {e}", config.method))?;

    let mut req = client.request(method, url);

    if let Some(RestApiAuth::Basic { username, password }) = &config.auth {
        req = req.basic_auth(username, Some(password));
    }

    Ok(req)
}

/// Execute a request with retry logic.
async fn execute_with_retry(
    client: &reqwest::Client,
    config: &RestApiConfig,
    url: &str,
) -> Result<reqwest::Response, ProviderError> {
    let max_retries = config.max_retries.unwrap_or(3);

    for attempt in 0..=max_retries {
        let req = build_request(client, config, url)?;

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    return Ok(resp);
                }
                // Retry on 429 (rate limited) and 5xx (server error).
                if attempt < max_retries && (status == 429 || status.is_server_error()) {
                    let delay = Duration::from_millis(500 * 2u64.pow(attempt));
                    debug!(
                        status = %status,
                        attempt = attempt + 1,
                        delay_ms = delay.as_millis(),
                        "retrying request"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                let body = resp.text().await.unwrap_or_default();
                return Err(format!("HTTP {status} from {url}: {body}").into());
            }
            Err(e) => {
                if attempt < max_retries && (e.is_timeout() || e.is_connect()) {
                    let delay = Duration::from_millis(500 * 2u64.pow(attempt));
                    debug!(
                        error = %e,
                        attempt = attempt + 1,
                        delay_ms = delay.as_millis(),
                        "retrying request after error"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(format!("request to {url} failed: {e}").into());
            }
        }
    }

    unreachable!()
}

// ---------------------------------------------------------------------------
// Data fetching with pagination
// ---------------------------------------------------------------------------

/// Fetch all data from the REST API, handling pagination.
async fn fetch_all_data(
    config: &RestApiConfig,
) -> Result<(SchemaRef, Vec<RecordBatch>), ProviderError> {
    let client = build_client(config)?;
    let max_pages = config.max_pages.unwrap_or(100);
    let rate_limit = config.rate_limit_ms.map(Duration::from_millis);

    let mut all_rows: Vec<Vec<serde_json::Value>> = Vec::new();
    let mut page_count = 0;

    match &config.pagination {
        None => {
            // Single request, no pagination.
            let resp = execute_with_retry(&client, config, &config.url).await?;
            let rows = parse_response(resp, config).await?;
            all_rows.push(rows);
        }
        Some(PaginationConfig::Offset {
            offset_param,
            limit_param,
            limit,
        }) => {
            let mut offset: usize = 0;
            loop {
                if page_count >= max_pages {
                    debug!(max_pages, "reached maximum page limit");
                    break;
                }

                let url = append_query_params(
                    &config.url,
                    &[
                        (offset_param.as_str(), &offset.to_string()),
                        (limit_param.as_str(), &limit.to_string()),
                    ],
                );

                let resp = execute_with_retry(&client, config, &url).await?;
                let rows = parse_response(resp, config).await?;
                let count = rows.len();
                all_rows.push(rows);
                page_count += 1;

                if count < *limit {
                    break; // Last page.
                }
                offset += count;

                if let Some(delay) = rate_limit {
                    tokio::time::sleep(delay).await;
                }
            }
        }
        Some(PaginationConfig::Cursor {
            cursor_param,
            cursor_path,
        }) => {
            let mut next_cursor: Option<String> = None;
            loop {
                if page_count >= max_pages {
                    debug!(max_pages, "reached maximum page limit");
                    break;
                }

                let url = match &next_cursor {
                    Some(cursor) => {
                        append_query_params(&config.url, &[(cursor_param.as_str(), cursor)])
                    }
                    None => config.url.clone(),
                };

                let resp = execute_with_retry(&client, config, &url).await?;
                let body = resp
                    .text()
                    .await
                    .map_err(|e| format!("failed to read response body: {e}"))?;
                let json: serde_json::Value = serde_json::from_str(&body)
                    .map_err(|e| format!("failed to parse JSON response: {e}"))?;

                // Extract cursor for next page.
                next_cursor = extract_json_value(&json, cursor_path)
                    .and_then(|v| v.as_str().map(|s| s.to_string()));

                let rows = extract_data_rows(&json, config)?;
                all_rows.push(rows);
                page_count += 1;

                if next_cursor.is_none() {
                    break;
                }

                if let Some(delay) = rate_limit {
                    tokio::time::sleep(delay).await;
                }
            }
        }
        Some(PaginationConfig::LinkHeader) => {
            let mut url = config.url.clone();
            loop {
                if page_count >= max_pages {
                    debug!(max_pages, "reached maximum page limit");
                    break;
                }

                let resp = execute_with_retry(&client, config, &url).await?;
                let next_url = parse_link_header_next(resp.headers());
                let rows = parse_response(resp, config).await?;
                all_rows.push(rows);
                page_count += 1;

                match next_url {
                    Some(next) => url = next,
                    None => break,
                }

                if let Some(delay) = rate_limit {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    // Flatten all rows.
    let rows: Vec<serde_json::Value> = all_rows.into_iter().flatten().collect();

    if rows.is_empty() {
        // Return empty table with inferred or user-defined schema.
        let schema = if !config.schema.is_empty() {
            build_user_schema(&config.schema)?
        } else {
            Arc::new(Schema::empty())
        };
        let empty = RecordBatch::new_empty(schema.clone());
        return Ok((schema, vec![empty]));
    }

    // Build schema (user-defined or inferred from first row).
    let schema = if !config.schema.is_empty() {
        build_user_schema(&config.schema)?
    } else {
        infer_schema_from_json(&rows)
    };

    let batch = json_rows_to_batch(&rows, &schema)?;
    Ok((schema, vec![batch]))
}

// ---------------------------------------------------------------------------
// Response parsing
// ---------------------------------------------------------------------------

/// Parse an HTTP response into a list of JSON value rows.
async fn parse_response(
    resp: reqwest::Response,
    config: &RestApiConfig,
) -> Result<Vec<serde_json::Value>, ProviderError> {
    match config.response_format {
        ResponseFormat::Json => {
            let body = resp
                .text()
                .await
                .map_err(|e| format!("failed to read response body: {e}"))?;
            let json: serde_json::Value = serde_json::from_str(&body)
                .map_err(|e| format!("failed to parse JSON response: {e}"))?;
            extract_data_rows(&json, config)
        }
        ResponseFormat::Ndjson => {
            let body = resp
                .text()
                .await
                .map_err(|e| format!("failed to read response body: {e}"))?;
            let mut rows = Vec::new();
            for line in body.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let value: serde_json::Value = serde_json::from_str(line)
                    .map_err(|e| format!("failed to parse NDJSON line: {e}"))?;
                rows.push(value);
            }
            Ok(rows)
        }
        ResponseFormat::Csv => {
            let body = resp
                .text()
                .await
                .map_err(|e| format!("failed to read response body: {e}"))?;
            csv_text_to_json_rows(&body)
        }
    }
}

/// Extract the data array from a JSON response using the configured data_path.
fn extract_data_rows(
    json: &serde_json::Value,
    config: &RestApiConfig,
) -> Result<Vec<serde_json::Value>, ProviderError> {
    let data = match &config.data_path {
        Some(path) => extract_json_value(json, path)
            .ok_or_else(|| format!("data_path '{}' not found in response", path))?,
        None => json,
    };

    match data {
        serde_json::Value::Array(arr) => Ok(arr.clone()),
        obj @ serde_json::Value::Object(_) => Ok(vec![obj.clone()]),
        other => Err(format!(
            "expected array or object at data path, got {}",
            json_type_name(other)
        )
        .into()),
    }
}

/// Extract a value from JSON using dot-notation (`data.items`) or JSON
/// Pointer (`/data/items`).
fn extract_json_value<'a>(
    json: &'a serde_json::Value,
    path: &str,
) -> Option<&'a serde_json::Value> {
    // Try JSON Pointer first (starts with /).
    if path.starts_with('/') {
        return json.pointer(path);
    }
    // Otherwise use dot-notation.
    let mut current = json;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

/// Parse a CSV text body into JSON value rows (objects with header keys).
fn csv_text_to_json_rows(text: &str) -> Result<Vec<serde_json::Value>, ProviderError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(text.as_bytes());

    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| format!("failed to read CSV headers: {e}"))?
        .iter()
        .map(|h| h.to_string())
        .collect();

    let mut rows = Vec::new();
    for result in reader.records() {
        let record = result.map_err(|e| format!("failed to read CSV record: {e}"))?;
        let mut obj = serde_json::Map::new();
        for (i, field) in record.iter().enumerate() {
            let key = headers
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col_{i}"));
            // Try to parse as number or bool, fall back to string.
            let value = if let Ok(n) = field.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else if let Ok(n) = field.parse::<f64>() {
                serde_json::Number::from_f64(n)
                    .map(serde_json::Value::Number)
                    .unwrap_or_else(|| serde_json::Value::String(field.to_string()))
            } else if field.eq_ignore_ascii_case("true") {
                serde_json::Value::Bool(true)
            } else if field.eq_ignore_ascii_case("false") {
                serde_json::Value::Bool(false)
            } else if field.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(field.to_string())
            };
            obj.insert(key, value);
        }
        rows.push(serde_json::Value::Object(obj));
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Pagination helpers
// ---------------------------------------------------------------------------

/// Append query parameters to a URL.
fn append_query_params(url: &str, params: &[(&str, &str)]) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    let query: Vec<String> = params.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{url}{separator}{}", query.join("&"))
}

/// Parse the Link header to find the `rel="next"` URL.
fn parse_link_header_next(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let link = headers.get("link")?.to_str().ok()?;
    for part in link.split(',') {
        let part = part.trim();
        if part.contains("rel=\"next\"") || part.contains("rel='next'") {
            // Extract URL between < and >.
            let start = part.find('<')? + 1;
            let end = part.find('>')?;
            return Some(part[start..end].to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Schema inference
// ---------------------------------------------------------------------------

/// Infer an Arrow schema from JSON rows by examining all rows.
fn infer_schema_from_json(rows: &[serde_json::Value]) -> SchemaRef {
    let mut field_types: HashMap<String, DataType> = HashMap::new();
    let mut field_order: Vec<String> = Vec::new();

    for row in rows {
        if let serde_json::Value::Object(obj) = row {
            for (key, value) in obj {
                let inferred = json_value_to_arrow_type(value);
                let entry = field_types.entry(key.clone()).or_insert(DataType::Null);
                if !field_order.contains(key) {
                    field_order.push(key.clone());
                }
                // Promote Null to a concrete type if we see one.
                if *entry == DataType::Null && inferred != DataType::Null {
                    *entry = inferred;
                }
            }
        }
    }

    let fields: Vec<Field> = field_order
        .iter()
        .map(|name| {
            let dt = field_types.get(name).cloned().unwrap_or(DataType::Utf8);
            // Default Null fields to Utf8.
            let dt = if dt == DataType::Null {
                DataType::Utf8
            } else {
                dt
            };
            Field::new(name, dt, true)
        })
        .collect();

    Arc::new(Schema::new(fields))
}

/// Map a JSON value to an Arrow data type.
fn json_value_to_arrow_type(value: &serde_json::Value) -> DataType {
    match value {
        serde_json::Value::Null => DataType::Null,
        serde_json::Value::Bool(_) => DataType::Boolean,
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        serde_json::Value::String(_) => DataType::Utf8,
        // Nested objects/arrays are serialized as JSON strings.
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => DataType::Utf8,
    }
}

/// Build a schema from user-defined field name → type string mapping.
fn build_user_schema(schema_map: &HashMap<String, String>) -> Result<SchemaRef, ProviderError> {
    let mut fields = Vec::with_capacity(schema_map.len());
    // Sort by key for deterministic ordering.
    let mut entries: Vec<_> = schema_map.iter().collect();
    entries.sort_by_key(|(k, _)| k.as_str());

    for (name, type_str) in entries {
        let dt = parse_type_string(type_str)
            .ok_or_else(|| format!("unsupported schema type '{type_str}' for field '{name}'"))?;
        fields.push(Field::new(name, dt, true));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Parse a user-friendly type string to an Arrow DataType.
fn parse_type_string(s: &str) -> Option<DataType> {
    match s.to_lowercase().as_str() {
        "bool" | "boolean" => Some(DataType::Boolean),
        "int8" => Some(DataType::Int8),
        "int16" => Some(DataType::Int16),
        "int32" | "int" | "integer" => Some(DataType::Int32),
        "int64" | "bigint" | "long" => Some(DataType::Int64),
        "float32" | "float" => Some(DataType::Float32),
        "float64" | "double" => Some(DataType::Float64),
        "utf8" | "string" | "text" => Some(DataType::Utf8),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// JSON → Arrow conversion
// ---------------------------------------------------------------------------

/// Convert JSON rows to an Arrow RecordBatch.
fn json_rows_to_batch(
    rows: &[serde_json::Value],
    schema: &SchemaRef,
) -> Result<RecordBatch, ProviderError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let array = build_column_from_json(rows, field.name(), field.data_type())?;
        columns.push(array);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| format!("failed to create record batch: {e}").into())
}

/// Build an Arrow array for one field from JSON rows.
fn build_column_from_json(
    rows: &[serde_json::Value],
    field_name: &str,
    data_type: &DataType,
) -> Result<ArrayRef, ProviderError> {
    match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get(field_name) {
                    Some(serde_json::Value::Bool(b)) => builder.append_value(*b),
                    Some(serde_json::Value::Null) | None => builder.append_null(),
                    Some(other) => {
                        // Try to coerce string "true"/"false".
                        if let Some(s) = other.as_str() {
                            match s.to_lowercase().as_str() {
                                "true" | "1" => builder.append_value(true),
                                "false" | "0" => builder.append_value(false),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row.get(field_name) {
                    Some(serde_json::Value::Number(n)) => {
                        if let Some(i) = n.as_i64() {
                            builder.append_value(i);
                        } else if let Some(f) = n.as_f64() {
                            builder.append_value(f as i64);
                        } else {
                            builder.append_null();
                        }
                    }
                    Some(serde_json::Value::String(s)) => {
                        if let Ok(i) = s.parse::<i64>() {
                            builder.append_value(i);
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match row.get(field_name) {
                    Some(serde_json::Value::Number(n)) => {
                        if let Some(f) = n.as_f64() {
                            builder.append_value(f);
                        } else {
                            builder.append_null();
                        }
                    }
                    Some(serde_json::Value::String(s)) => {
                        if let Ok(f) = s.parse::<f64>() {
                            builder.append_value(f);
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match row.get(field_name) {
                    Some(serde_json::Value::String(s)) => builder.append_value(s),
                    Some(serde_json::Value::Null) | None => builder.append_null(),
                    Some(other) => {
                        // Serialize non-string values (numbers, bools, objects, arrays) to string.
                        builder.append_value(other.to_string());
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Null => Ok(Arc::new(NullArray::new(rows.len()))),
        _ => {
            // Fallback: read as string.
            let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match row.get(field_name) {
                    Some(serde_json::Value::Null) | None => builder.append_null(),
                    Some(v) => builder.append_value(v.to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

/// Return the JSON type name for error messages.
fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Config validation --

    #[test]
    fn rest_api_source_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RestApiSource>();
    }

    #[test]
    fn rejects_empty_url() {
        let source = RestApiSource::new();
        let config = SourceConfig {
            connector: "rest_api".to_string(),
            config: serde_json::json!({ "url": "" }),
            cache_row_limit: None,
        };
        let result = source.create_table_provider(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-empty"));
    }

    #[test]
    fn rejects_invalid_method() {
        let source = RestApiSource::new();
        let config = SourceConfig {
            connector: "rest_api".to_string(),
            config: serde_json::json!({
                "url": "http://example.com/api",
                "method": "INVALID"
            }),
            cache_row_limit: None,
        };
        let result = source.create_table_provider(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unsupported"));
    }

    // -- JSON extraction --

    #[test]
    fn extract_dot_notation() {
        let json: serde_json::Value = serde_json::json!({
            "data": { "items": [1, 2, 3] }
        });
        let result = extract_json_value(&json, "data.items").unwrap();
        assert_eq!(result, &serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn extract_json_pointer() {
        let json: serde_json::Value = serde_json::json!({
            "data": { "items": [1, 2, 3] }
        });
        let result = extract_json_value(&json, "/data/items").unwrap();
        assert_eq!(result, &serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn extract_missing_path_returns_none() {
        let json: serde_json::Value = serde_json::json!({ "data": {} });
        assert!(extract_json_value(&json, "data.items").is_none());
    }

    // -- Schema inference --

    #[test]
    fn infer_schema_from_rows() {
        let rows = vec![
            serde_json::json!({"id": 1, "name": "Alice", "active": true}),
            serde_json::json!({"id": 2, "name": "Bob", "active": false}),
        ];
        let schema = infer_schema_from_json(&rows);
        assert_eq!(schema.fields().len(), 3);

        let id_field = schema.field_with_name("id").unwrap();
        assert_eq!(id_field.data_type(), &DataType::Int64);

        let name_field = schema.field_with_name("name").unwrap();
        assert_eq!(name_field.data_type(), &DataType::Utf8);

        let active_field = schema.field_with_name("active").unwrap();
        assert_eq!(active_field.data_type(), &DataType::Boolean);
    }

    #[test]
    fn infer_schema_promotes_null() {
        let rows = vec![
            serde_json::json!({"id": null, "name": "Alice"}),
            serde_json::json!({"id": 42, "name": "Bob"}),
        ];
        let schema = infer_schema_from_json(&rows);
        let id_field = schema.field_with_name("id").unwrap();
        assert_eq!(id_field.data_type(), &DataType::Int64);
    }

    #[test]
    fn infer_schema_nested_objects_become_utf8() {
        let rows = vec![serde_json::json!({"meta": {"key": "val"}})];
        let schema = infer_schema_from_json(&rows);
        let meta_field = schema.field_with_name("meta").unwrap();
        assert_eq!(meta_field.data_type(), &DataType::Utf8);
    }

    // -- User-defined schema --

    #[test]
    fn build_user_schema_valid() {
        let mut map = HashMap::new();
        map.insert("id".to_string(), "int64".to_string());
        map.insert("name".to_string(), "string".to_string());
        let schema = build_user_schema(&map).unwrap();
        assert_eq!(schema.fields().len(), 2);
    }

    #[test]
    fn build_user_schema_invalid_type() {
        let mut map = HashMap::new();
        map.insert("x".to_string(), "timestamp".to_string());
        assert!(build_user_schema(&map).is_err());
    }

    // -- JSON → Arrow conversion --

    #[test]
    fn json_rows_to_batch_basic() {
        let rows = vec![
            serde_json::json!({"id": 1, "name": "Alice"}),
            serde_json::json!({"id": 2, "name": "Bob"}),
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = json_rows_to_batch(&rows, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn json_rows_handles_missing_fields() {
        let rows = vec![
            serde_json::json!({"id": 1}),
            serde_json::json!({"id": 2, "name": "Bob"}),
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = json_rows_to_batch(&rows, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    // -- Link header parsing --

    #[test]
    fn parse_link_header() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static(
                r#"<https://api.example.com/items?page=2>; rel="next", <https://api.example.com/items?page=5>; rel="last""#,
            ),
        );
        let next = parse_link_header_next(&headers);
        assert_eq!(
            next.as_deref(),
            Some("https://api.example.com/items?page=2")
        );
    }

    #[test]
    fn parse_link_header_no_next() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static(r#"<https://api.example.com/items?page=5>; rel="last""#),
        );
        assert!(parse_link_header_next(&headers).is_none());
    }

    // -- URL helper --

    #[test]
    fn append_query_params_no_existing() {
        let url = append_query_params("https://api.example.com/items", &[("page", "2")]);
        assert_eq!(url, "https://api.example.com/items?page=2");
    }

    #[test]
    fn append_query_params_existing() {
        let url = append_query_params("https://api.example.com/items?foo=bar", &[("page", "2")]);
        assert_eq!(url, "https://api.example.com/items?foo=bar&page=2");
    }

    // -- Data extraction --

    #[test]
    fn extract_data_rows_array() {
        let json = serde_json::json!({
            "results": [{"id": 1}, {"id": 2}]
        });
        let config = RestApiConfig {
            url: String::new(),
            method: "GET".to_string(),
            headers: HashMap::new(),
            auth: None,
            response_format: ResponseFormat::Json,
            data_path: Some("results".to_string()),
            pagination: None,
            schema: HashMap::new(),
            rate_limit_ms: None,
            max_retries: None,
            max_pages: None,
        };
        let rows = extract_data_rows(&json, &config).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn extract_data_rows_single_object() {
        let json = serde_json::json!({"id": 1, "name": "Alice"});
        let config = RestApiConfig {
            url: String::new(),
            method: "GET".to_string(),
            headers: HashMap::new(),
            auth: None,
            response_format: ResponseFormat::Json,
            data_path: None,
            pagination: None,
            schema: HashMap::new(),
            rate_limit_ms: None,
            max_retries: None,
            max_pages: None,
        };
        let rows = extract_data_rows(&json, &config).unwrap();
        assert_eq!(rows.len(), 1);
    }

    // -- CSV parsing --

    #[test]
    fn csv_text_to_json_rows_basic() {
        let csv = "id,name,score\n1,Alice,95.5\n2,Bob,87.0\n";
        let rows = csv_text_to_json_rows(csv).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], serde_json::json!(1));
        assert_eq!(rows[0]["name"], serde_json::json!("Alice"));
        assert_eq!(rows[1]["name"], serde_json::json!("Bob"));
    }

    // -- Type string parsing --

    #[test]
    fn parse_type_string_variants() {
        assert_eq!(parse_type_string("int64"), Some(DataType::Int64));
        assert_eq!(parse_type_string("string"), Some(DataType::Utf8));
        assert_eq!(parse_type_string("boolean"), Some(DataType::Boolean));
        assert_eq!(parse_type_string("float64"), Some(DataType::Float64));
        assert_eq!(parse_type_string("BIGINT"), Some(DataType::Int64));
        assert!(parse_type_string("unknown_type").is_none());
    }
}
