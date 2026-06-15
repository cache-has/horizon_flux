// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Stdout sink connector for debugging and CLI usage.
//!
//! Writes Arrow record batches to standard output in configurable formats:
//! table (pretty-printed, psql-style), CSV, JSON, or NDJSON.

use std::time::Instant;

use armillary_datafusion::provider::{
    MaterializationContext, MaterializationReceipt, PipelineSink, ProviderError, WriteOptions,
    WriteStats,
};
use armillary_engine::node::SinkConfig;
use arrow::record_batch::RecordBatch;
use arrow::util::display::ArrayFormatter;
use async_trait::async_trait;

use crate::config::{StdoutConfig, StdoutFormat};

/// Sink connector that writes record batches to stdout.
pub struct StdoutSink;

impl StdoutSink {
    pub fn new() -> Self {
        Self
    }
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PipelineSink for StdoutSink {
    async fn write(
        &self,
        config: &SinkConfig,
        data: Vec<RecordBatch>,
        _options: &WriteOptions,
        ctx: &MaterializationContext,
    ) -> Result<MaterializationReceipt, ProviderError> {
        let start = Instant::now();

        let stdout_config: StdoutConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid stdout sink config: {e}"))?;

        if data.is_empty() {
            let stats = WriteStats {
                rows_written: 0,
                bytes_written: 0,
                duration: start.elapsed(),
            };
            return Ok(MaterializationReceipt::from_write_stats(&stats, ctx));
        }

        let max_rows = stdout_config.max_rows;
        let mut rows_written: u64 = 0;
        let mut bytes_written: u64 = 0;
        let mut rows_remaining = max_rows;

        for batch in &data {
            let batch = if let Some(remaining) = rows_remaining {
                if remaining == 0 {
                    break;
                }
                let take = remaining.min(batch.num_rows());
                rows_remaining = Some(remaining - take);
                batch.slice(0, take)
            } else {
                batch.clone()
            };

            let output = match stdout_config.format {
                StdoutFormat::Table => format_table(&batch)?,
                StdoutFormat::Csv => format_csv(&batch, rows_written == 0)?,
                StdoutFormat::Json => format_json(&batch)?,
                StdoutFormat::Ndjson => format_ndjson(&batch)?,
            };

            bytes_written += output.len() as u64;
            print!("{output}");
            rows_written += batch.num_rows() as u64;
        }

        // Print a row count footer for table format.
        if matches!(stdout_config.format, StdoutFormat::Table) {
            let footer = if rows_written == 1 {
                "(1 row)\n".to_string()
            } else {
                format!("({rows_written} rows)\n")
            };
            bytes_written += footer.len() as u64;
            print!("{footer}");
        }

        let stats = WriteStats {
            rows_written,
            bytes_written,
            duration: start.elapsed(),
        };
        Ok(MaterializationReceipt::from_write_stats(&stats, ctx))
    }

    fn validate_config(&self, config: &SinkConfig) -> Result<(), ProviderError> {
        let _stdout_config: StdoutConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| format!("invalid stdout sink config: {e}"))?;
        Ok(())
    }
}

/// Format a record batch as a pretty-printed table (psql-style).
fn format_table(batch: &RecordBatch) -> Result<String, ProviderError> {
    let schema = batch.schema();
    let num_cols = schema.fields().len();
    let num_rows = batch.num_rows();

    if num_cols == 0 {
        return Ok(String::new());
    }

    // Build formatters for each column.
    let formatters: Vec<ArrayFormatter> = (0..num_cols)
        .map(|i| {
            ArrayFormatter::try_new(batch.column(i).as_ref(), &Default::default())
                .map_err(|e| -> ProviderError { format!("failed to create formatter: {e}").into() })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Compute column widths: max of header length and formatted value lengths.
    let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();

    for row in 0..num_rows {
        for (col, formatter) in formatters.iter().enumerate() {
            let val_len = formatter.value(row).to_string().len();
            if val_len > widths[col] {
                widths[col] = val_len;
            }
        }
    }

    let mut out = String::new();

    // Header row.
    for (i, header) in headers.iter().enumerate() {
        if i > 0 {
            out.push_str(" | ");
        }
        out.push_str(&format!("{:width$}", header, width = widths[i]));
    }
    out.push('\n');

    // Separator.
    for (i, w) in widths.iter().enumerate() {
        if i > 0 {
            out.push_str("-+-");
        }
        out.push_str(&"-".repeat(*w));
    }
    out.push('\n');

    // Data rows.
    for row in 0..num_rows {
        for (col, formatter) in formatters.iter().enumerate() {
            if col > 0 {
                out.push_str(" | ");
            }
            let val = formatter.value(row).to_string();
            out.push_str(&format!("{:width$}", val, width = widths[col]));
        }
        out.push('\n');
    }

    Ok(out)
}

/// Format a record batch as CSV.
fn format_csv(batch: &RecordBatch, include_header: bool) -> Result<String, ProviderError> {
    let mut buf = Vec::new();
    {
        let builder = arrow::csv::WriterBuilder::new().with_header(include_header);
        let mut writer = builder.build(&mut buf);
        writer
            .write(batch)
            .map_err(|e| -> ProviderError { format!("failed to write CSV: {e}").into() })?;
    }
    String::from_utf8(buf)
        .map_err(|e| -> ProviderError { format!("invalid UTF-8 in CSV: {e}").into() })
}

/// Format a record batch as a JSON array.
fn format_json(batch: &RecordBatch) -> Result<String, ProviderError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
        writer
            .write(batch)
            .map_err(|e| -> ProviderError { format!("failed to write JSON: {e}").into() })?;
        writer
            .finish()
            .map_err(|e| -> ProviderError { format!("failed to finish JSON: {e}").into() })?;
    }
    String::from_utf8(buf)
        .map_err(|e| -> ProviderError { format!("invalid UTF-8 in JSON: {e}").into() })
}

/// Format a record batch as newline-delimited JSON.
fn format_ndjson(batch: &RecordBatch) -> Result<String, ProviderError> {
    // arrow::json::LineDelimitedWriter already outputs NDJSON (one JSON object per line).
    format_json(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
                Arc::new(Float64Array::from(vec![95.5, 87.0, 92.3])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn stdout_sink_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StdoutSink>();
    }

    #[test]
    fn table_format_output() {
        let batch = test_batch();
        let output = format_table(&batch).unwrap();
        assert!(output.contains("id"));
        assert!(output.contains("name"));
        assert!(output.contains("score"));
        assert!(output.contains("alice"));
        assert!(output.contains("charlie"));
        // Check separator line exists.
        assert!(output.contains("-+-"));
    }

    #[test]
    fn csv_format_output() {
        let batch = test_batch();
        let output = format_csv(&batch, true).unwrap();
        assert!(output.starts_with("id,name,score"));
        assert!(output.contains("alice"));
    }

    #[test]
    fn json_format_output() {
        let batch = test_batch();
        let output = format_json(&batch).unwrap();
        assert!(output.contains("\"id\""));
        assert!(output.contains("\"alice\""));
    }

    #[tokio::test]
    async fn write_with_max_rows() {
        let sink = StdoutSink::new();
        let config = SinkConfig {
            connector: "stdout".to_string(),
            materialization: None,
            config: serde_json::json!({ "format": "csv", "max_rows": 2 }),
        };
        let batch = test_batch();
        let receipt = sink
            .write(
                &config,
                vec![batch],
                &WriteOptions::default(),
                &MaterializationContext::default(),
            )
            .await
            .unwrap();
        assert_eq!(receipt.rows_written, 2);
    }

    #[tokio::test]
    async fn validate_config_accepts_valid() {
        let sink = StdoutSink::new();
        let config = SinkConfig {
            connector: "stdout".to_string(),
            materialization: None,
            config: serde_json::json!({ "format": "table" }),
        };
        assert!(sink.validate_config(&config).is_ok());
    }

    #[tokio::test]
    async fn validate_config_accepts_empty() {
        let sink = StdoutSink::new();
        let config = SinkConfig {
            connector: "stdout".to_string(),
            materialization: None,
            config: serde_json::json!({}),
        };
        assert!(sink.validate_config(&config).is_ok());
    }
}
