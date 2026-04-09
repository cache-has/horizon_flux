// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Arrow IPC helpers for the plugin protocol.
//!
//! - [`encode_schema_b64`] encodes an Arrow [`Schema`] as the base64 of an
//!   Arrow IPC `Schema` message, suitable for the `input_schema_ipc_b64`
//!   field of [`crate::protocol::ConfigureSink`].
//! - [`encode_record_batch`] encodes a single [`RecordBatch`] as a complete
//!   Arrow IPC stream containing exactly one batch (header + dictionaries +
//!   batch + EOS). One frame on the wire = one stream.
//! - [`decode_schema_b64`] / [`decode_record_batch`] are the symmetric
//!   helpers used by hosts (and by the plugin SDK).

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ArrowIpcError {
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("base64 decode failed: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error("expected exactly one record batch in stream, got {0}")]
    BatchCount(usize),

    #[error("stream contained no schema")]
    NoSchema,
}

/// Encode `schema` as a base64-wrapped Arrow IPC stream containing only the
/// schema header (no batches). This matches the `input_schema_ipc_b64` field
/// of `ConfigureSink`.
pub fn encode_schema_b64(schema: &Schema) -> Result<String, ArrowIpcError> {
    let buf: Vec<u8> = Vec::new();
    let writer = StreamWriter::try_new_with_options(buf, schema, IpcWriteOptions::default())?;
    let buf = writer.into_inner()?;
    Ok(B64.encode(buf))
}

/// Decode a base64 Arrow IPC stream and return its schema.
pub fn decode_schema_b64(b64: &str) -> Result<Arc<Schema>, ArrowIpcError> {
    let bytes = B64.decode(b64)?;
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    Ok(reader.schema())
}

/// Encode one [`RecordBatch`] as a self-contained Arrow IPC stream.
pub fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>, ArrowIpcError> {
    let buf: Vec<u8> = Vec::new();
    let mut writer =
        StreamWriter::try_new_with_options(buf, batch.schema_ref(), IpcWriteOptions::default())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(writer.into_inner()?)
}

/// Decode a self-contained Arrow IPC stream that holds exactly one batch.
pub fn decode_record_batch(bytes: &[u8]) -> Result<RecordBatch, ArrowIpcError> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let mut out: Option<RecordBatch> = None;
    let mut count = 0usize;
    for b in reader {
        let b = b?;
        count += 1;
        if out.is_none() {
            out = Some(b);
        }
    }
    if count != 1 {
        return Err(ArrowIpcError::BatchCount(count));
    }
    out.ok_or(ArrowIpcError::NoSchema)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn schema_round_trip() {
        let batch = sample_batch();
        let b64 = encode_schema_b64(batch.schema_ref()).unwrap();
        let decoded = decode_schema_b64(&b64).unwrap();
        assert_eq!(decoded.fields().len(), 2);
        assert_eq!(decoded.field(0).name(), "id");
    }

    #[test]
    fn batch_round_trip() {
        let batch = sample_batch();
        let bytes = encode_record_batch(&batch).unwrap();
        let decoded = decode_record_batch(&bytes).unwrap();
        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.num_columns(), 2);
    }
}
