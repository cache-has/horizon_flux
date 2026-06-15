// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Length-prefixed framing for plugin protocol messages.
//!
//! Wire format (see `docs/plugins/protocol-v1.md` §1.1):
//!
//! ```text
//! +----------------+--------+----------------+
//! | length (u32 LE)|  kind  |    payload     |
//! +----------------+--------+----------------+
//! ```
//!
//! `length` does not include itself or the `kind` byte. Payloads larger than
//! [`MAX_PAYLOAD_LEN`] are rejected as a protocol violation.

use std::io::{Read, Write};

use thiserror::Error;

/// Maximum payload size on a single frame: 64 MiB.
pub const MAX_PAYLOAD_LEN: usize = 0x0400_0000;

/// One-byte tag identifying the kind of a framed message.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageKind {
    Hello = 0x01,
    HelloAck = 0x02,
    ConfigureSink = 0x10,
    ConfigureAck = 0x11,
    DeclareResource = 0x15,
    RecordBatch = 0x20,
    BatchAck = 0x21,
    Commit = 0x30,
    CommitAck = 0x31,
    Abort = 0x40,
    AbortAck = 0x41,
    Log = 0x50,
    Error = 0x51,
    Shutdown = 0xF0,
}

impl MessageKind {
    pub fn from_u8(byte: u8) -> Result<Self, FrameError> {
        Ok(match byte {
            0x01 => Self::Hello,
            0x02 => Self::HelloAck,
            0x10 => Self::ConfigureSink,
            0x11 => Self::ConfigureAck,
            0x15 => Self::DeclareResource,
            0x20 => Self::RecordBatch,
            0x21 => Self::BatchAck,
            0x30 => Self::Commit,
            0x31 => Self::CommitAck,
            0x40 => Self::Abort,
            0x41 => Self::AbortAck,
            0x50 => Self::Log,
            0x51 => Self::Error,
            0xF0 => Self::Shutdown,
            other if is_reserved_v2(other) => return Err(FrameError::ReservedKind(other)),
            other => return Err(FrameError::UnknownKind(other)),
        })
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Returns true for kind bytes the v1 protocol explicitly reserves for v2.
/// Hosts log + ignore these on the receive path; plugins must not send them.
pub fn is_reserved_v2(byte: u8) -> bool {
    matches!(byte, 0x60..=0x8F)
}

/// One framed message produced by [`read_frame`].
#[derive(Debug, Clone)]
pub struct Frame {
    pub kind: MessageKind,
    pub payload: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("frame payload of {0} bytes exceeds {MAX_PAYLOAD_LEN} byte limit")]
    PayloadTooLarge(usize),

    #[error("unknown message kind 0x{0:02X}")]
    UnknownKind(u8),

    #[error("message kind 0x{0:02X} is reserved for protocol v2")]
    ReservedKind(u8),

    #[error("unexpected EOF after reading {read} of {expected} bytes")]
    UnexpectedEof { read: usize, expected: usize },

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Encode and write a single frame.
pub fn write_frame<W: Write>(
    w: &mut W,
    kind: MessageKind,
    payload: &[u8],
) -> Result<(), FrameError> {
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(FrameError::PayloadTooLarge(payload.len()));
    }
    let len = payload.len() as u32;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&[kind.as_u8()])?;
    w.write_all(payload)?;
    Ok(())
}

/// Read exactly one frame from the stream.
pub fn read_frame<R: Read>(r: &mut R) -> Result<Frame, FrameError> {
    let mut len_buf = [0u8; 4];
    read_exact(r, &mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_PAYLOAD_LEN {
        return Err(FrameError::PayloadTooLarge(len));
    }
    let mut kind_buf = [0u8; 1];
    read_exact(r, &mut kind_buf)?;
    let kind = MessageKind::from_u8(kind_buf[0])?;
    let mut payload = vec![0u8; len];
    read_exact(r, &mut payload)?;
    Ok(Frame { kind, payload })
}

fn read_exact<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<(), FrameError> {
    let mut total = 0;
    while total < buf.len() {
        match r.read(&mut buf[total..]) {
            Ok(0) => {
                return Err(FrameError::UnexpectedEof {
                    read: total,
                    expected: buf.len(),
                });
            }
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(FrameError::Io(e)),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn round_trip_a_control_frame() {
        let payload = br#"{"protocol":1,"armillary_version":"0.5.0"}"#;
        let mut buf = Vec::new();
        write_frame(&mut buf, MessageKind::Hello, payload).unwrap();
        let mut cur = Cursor::new(buf);
        let frame = read_frame(&mut cur).unwrap();
        assert_eq!(frame.kind, MessageKind::Hello);
        assert_eq!(frame.payload, payload);
    }

    #[test]
    fn round_trip_empty_payload() {
        let mut buf = Vec::new();
        write_frame(&mut buf, MessageKind::Shutdown, &[]).unwrap();
        let mut cur = Cursor::new(buf);
        let frame = read_frame(&mut cur).unwrap();
        assert_eq!(frame.kind, MessageKind::Shutdown);
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn unknown_kind_rejected() {
        let buf = vec![0u8, 0, 0, 0, 0xAB];
        let mut cur = Cursor::new(buf);
        let err = read_frame(&mut cur).unwrap_err();
        assert!(matches!(err, FrameError::UnknownKind(0xAB)));
    }

    #[test]
    fn reserved_v2_kind_rejected() {
        let buf = vec![0u8, 0, 0, 0, 0x60];
        let mut cur = Cursor::new(buf);
        let err = read_frame(&mut cur).unwrap_err();
        assert!(matches!(err, FrameError::ReservedKind(0x60)));
    }

    #[test]
    fn oversized_payload_rejected_on_write() {
        let big = vec![0u8; MAX_PAYLOAD_LEN + 1];
        let mut buf = Vec::new();
        let err = write_frame(&mut buf, MessageKind::RecordBatch, &big).unwrap_err();
        assert!(matches!(err, FrameError::PayloadTooLarge(_)));
    }

    #[test]
    fn oversized_payload_rejected_on_read() {
        let mut buf = Vec::new();
        let len = (MAX_PAYLOAD_LEN as u32 + 1).to_le_bytes();
        buf.extend_from_slice(&len);
        buf.push(MessageKind::RecordBatch.as_u8());
        let mut cur = Cursor::new(buf);
        let err = read_frame(&mut cur).unwrap_err();
        assert!(matches!(err, FrameError::PayloadTooLarge(_)));
    }

    #[test]
    fn truncated_frame_is_eof() {
        // length=10 but only 4 bytes of payload provided
        let mut buf = Vec::new();
        buf.extend_from_slice(&10u32.to_le_bytes());
        buf.push(MessageKind::Log.as_u8());
        buf.extend_from_slice(b"abcd");
        let mut cur = Cursor::new(buf);
        let err = read_frame(&mut cur).unwrap_err();
        assert!(matches!(err, FrameError::UnexpectedEof { .. }));
    }
}
