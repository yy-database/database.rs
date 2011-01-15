//! MongoDB wire protocol primitives.
//!
//! First step: read OP_MSG frame headers and route `hello`/`ping`.
//! BSON document payload handling lands once the handshake skeleton compiles.

use std::io::{self, Read, Write};

/// MongoDB wire protocol opcode for modern (3.6+) message framing.
pub const OP_MSG: i32 = 2013;

/// Limits applied to inbound frames before we inspect the payload.
#[derive(Debug, Clone, Copy)]
pub struct FrameLimits {
    /// Maximum total frame bytes we will accept from one client.
    pub max_frame_size: u32,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self { max_frame_size: 48 * 1024 * 1024 }
    }
}

/// Parsed OP_MSG header: the length + opcode fields read off the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MsgHeader {
    /// Total frame length including the 4-byte length prefix itself.
    pub total_length: u32,
    /// Client-provided request identifier. Echoed back in replies.
    pub request_id: i32,
    /// Response-to identifier (zero for client requests).
    pub response_to: i32,
    /// Wire protocol opcode. Must be OP_MSG for the current implementation.
    pub opcode: i32,
}

/// One inbound MongoDB frame: header + raw body bytes.
#[derive(Debug, Clone)]
pub struct Frame {
    /// Header fields decoded from the wire.
    pub header: MsgHeader,
    /// Payload bytes after the 16-byte header. May be empty for PING-style messages.
    pub body: Vec<u8>,
}

/// Errors specific to frame decoding.
#[derive(Debug)]
pub enum FrameError {
    /// TCP stream closed cleanly before a full header arrived.
    ConnectionClosed,
    /// The length prefix exceeded the configured maximum.
    FrameTooLarge { announced: u32, limit: u32 },
    /// Frame did not use the OP_MSG opcode we support.
    UnsupportedOpcode { opcode: i32 },
    /// Underlying I/O failure while reading bytes.
    Io(io::Error),
}

impl FrameError {
    /// Human-readable frame error text for logging and wire replies.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ConnectionClosed => "mongo frame: connection closed",
            Self::FrameTooLarge { .. } => "mongo frame: frame too large",
            Self::UnsupportedOpcode { .. } => "mongo frame: unsupported opcode",
            Self::Io(_) => "mongo frame: i/o error",
        }
    }
}

impl From<io::Error> for FrameError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

/// Reads one OP_MSG frame from the given stream using the provided limits.
///
/// Returns `None` on a clean connection close before any bytes arrive.
pub fn read_frame<R>(stream: &mut R, limits: FrameLimits) -> Result<Option<Frame>, FrameError>
where
    R: Read,
{
    let mut header_buf = [0_u8; 16];
    read_exact(stream, &mut header_buf)?;

    let total_length = u32::from_le_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
    if total_length < 16 {
        return Err(FrameError::FrameTooLarge { announced: total_length, limit: 0 });
    }
    if total_length > limits.max_frame_size {
        return Err(FrameError::FrameTooLarge { announced: total_length, limit: limits.max_frame_size });
    }

    let request_id = i32::from_le_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);
    let response_to = i32::from_le_bytes([header_buf[8], header_buf[9], header_buf[10], header_buf[11]]);
    let opcode = i32::from_le_bytes([header_buf[12], header_buf[13], header_buf[14], header_buf[15]]);

    if opcode != OP_MSG {
        return Err(FrameError::UnsupportedOpcode { opcode });
    }

    let body_len = (total_length - 16) as usize;
    let mut body = vec![0_u8; body_len];
    if body_len > 0 {
        read_exact(stream, &mut body)?;
    }

    Ok(Some(Frame { header: MsgHeader { total_length, request_id, response_to, opcode }, body }))
}

/// Writes one OP_MSG response frame to the given stream.
pub fn write_frame<W>(stream: &mut W, request_id: i32, response_to: i32, body: &[u8]) -> Result<(), FrameError>
where
    W: Write,
{
    let total_length: u32 = 16 + body.len() as u32;
    stream.write_all(&total_length.to_le_bytes())?;
    stream.write_all(&request_id.to_le_bytes())?;
    stream.write_all(&response_to.to_le_bytes())?;
    stream.write_all(&OP_MSG.to_le_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}

fn read_exact<R>(stream: &mut R, buf: &mut [u8]) -> Result<(), FrameError>
where
    R: Read,
{
    let mut total = 0;
    while total < buf.len() {
        match stream.read(&mut buf[total..]) {
            Ok(0) => {
                if total == 0 {
                    return Err(FrameError::ConnectionClosed);
                }
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "mongo frame truncated").into());
            }
            Ok(n) => total += n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(FrameError::Io(error)),
        }
    }
    Ok(())
}
