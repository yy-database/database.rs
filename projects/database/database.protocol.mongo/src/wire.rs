//! MongoDB 线协议原语。
//!
//! 读取本栈应答的两类帧：普通命令用 OP_MSG；驱动在切换到 OP_MSG 之前
//! 仍会发的遗留握手用 OP_QUERY。

use std::io::{self, Read, Write};

/// 现代（3.6+）消息组帧的 MongoDB 线协议操作码。
pub const OP_MSG: i32 = 2013;

/// 遗留查询操作码。真实驱动用它做首次 `isMaster` / `hello` 探测，
/// 在确认对端支持 OP_MSG 之前发送。
pub const OP_QUERY: i32 = 2004;

/// 遗留回复操作码，对应 [`OP_QUERY`] 的应答形状。
pub const OP_REPLY: i32 = 1;

/// 检查载荷前施加于入站帧的上限。
#[derive(Debug, Clone, Copy)]
pub struct FrameLimits {
    /// 单个客户端可接受的最大整帧字节数。
    pub max_frame_size: u32,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self { max_frame_size: 48 * 1024 * 1024 }
    }
}

/// 已解析的 OP_MSG 头：从线上读出的长度与操作码字段。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MsgHeader {
    /// 整帧长度，含 4 字节长度前缀自身。
    pub total_length: u32,
    /// 客户端请求标识，回复时原样回显。
    pub request_id: i32,
    /// 应答对象标识（客户端请求时为 0）。
    pub response_to: i32,
    /// 从帧头读出的线协议操作码。
    pub opcode: i32,
}

/// 一条入站 MongoDB 帧：头 + 原始 body 字节。
#[derive(Debug, Clone)]
pub struct Frame {
    /// 从线上解码的头字段。
    pub header: MsgHeader,
    /// 16 字节头之后的载荷；PING 类消息可为空。
    pub body: Vec<u8>,
}

/// 帧解码专用错误。
#[derive(Debug)]
pub enum FrameError {
    /// 完整头到达前 TCP 流干净关闭。
    ConnectionClosed,
    /// 长度前缀超过配置上限。
    FrameTooLarge {
        /// 客户端声明的帧长度。
        announced: u32,
        /// 本栈接受的最大长度。
        limit: u32,
    },
    /// 帧使用了本栈不应答的操作码。
    UnsupportedOpcode {
        /// 从帧头读出的操作码。
        opcode: i32,
    },
    /// 读字节时的底层 I/O 失败。
    Io(io::Error),
}

impl FrameError {
    /// 供日志与线回复使用的可读帧错误文本。
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

/// Reads one MongoDB frame from the given stream using the provided limits.
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

    if opcode != OP_MSG && opcode != OP_QUERY {
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

/// Writes one OP_REPLY frame carrying a single BSON document.
///
/// Legacy replies carry `responseFlags`, `cursorId`, `startingFrom` and
/// `numberReturned` before the document, all zero except the document count.
pub fn write_reply_frame<W>(stream: &mut W, request_id: i32, response_to: i32, document: &[u8]) -> Result<(), FrameError>
where
    W: Write,
{
    let total_length: u32 = 16 + 20 + document.len() as u32;
    stream.write_all(&total_length.to_le_bytes())?;
    stream.write_all(&request_id.to_le_bytes())?;
    stream.write_all(&response_to.to_le_bytes())?;
    stream.write_all(&OP_REPLY.to_le_bytes())?;
    stream.write_all(&0_i32.to_le_bytes())?;
    stream.write_all(&0_i64.to_le_bytes())?;
    stream.write_all(&0_i32.to_le_bytes())?;
    stream.write_all(&1_i32.to_le_bytes())?;
    stream.write_all(document)?;
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
