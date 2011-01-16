//! PostgreSQL 启动包与类型消息组帧，不含 SQL 解释。

#![deny(missing_docs)]

use std::collections::BTreeMap;

/// 从协议 v3 启动包解析出的连接参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupParameters {
    values: BTreeMap<String, String>,
}

/// 非法的启动参数载荷。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupParameterError {
    /// 载荷缺少末尾空键终止符。
    MissingTerminator,
    /// 某个键没有对应值。
    MissingValue,
    /// 键或值不是 UTF-8。
    InvalidUtf8,
    /// 同一参数出现多次。
    DuplicateKey,
    /// 缺少必需的非空 `user` 参数。
    MissingUser,
}

impl StartupParameters {
    /// 解析以 NUL 分隔、以空键终止的键值对。
    pub fn parse(payload: &[u8]) -> Result<Self, StartupParameterError> {
        let mut values = BTreeMap::new();
        let mut offset = 0;
        loop {
            let key_end = payload[offset..]
                .iter()
                .position(|byte| *byte == 0)
                .map(|relative| offset + relative)
                .ok_or(StartupParameterError::MissingTerminator)?;
            if key_end == offset {
                if key_end + 1 != payload.len() {
                    return Err(StartupParameterError::MissingTerminator);
                }
                break;
            }
            let key = std::str::from_utf8(&payload[offset..key_end]).map_err(|_| StartupParameterError::InvalidUtf8)?;
            offset = key_end + 1;
            if offset >= payload.len() {
                return Err(StartupParameterError::MissingValue);
            }
            let value_end = payload[offset..]
                .iter()
                .position(|byte| *byte == 0)
                .map(|relative| offset + relative)
                .ok_or(StartupParameterError::MissingTerminator)?;
            let value = std::str::from_utf8(&payload[offset..value_end]).map_err(|_| StartupParameterError::InvalidUtf8)?;
            if values.insert(key.to_owned(), value.to_owned()).is_some() {
                return Err(StartupParameterError::DuplicateKey);
            }
            offset = value_end + 1;
        }
        if values.get("user").is_none_or(String::is_empty) {
            return Err(StartupParameterError::MissingUser);
        }
        Ok(Self { values })
    }

    /// 读取某个启动参数。
    pub fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    /// 客户端请求的会话用户。
    pub fn user(&self) -> &str {
        self.get("user").expect("startup parser requires a nonempty user")
    }

    /// 请求的数据库名；缺省时回落到用户名。
    pub fn database(&self) -> &str {
        self.get("database").filter(|database| !database.is_empty()).unwrap_or_else(|| self.user())
    }
}

/// 一条启动包。其 code 标识协议版本或特殊请求。
#[derive(Debug, PartialEq, Eq)]
pub struct StartupPacket<'input> {
    /// 协议版本、SSL/GSS 请求码或取消请求码。
    pub code: u32,
    /// code 之后的字节，由连接状态机解释。
    pub payload: &'input [u8],
    /// 已消费字节数（含长度与 code）。
    pub consumed: usize,
}

/// 一条借用二进制载荷的类型消息。
#[derive(Debug, PartialEq, Eq)]
pub struct Message<'input> {
    /// 消息标识，含义取决于方向与会话状态。
    pub tag: u8,
    /// 二进制消息载荷。
    pub payload: &'input [u8],
    /// 已消费字节数（含 tag 与长度字）。
    pub consumed: usize,
}

/// 畸形组帧或资源超限。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// 长度字段小于其自身必填头。
    InvalidLength,
    /// 编码后的整帧长度超过调用方上限。
    FrameTooLarge,
}

/// 解码无类型启动包。不负责接受或认证会话。
pub fn decode_startup(input: &[u8], max_frame: usize) -> Result<Option<StartupPacket<'_>>, FrameError> {
    if input.len() < 4 {
        return Ok(None);
    }
    let length = read_u32(input) as usize;
    if length < 8 {
        return Err(FrameError::InvalidLength);
    }
    if length > max_frame {
        return Err(FrameError::FrameTooLarge);
    }
    if input.len() < length {
        return Ok(None);
    }
    Ok(Some(StartupPacket { code: read_u32(&input[4..]), payload: &input[8..length], consumed: length }))
}

/// 解码一条类型消息。长度字包含自身，不含 tag。
pub fn decode_message(input: &[u8], max_frame: usize) -> Result<Option<Message<'_>>, FrameError> {
    if input.len() < 5 {
        return Ok(None);
    }
    let length = read_u32(&input[1..]) as usize;
    if length < 4 {
        return Err(FrameError::InvalidLength);
    }
    let consumed = length.checked_add(1).ok_or(FrameError::FrameTooLarge)?;
    if consumed > max_frame {
        return Err(FrameError::FrameTooLarge);
    }
    if input.len() < consumed {
        return Ok(None);
    }
    Ok(Some(Message { tag: input[0], payload: &input[5..consumed], consumed }))
}

/// 在调用方给出的整帧上限下编码一条类型消息。
pub fn encode_message(tag: u8, payload: &[u8], max_frame: usize) -> Result<Vec<u8>, FrameError> {
    let length = payload.len().checked_add(4).ok_or(FrameError::FrameTooLarge)?;
    let encoded_length = u32::try_from(length).map_err(|_| FrameError::FrameTooLarge)?;
    let consumed = length.checked_add(1).ok_or(FrameError::FrameTooLarge)?;
    if consumed > max_frame {
        return Err(FrameError::FrameTooLarge);
    }
    let mut bytes = Vec::with_capacity(consumed);
    bytes.push(tag);
    bytes.extend_from_slice(&encoded_length.to_be_bytes());
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

fn read_u32(input: &[u8]) -> u32 {
    u32::from_be_bytes(input[..4].try_into().expect("checked message header"))
}
