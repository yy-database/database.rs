//! 有界 RESP2 请求组帧。SQL / VOS 源语法仍归 Oak。

/// 单条命令的资源上限，与流水线长度无关。
#[derive(Clone, Copy, Debug)]
pub struct RequestLimits {
    /// 单条命令最大编码字节数。
    pub max_frame_bytes: usize,
    /// 最大命令与参数个数。
    pub max_arguments: usize,
    /// 单个二进制参数最大字节数。
    pub max_bulk_bytes: usize,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self { max_frame_bytes: 1_048_576, max_arguments: 1024, max_bulk_bytes: 1_048_576 }
    }
}

/// 一条完整命令，参数字节借用自输入缓冲。
#[derive(Debug, PartialEq, Eq)]
pub struct Request<'input> {
    /// 命令名及其参数，不做 UTF-8 转换。
    pub arguments: Vec<&'input [u8]>,
    /// 已消费的编码字节数，后续流水线帧保持未触碰。
    pub consumed: usize,
}

/// 畸形或耗尽资源的请求，连接应拒绝。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// 命令必须是非空的 bulk string 数组。
    UnexpectedType,
    /// 长度为负、非十进制、空或溢出。
    InvalidLength,
    /// 行或 bulk 载荷不以 CRLF 结束。
    InvalidTerminator,
    /// 数组不含命令名。
    EmptyCommand,
    /// 编码后的命令超过配置的字节上限。
    FrameTooLarge,
    /// 参数个数超过配置上限。
    TooManyArguments,
    /// 某个 bulk 参数超过配置上限。
    BulkTooLarge,
}

/// 把一条 RESP2 命令编码为 bulk-string 数组。
pub fn encode_request(arguments: &[&[u8]], limits: RequestLimits) -> Result<Vec<u8>, FrameError> {
    if arguments.is_empty() {
        return Err(FrameError::EmptyCommand);
    }
    if arguments.len() > limits.max_arguments {
        return Err(FrameError::TooManyArguments);
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(format!("*{}\r\n", arguments.len()).as_bytes());
    for argument in arguments {
        if argument.len() > limits.max_bulk_bytes {
            return Err(FrameError::BulkTooLarge);
        }
        encoded.extend_from_slice(format!("${}\r\n", argument.len()).as_bytes());
        encoded.extend_from_slice(argument);
        encoded.extend_from_slice(b"\r\n");
    }
    if encoded.len() > limits.max_frame_bytes {
        return Err(FrameError::FrameTooLarge);
    }
    Ok(encoded)
}

/// 解码首条 RESP2 命令；帧不完整时返回 `None`。
/// 这是线组帧，不是语言前端或命令绑定器。
pub fn decode_request(input: &[u8], limits: RequestLimits) -> Result<Option<Request<'_>>, FrameError> {
    let bounded = &input[..input.len().min(limits.max_frame_bytes)];
    let decoded = decode_bounded(bounded, limits)?;
    if decoded.is_none() && input.len() >= limits.max_frame_bytes {
        return Err(FrameError::FrameTooLarge);
    }
    Ok(decoded)
}

fn decode_bounded(input: &[u8], limits: RequestLimits) -> Result<Option<Request<'_>>, FrameError> {
    let mut offset = 0;
    let Some(count) = read_length(input, &mut offset, b'*')?
    else {
        return Ok(None);
    };
    if count == 0 {
        return Err(FrameError::EmptyCommand);
    }
    if count > limits.max_arguments {
        return Err(FrameError::TooManyArguments);
    }
    let mut arguments = Vec::new();
    for _ in 0..count {
        let Some(length) = read_length(input, &mut offset, b'$')?
        else {
            return Ok(None);
        };
        if length > limits.max_bulk_bytes {
            return Err(FrameError::BulkTooLarge);
        }
        let end = offset.checked_add(length).ok_or(FrameError::FrameTooLarge)?;
        let consumed = end.checked_add(2).ok_or(FrameError::FrameTooLarge)?;
        if consumed > limits.max_frame_bytes {
            return Err(FrameError::FrameTooLarge);
        }
        if input.len() < consumed {
            return Ok(None);
        }
        if &input[end..consumed] != b"\r\n" {
            return Err(FrameError::InvalidTerminator);
        }
        arguments.push(&input[offset..end]);
        offset = consumed;
    }
    Ok(Some(Request { arguments, consumed: offset }))
}

fn read_length(input: &[u8], offset: &mut usize, marker: u8) -> Result<Option<usize>, FrameError> {
    let Some(first) = input.get(*offset)
    else {
        return Ok(None);
    };
    if *first != marker {
        return Err(FrameError::UnexpectedType);
    }
    let mut cursor = *offset + 1;
    let digits_start = cursor;
    let mut value = 0usize;
    while let Some(byte) = input.get(cursor) {
        if *byte == b'\r' {
            if cursor == digits_start {
                return Err(FrameError::InvalidLength);
            }
            let Some(next) = input.get(cursor + 1)
            else {
                return Ok(None);
            };
            if *next != b'\n' {
                return Err(FrameError::InvalidTerminator);
            }
            *offset = cursor + 2;
            return Ok(Some(value));
        }
        if *byte == b'\n' {
            return Err(FrameError::InvalidTerminator);
        }
        if !byte.is_ascii_digit() {
            return Err(FrameError::InvalidLength);
        }
        value = value.checked_mul(10).and_then(|number| number.checked_add(usize::from(*byte - b'0'))).ok_or(FrameError::InvalidLength)?;
        cursor += 1;
    }
    Ok(None)
}
