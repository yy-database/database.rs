//! MySQL classic 协议单包组帧，不含 SQL 解释。

#![deny(missing_docs)]

/// classic 包头能表示的最大载荷长度。
pub const MAX_PACKET_PAYLOAD: usize = 0x00ff_ffff;

/// classic 协议握手宣告的版本号。
pub const PROTOCOL_VERSION: u8 = 10;

/// 一条借用接收缓冲区二进制载荷的包。
#[derive(Debug, PartialEq, Eq)]
pub struct Packet<'input> {
    /// 序号，由连接状态机校验。
    pub sequence: u8,
    /// 载荷，可能是更大逻辑消息的一个分片。
    pub payload: &'input [u8],
    /// 已消费字节数（含 4 字节头）。
    pub consumed: usize,
}

/// 组帧失败或资源超限。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketError {
    /// 载荷超过调用方接收上限或线长范围。
    PayloadTooLarge,
}

/// 解码一条 classic 包，流水线后续数据保持未消费。
/// 最大长度包并不等于完整逻辑消息：连接层必须重组续包并校验序号。
pub fn decode_packet(input: &[u8], max_payload: usize) -> Result<Option<Packet<'_>>, PacketError> {
    if input.len() < 4 {
        return Ok(None);
    }
    let length = usize::from(input[0]) | (usize::from(input[1]) << 8) | (usize::from(input[2]) << 16);
    if length > max_payload {
        return Err(PacketError::PayloadTooLarge);
    }
    let consumed = length + 4;
    if input.len() < consumed {
        return Ok(None);
    }
    Ok(Some(Packet { sequence: input[3], payload: &input[4..consumed], consumed }))
}

/// 编码单包。不会把一条逻辑消息拆成多包。
pub fn encode_packet(sequence: u8, payload: &[u8]) -> Result<Vec<u8>, PacketError> {
    if payload.len() > MAX_PACKET_PAYLOAD {
        return Err(PacketError::PayloadTooLarge);
    }
    let length = payload.len();
    let mut bytes = Vec::with_capacity(length + 4);
    bytes.extend_from_slice(&[length as u8, (length >> 8) as u8, (length >> 16) as u8, sequence]);
    bytes.extend_from_slice(payload);
    Ok(bytes)
}
