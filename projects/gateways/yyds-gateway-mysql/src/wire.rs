//! MySQL classic protocol single-packet framing, without SQL interpretation.

/// Maximum payload length representable by a classic packet header.
pub const MAX_PACKET_PAYLOAD: usize = 0x00ff_ffff;

/// One packet borrowing binary payload bytes from a receive buffer.
#[derive(Debug, PartialEq, Eq)]
pub struct Packet<'input> {
    /// Sequence identifier, checked by the connection state machine.
    pub sequence: u8,
    /// Payload, possibly one fragment of a larger protocol message.
    pub payload: &'input [u8],
    /// Bytes consumed, including the four-byte header.
    pub consumed: usize,
}

/// Packet framing or resource limit failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketError {
    /// Payload exceeds the caller's receive limit or the wire length range.
    PayloadTooLarge,
}

/// Decodes one classic packet, leaving pipeline data unconsumed.
/// A maximum-length packet is not a complete logical message: the connection
/// layer must reassemble continuations and validate their sequence numbers.
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

/// Encodes a single packet. This does not split a logical message into packets.
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
