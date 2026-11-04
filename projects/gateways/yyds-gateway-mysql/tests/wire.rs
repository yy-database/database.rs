use yyds_gateway_mysql::wire::{PacketError, decode_packet, encode_packet};

#[test]
fn binary_payload_roundtrip_and_every_fragment_boundary() {
    let frame = encode_packet(255, b"\0\xff\r\n").unwrap();
    assert_eq!(&frame[..4], &[4, 0, 0, 255]);
    for boundary in 0..frame.len() {
        assert_eq!(decode_packet(&frame[..boundary], 1024), Ok(None));
    }
    let packet = decode_packet(&frame, 1024).unwrap().unwrap();
    assert_eq!(packet.sequence, 255);
    assert_eq!(packet.payload, b"\0\xff\r\n");
    assert_eq!(packet.consumed, frame.len());
}

#[test]
fn pipeline_and_empty_terminator_packets() {
    let first = encode_packet(0, b"hello").unwrap();
    let second = encode_packet(1, b"").unwrap();
    let bytes = [first.as_slice(), second.as_slice()].concat();
    let packet = decode_packet(&bytes, 5).unwrap().unwrap();
    assert_eq!(packet.consumed, first.len());
    let terminator = decode_packet(&bytes[packet.consumed..], 0).unwrap().unwrap();
    assert_eq!(terminator.sequence, 1);
    assert!(terminator.payload.is_empty());
}

#[test]
fn little_endian_lengths_and_early_limit_rejection() {
    assert_eq!(decode_packet(&[0, 1, 0, 0], 255), Err(PacketError::PayloadTooLarge));
    assert_eq!(decode_packet(&[0, 0, 1, 0], 65535), Err(PacketError::PayloadTooLarge));
    assert_eq!(decode_packet(&[255, 255, 255, 0], 1048576), Err(PacketError::PayloadTooLarge));
    assert_eq!(decode_packet(&[0, 1, 0, 0], 256), Ok(None));
}
