use yyds_gateway_redis::resp::{decode_request, FrameError, RequestLimits};

#[test]
fn request_preserves_binary_arguments() {
    let request = decode_request(b"*2\r\n$4\r\nECHO\r\n$5\r\n\0\xff\r\n!\r\n", RequestLimits::default())
        .expect("valid frame")
        .expect("complete");
    assert_eq!(request.arguments, vec![b"ECHO".as_slice(), b"\0\xff\r\n!".as_slice()]);
    assert_eq!(request.consumed, 25);
}

#[test]
fn every_fragment_boundary_is_incomplete() {
    let frame = b"*2\r\n$4\r\nPING\r\n$0\r\n\r\n";
    for boundary in 0..frame.len() {
        assert_eq!(decode_request(&frame[..boundary], RequestLimits::default()), Ok(None));
    }
    let request = decode_request(frame, RequestLimits::default()).unwrap().unwrap();
    assert_eq!(request.arguments, vec![b"PING".as_slice(), b"".as_slice()]);
}

#[test]
fn pipeline_frames_have_independent_limits() {
    let frame = b"*1\r\n$4\r\nPING\r\n";
    let pipeline = [frame.as_slice(), frame.as_slice()].concat();
    let limits = RequestLimits { max_frame_bytes: frame.len(), ..RequestLimits::default() };
    let first = decode_request(&pipeline, limits).unwrap().unwrap();
    assert_eq!(first.consumed, frame.len());
    let second = decode_request(&pipeline[first.consumed..], limits).unwrap().unwrap();
    assert_eq!(first.arguments, second.arguments);
}

#[test]
fn malformed_frames_are_rejected() {
    for (frame, expected) in [
        (b"*0\r\n".as_slice(), FrameError::EmptyCommand),
        (b"*-1\r\n".as_slice(), FrameError::InvalidLength),
        (b"*\r\n".as_slice(), FrameError::InvalidLength),
        (b"*1\n".as_slice(), FrameError::InvalidTerminator),
        (b"*1\r\n+PING\r\n".as_slice(), FrameError::UnexpectedType),
        (b"*1\r\n$-1\r\n".as_slice(), FrameError::InvalidLength),
        (b"*1\r\n$4\r\nPINGxx".as_slice(), FrameError::InvalidTerminator),
        (b"*999999999999999999999999999\r\n".as_slice(), FrameError::InvalidLength),
    ] {
        assert_eq!(decode_request(frame, RequestLimits::default()), Err(expected));
    }
}

#[test]
fn advertised_sizes_are_bounded_before_payload_arrives() {
    let limits = RequestLimits { max_frame_bytes: 31, max_arguments: 2, max_bulk_bytes: 8 };
    assert_eq!(decode_request(b"*3\r\n", limits), Err(FrameError::TooManyArguments));
    assert_eq!(decode_request(b"*1\r\n$9\r\n", limits), Err(FrameError::BulkTooLarge));
    assert_eq!(decode_request(b"*2\r\n$8\r\n12345678\r\n$8\r\n", limits), Err(FrameError::FrameTooLarge));
    let zero = RequestLimits { max_frame_bytes: 0, ..limits };
    assert_eq!(decode_request(b"", zero), Err(FrameError::FrameTooLarge));
}
