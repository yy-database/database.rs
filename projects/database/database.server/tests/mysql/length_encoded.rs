use database_server::mysql::connection::encode_length_encoded;

/// `251` is the one length a single byte cannot carry: `0xFB` already means
/// `NULL`, so a 251-octet value has to take the `0xFC` form. Encoding it as
/// `0xFC 0x00 0x00` declares a zero-octet value and shifts every following
/// byte of the packet, which drivers report as an unparsable column.
#[test]
fn length_encoded_251_keeps_the_declared_length() {
    let mut bytes = Vec::new();
    encode_length_encoded(&mut bytes, 251);
    assert_eq!(bytes, vec![0xFC, 0xFB, 0x00]);
}

#[test]
fn length_encoded_boundaries_round_trip_their_size() {
    for (value, encoded) in
        [(0_u64, vec![0x00]), (250, vec![0xFA]), (252, vec![0xFC, 0xFC, 0x00]), (0x1_0000, vec![0xFD, 0x00, 0x00, 0x01, 0x00])]
    {
        let mut bytes = Vec::new();
        encode_length_encoded(&mut bytes, value);
        assert_eq!(bytes, encoded, "length-encoded form changed for {value}");
    }
}
