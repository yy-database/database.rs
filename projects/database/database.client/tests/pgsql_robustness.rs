//! Scripted protocol-v3 servers that drive failure paths no SQL engine can
//! produce on demand: a peer that dies mid-result, a truncated frame, the
//! `ReadyForQuery` recovery after `08P01`, and the `CancelRequest` bytes.
//!
//! The fixtures speak only framing. They are not an engine, and their replies
//! are fixed byte scripts.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::Duration,
};

use database_client::pgsql::{
    client::{ClientConfig, Connection, Param},
    error::PostgresError,
};

const WAIT: Duration = Duration::from_secs(5);

fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(payload.len() + 5);
    bytes.push(tag);
    bytes.extend_from_slice(&(payload.len() as u32 + 4).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn read_startup(stream: &mut TcpStream) {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).expect("startup length");
    let length = u32::from_be_bytes(length) as usize;
    let mut body = vec![0_u8; length - 4];
    stream.read_exact(&mut body).expect("startup body");
}

fn startup_ok(stream: &mut TcpStream, key: Option<(i32, i32)>) {
    stream.write_all(&frame(b'R', &[0, 0, 0, 0])).expect("AuthenticationOk");
    if let Some((process_id, secret_key)) = key {
        let mut payload = Vec::new();
        payload.extend_from_slice(&process_id.to_be_bytes());
        payload.extend_from_slice(&secret_key.to_be_bytes());
        stream.write_all(&frame(b'K', &payload)).expect("BackendKeyData");
    }
    stream.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
}

fn read_tagged(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0_u8; 5];
    stream.read_exact(&mut header).expect("message header");
    let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut payload = vec![0_u8; length - 4];
    stream.read_exact(&mut payload).expect("message payload");
    (header[0], payload)
}

fn row_description(column: &str) -> Vec<u8> {
    let mut payload = vec![0, 1];
    payload.extend_from_slice(column.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&[0, 0, 0, 0]); // table oid
    payload.extend_from_slice(&[0, 0]); // column attribute
    payload.extend_from_slice(&[0, 0, 0, 23]); // int4
    payload.extend_from_slice(&[0, 0]); // type size
    payload.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // type modifier
    payload.extend_from_slice(&[0, 0]); // text format
    payload
}

fn data_row(value: &str) -> Vec<u8> {
    let mut payload = vec![0, 1];
    payload.extend_from_slice(&(value.len() as i32).to_be_bytes());
    payload.extend_from_slice(value.as_bytes());
    payload
}

fn spawn_server<F>(script: F) -> (u16, thread::JoinHandle<()>)
where
    F: FnOnce(TcpListener) + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let port = listener.local_addr().expect("local address").port();
    let handle = thread::spawn(move || script(listener));
    (port, handle)
}

fn config(port: u16) -> ClientConfig {
    ClientConfig::new("127.0.0.1", port, "postgres", "postgres")
}

#[test]
fn cancel_request_uses_a_fresh_connection_and_the_backend_key() {
    let (sender, receiver) = mpsc::channel::<Vec<u8>>();
    let (port, handle) = spawn_server(move |listener| {
        let (mut main, _) = listener.accept().expect("main session");
        read_startup(&mut main);
        startup_ok(&mut main, Some((1234, 5678)));

        let (_tag, _payload) = read_tagged(&mut main);
        main.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        main.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");

        let (mut cancel, _) = listener.accept().expect("cancel connection");
        let mut packet = [0_u8; 16];
        cancel.read_exact(&mut packet).expect("CancelRequest bytes");
        sender.send(packet.to_vec()).expect("send captured packet");
        drop(cancel);

        let (_tag, _payload) = read_tagged(&mut main);
        main.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        main.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let key = connection.backend_key_data().expect("server sent BackendKeyData");
    assert_eq!((key.process_id, key.secret_key), (1234, 5678));
    assert_eq!(connection.execute("SELECT 1").expect("first statement")[0].tag, "SELECT 1");

    connection.cancel().expect("cancel");
    let packet = receiver.recv_timeout(WAIT).expect("cancel packet arrived");
    assert_eq!(packet.len(), 16, "CancelRequest is exactly sixteen bytes");
    assert_eq!(u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]), 80_877_102);
    assert_eq!(i32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]), 1234);
    assert_eq!(i32::from_be_bytes([packet[12], packet[13], packet[14], packet[15]]), 5678);

    assert_eq!(connection.execute("SELECT 1").expect("same session after cancel")[0].tag, "SELECT 1");
    handle.join().expect("server thread");
    let _ = connection.close();
}

#[test]
fn server_error_then_ready_for_query_keeps_the_connection_usable() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((7, 9)));

        let (_tag, _payload) = read_tagged(&mut server);
        server.write_all(&frame(b'E', b"SERROR\0VERROR\0C08P01\0Mbind message supplies 0 parameters\0\0")).expect("ErrorResponse");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");

        let (_tag, _payload) = read_tagged(&mut server);
        server.write_all(&frame(b'T', &row_description("n"))).expect("RowDescription");
        server.write_all(&frame(b'D', &data_row("1"))).expect("DataRow");
        server.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let error = connection.execute("SELECT $1").expect_err("server error");
    assert_eq!(error.code(), Some("08P01"));
    let results = connection.execute("SELECT 1 AS n").expect("next statement on the same socket");
    assert_eq!(results[0].rows, vec![vec![Some("1".to_owned())]]);
    assert_eq!(results[0].column_names().collect::<Vec<_>>(), vec!["n"]);
    handle.join().expect("server thread");
    let _ = connection.close();
}

#[test]
fn peer_close_after_a_partial_row_poisons_the_connection() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((1, 2)));
        let (_tag, _payload) = read_tagged(&mut server);
        server.write_all(&frame(b'T', &row_description("n"))).expect("RowDescription");
        server.write_all(&frame(b'D', &data_row("1"))).expect("DataRow");
        // No CommandComplete and no ReadyForQuery: the result is cut in half.
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let error = connection.execute("SELECT 1 AS n").expect_err("partial result must fail");
    assert!(matches!(error, PostgresError::Io(_)), "unexpected {error}");
    let again = connection.execute("SELECT 2").expect_err("poisoned connection must refuse");
    assert_eq!(again.message(), format!("connection is unusable after {error}"));
    handle.join().expect("server thread");
    let _ = connection.close();
}

#[test]
fn truncated_message_length_poisons_the_connection() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((3, 4)));
        let (_tag, _payload) = read_tagged(&mut server);
        // Declares a 20-byte message but only delivers five payload bytes.
        server.write_all(&[b'D', 0, 0, 0, 20, 0, 1, 0, 0, 0]).expect("half frame");
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let error = connection.execute("SELECT 1").expect_err("half frame must fail");
    assert!(matches!(error, PostgresError::Io(_)), "unexpected {error}");
    let again = connection.execute("SELECT 2").expect_err("poisoned connection must refuse");
    assert_eq!(again.message(), format!("connection is unusable after {error}"));
    handle.join().expect("server thread");
    let _ = connection.close();
}

#[test]
fn cancel_without_backend_key_data_is_a_protocol_error() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, None);
        server.set_read_timeout(Some(Duration::from_millis(50))).expect("read timeout");
    });

    let connection = Connection::open(&config(port)).expect("open");
    assert!(connection.backend_key_data().is_none(), "no BackendKeyData was sent");
    let error = connection.cancel().expect_err("cancel must refuse");
    assert_eq!(error.message(), "server sent no BackendKeyData to cancel");
    handle.join().expect("server thread");
}

fn copy_response_payload() -> Vec<u8> {
    // Text format, two columns, both text.
    vec![0, 0, 2, 0, 0, 0, 0]
}

fn read_copy_stream(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut frames = Vec::new();
    loop {
        let (tag, payload) = read_tagged(stream);
        frames.push((tag, payload));
        if tag != b'd' {
            return frames;
        }
    }
}

#[test]
fn copy_in_streams_every_chunk_and_reports_the_tag() {
    let (sender, receiver) = mpsc::channel::<Vec<(u8, Vec<u8>)>>();
    let (port, handle) = spawn_server(move |listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((11, 12)));

        let (tag, payload) = read_tagged(&mut server);
        assert_eq!(tag, b'Q');
        assert_eq!(payload, b"COPY t FROM STDIN\0");
        server.write_all(&frame(b'G', &copy_response_payload())).expect("CopyInResponse");

        let frames = read_copy_stream(&mut server);
        sender.send(frames).expect("captured copy stream");
        server.write_all(&frame(b'C', b"COPY 2\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");

        let (_tag, _payload) = read_tagged(&mut server);
        server.write_all(&frame(b'T', &row_description("n"))).expect("RowDescription");
        server.write_all(&frame(b'D', &data_row("1"))).expect("DataRow");
        server.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let report = connection.copy_in("COPY t FROM STDIN", &[b"1\ta\n".as_slice(), b"2\tb\n".as_slice()]).expect("copy in");
    assert_eq!(report.tag, "COPY 2");
    assert!(report.error.is_none(), "unexpected {:?}", report.error);

    let frames = receiver.recv_timeout(WAIT).expect("copy stream");
    assert_eq!(frames.len(), 3, "two data chunks and one CopyDone");
    assert_eq!(frames[0], (b'd', b"1\ta\n".to_vec()));
    assert_eq!(frames[1], (b'd', b"2\tb\n".to_vec()));
    assert_eq!(frames[2], (b'c', Vec::new()));

    let results = connection.execute("SELECT 1 AS n").expect("same session after copy in");
    assert_eq!(results[0].rows, vec![vec![Some("1".to_owned())]]);
    handle.join().expect("server thread");
    let _ = connection.close();
}

#[test]
fn copy_in_mid_error_keeps_the_connection_usable() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((13, 14)));

        let (tag, _payload) = read_tagged(&mut server);
        assert_eq!(tag, b'Q');
        server.write_all(&frame(b'G', &copy_response_payload())).expect("CopyInResponse");
        let _frames = read_copy_stream(&mut server);
        server.write_all(&frame(b'E', b"SERROR\0VERROR\0C22P02\0Minvalid input syntax for type integer\0\0")).expect("ErrorResponse");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");

        let (tag, _payload) = read_tagged(&mut server);
        assert_eq!(tag, b'Q');
        server.write_all(&frame(b'T', &row_description("n"))).expect("RowDescription");
        server.write_all(&frame(b'D', &data_row("2"))).expect("DataRow");
        server.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let report = connection.copy_in("COPY t FROM STDIN", &[b"1\ta\n".as_slice(), b"bad\n".as_slice()]).expect("copy in");
    let error = report.error.expect("the server error is reported, not thrown");
    assert_eq!(error.code(), Some("22P02"));
    assert_eq!(report.tag, "", "no CommandComplete arrived");
    let results = connection.execute("SELECT 1 AS n").expect("same session after a mid-copy error");
    assert_eq!(results[0].rows, vec![vec![Some("2".to_owned())]]);
    handle.join().expect("server thread");
    let _ = connection.close();
}

#[test]
fn copy_in_rejected_before_copy_response_sends_no_data() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((15, 16)));

        let (tag, _payload) = read_tagged(&mut server);
        assert_eq!(tag, b'Q');
        server.write_all(&frame(b'E', b"SERROR\0VERROR\0C42P01\0Mrelation \"missing\" does not exist\0\0")).expect("ErrorResponse");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");

        let (next, _payload) = read_tagged(&mut server);
        assert_eq!(next, b'Q', "the client must not stream CopyData without CopyInResponse");
        server.write_all(&frame(b'T', &row_description("n"))).expect("RowDescription");
        server.write_all(&frame(b'D', &data_row("3"))).expect("DataRow");
        server.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let report = connection.copy_in("COPY missing FROM STDIN", &[b"1\ta\n".as_slice()]).expect("copy in");
    assert_eq!(report.error.expect("server error").code(), Some("42P01"));
    let results = connection.execute("SELECT 1 AS n").expect("same session after a rejected copy");
    assert_eq!(results[0].rows, vec![vec![Some("3".to_owned())]]);
    handle.join().expect("server thread");
    let _ = connection.close();
}

#[test]
fn copy_out_collects_every_chunk_and_reports_the_tag() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((17, 18)));

        let (tag, _payload) = read_tagged(&mut server);
        assert_eq!(tag, b'Q');
        server.write_all(&frame(b'H', &copy_response_payload())).expect("CopyOutResponse");
        server.write_all(&frame(b'd', b"1\ta\n")).expect("CopyData");
        server.write_all(&frame(b'd', b"2\t\\N\n")).expect("CopyData");
        server.write_all(&frame(b'c', &[])).expect("CopyDone");
        server.write_all(&frame(b'C', b"COPY 2\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");

        let (_tag, _payload) = read_tagged(&mut server);
        server.write_all(&frame(b'T', &row_description("n"))).expect("RowDescription");
        server.write_all(&frame(b'D', &data_row("4"))).expect("DataRow");
        server.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let report = connection.copy_out("COPY t TO STDOUT").expect("copy out");
    assert_eq!(report.data, b"1\ta\n2\t\\N\n".to_vec());
    assert_eq!(report.tag, "COPY 2");
    assert!(report.error.is_none(), "unexpected {:?}", report.error);
    let results = connection.execute("SELECT 1 AS n").expect("same session after copy out");
    assert_eq!(results[0].rows, vec![vec![Some("4".to_owned())]]);
    handle.join().expect("server thread");
    let _ = connection.close();
}

#[test]
fn copy_in_streams_a_large_batch_without_buffering_it() {
    let (sender, receiver) = mpsc::channel::<usize>();
    let (port, handle) = spawn_server(move |listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((23, 24)));

        let (tag, _payload) = read_tagged(&mut server);
        assert_eq!(tag, b'Q');
        server.write_all(&frame(b'G', &copy_response_payload())).expect("CopyInResponse");
        let frames = read_copy_stream(&mut server);
        sender.send(frames.len()).expect("frame count");
        server.write_all(&frame(b'C', b"COPY 500\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
    });

    let chunks: Vec<Vec<u8>> = (0..500).map(|index| format!("{index}\n").into_bytes()).collect();
    let slices: Vec<&[u8]> = chunks.iter().map(|chunk| chunk.as_slice()).collect();
    let mut connection = Connection::open(&config(port)).expect("open");
    let report = connection.copy_in("COPY t FROM STDIN", &slices).expect("copy in");
    assert_eq!(report.tag, "COPY 500");
    assert_eq!(receiver.recv_timeout(WAIT).expect("frame count"), 501, "500 chunks and one CopyDone");
    handle.join().expect("server thread");
    let _ = connection.close();
}

#[test]
fn copy_in_transport_loss_poisons_the_connection() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((25, 26)));

        let (tag, _payload) = read_tagged(&mut server);
        assert_eq!(tag, b'Q');
        server.write_all(&frame(b'G', &copy_response_payload())).expect("CopyInResponse");
        let _data = read_tagged(&mut server);
        let _done = read_tagged(&mut server);
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let error = connection.copy_in("COPY t FROM STDIN", &[b"1\ta\n".as_slice()]).expect_err("a closed peer must throw");
    assert!(matches!(error, PostgresError::Io(_)), "unexpected {error:?}");
    let second = connection.execute("SELECT 1").expect_err("poisoned connection");
    assert!(second.to_string().starts_with("connection is unusable after"), "unexpected {second}");
    handle.join().expect("server thread");
}

#[test]
fn copy_out_error_keeps_the_prefix_and_the_connection_usable() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((19, 20)));

        let (tag, _payload) = read_tagged(&mut server);
        assert_eq!(tag, b'Q');
        server.write_all(&frame(b'H', &copy_response_payload())).expect("CopyOutResponse");
        server.write_all(&frame(b'd', b"1\ta\n")).expect("CopyData");
        server.write_all(&frame(b'E', b"SERROR\0VERROR\0C22025\0Minvalid escape sequence\0\0")).expect("ErrorResponse");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");

        let (_tag, _payload) = read_tagged(&mut server);
        server.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let report = connection.copy_out("COPY t TO STDOUT").expect("copy out");
    assert_eq!(report.data, b"1\ta\n".to_vec(), "the received prefix is kept");
    assert_eq!(report.tag, "", "no CommandComplete arrived");
    assert_eq!(report.error.expect("server error").code(), Some("22025"));
    connection.execute("SELECT 1").expect("same session after a copy-out error");
    handle.join().expect("server thread");
    let _ = connection.close();
}

fn typed_row_description(column: &str, oid: u32, format: u16) -> Vec<u8> {
    let mut payload = vec![0, 1];
    payload.extend_from_slice(column.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&[0, 0, 0, 0]); // table oid
    payload.extend_from_slice(&[0, 0]); // column attribute
    payload.extend_from_slice(&oid.to_be_bytes());
    payload.extend_from_slice(&4_i16.to_be_bytes());
    payload.extend_from_slice(&(-1_i32).to_be_bytes());
    payload.extend_from_slice(&format.to_be_bytes());
    payload
}

fn binary_row(bytes: &[u8]) -> Vec<u8> {
    let mut payload = vec![0, 1];
    payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    payload.extend_from_slice(bytes);
    payload
}

#[test]
fn binary_result_rows_are_decoded_from_the_wire() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((27, 28)));

        let (_tag, _payload) = read_tagged(&mut server);
        let _bind = read_tagged(&mut server);
        let _describe = read_tagged(&mut server);
        let _execute = read_tagged(&mut server);
        let _sync = read_tagged(&mut server);
        server.write_all(&frame(b'1', b"")).expect("ParseComplete");
        server.write_all(&frame(b'2', b"")).expect("BindComplete");
        server.write_all(&frame(b'T', &typed_row_description("n", 23, 1))).expect("RowDescription");
        server.write_all(&frame(b'D', &binary_row(&[0, 0, 0, 42]))).expect("DataRow");
        server.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let results = connection.execute_with_param_formats("SELECT 42", &[], &[], &[1]).expect("binary result");
    assert_eq!(results[0].rows, vec![vec![Some("42".to_owned())]]);
    assert_eq!(results[0].tag, "SELECT 1");
    handle.join().expect("server thread");
    let _ = connection.close();
}

#[test]
fn binary_decode_errors_drain_to_ready_for_query() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((29, 30)));

        for _ in 0..5 {
            let _ = read_tagged(&mut server);
        }
        server.write_all(&frame(b'1', b"")).expect("ParseComplete");
        server.write_all(&frame(b'2', b"")).expect("BindComplete");
        server.write_all(&frame(b'T', &typed_row_description("f", 701, 1))).expect("RowDescription");
        server.write_all(&frame(b'D', &binary_row(&[0, 0, 0, 0, 0, 0, 0, 0]))).expect("DataRow");
        server.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");

        let (tag, payload) = read_tagged(&mut server);
        assert_eq!(tag, b'Q');
        assert!(payload.starts_with(b"SELECT 1"));
        server.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let error = connection.execute_with_param_formats("SELECT 1::float8", &[], &[], &[1]).expect_err("float8 binary is unsupported");
    assert_eq!(error.feature(), Some("binary-result-type-701"));
    connection.execute("SELECT 1").expect("the session survives a decode error");
    handle.join().expect("server thread");
    let _ = connection.close();
}

#[test]
fn portal_suspension_keeps_rows_and_the_session_open() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((31, 32)));

        // Prepare: Parse, Describe(S), Sync.
        for _ in 0..3 {
            let _ = read_tagged(&mut server);
        }
        server.write_all(&frame(b'1', b"")).expect("ParseComplete");
        server.write_all(&frame(b't', &[0, 1, 0, 0, 0, 23])).expect("ParameterDescription");
        server.write_all(&frame(b'T', &row_description("n"))).expect("RowDescription");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");

        // Bind: Bind, Describe(P), Sync.
        for _ in 0..3 {
            let _ = read_tagged(&mut server);
        }
        server.write_all(&frame(b'2', b"")).expect("BindComplete");
        server.write_all(&frame(b'T', &row_description("n"))).expect("RowDescription");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");

        // First execute: Describe(P), Execute, Sync. Suspends after one row.
        for _ in 0..3 {
            let _ = read_tagged(&mut server);
        }
        server.write_all(&frame(b'T', &row_description("n"))).expect("RowDescription");
        server.write_all(&frame(b'D', &data_row("1"))).expect("DataRow");
        server.write_all(&frame(b's', b"")).expect("PortalSuspended");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");

        // Second execute: Describe(P), Execute, Sync. Finishes the portal.
        for _ in 0..3 {
            let _ = read_tagged(&mut server);
        }
        server.write_all(&frame(b'T', &row_description("n"))).expect("RowDescription");
        server.write_all(&frame(b'D', &data_row("2"))).expect("DataRow");
        server.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");

        // Close: Close, Sync.
        for _ in 0..2 {
            let _ = read_tagged(&mut server);
        }
        server.write_all(&frame(b'3', b"")).expect("CloseComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let prepared = connection.prepare("SELECT n", &[]).expect("prepare");
    let portal = connection.bind_portal(&prepared.name, "p1", &[], &[]).expect("bind portal");
    assert_eq!(portal.name, "p1");
    let first = connection.execute_portal("p1", 1).expect("first batch");
    assert!(first.suspended, "the portal must suspend at the row limit");
    assert_eq!(first.results[0].rows, vec![vec![Some("1".to_owned())]]);
    assert_eq!(first.results[0].tag, "", "PortalSuspended carries no command tag");
    let second = connection.execute_portal("p1", 1).expect("second batch");
    assert!(!second.suspended, "the second execute finishes the portal");
    assert_eq!(second.results[0].rows, vec![vec![Some("2".to_owned())]]);
    assert_eq!(second.results[0].tag, "SELECT 1");
    connection.close_portal("p1").expect("close portal");
    handle.join().expect("server thread");
    let _ = connection.close();
}

#[test]
fn binary_parameters_are_bound_as_bytes() {
    let (port, handle) = spawn_server(|listener| {
        let (mut server, _) = listener.accept().expect("session");
        read_startup(&mut server);
        startup_ok(&mut server, Some((33, 34)));

        let (tag, _payload) = read_tagged(&mut server);
        assert_eq!(tag, b'P');
        let (tag, payload) = read_tagged(&mut server);
        assert_eq!(tag, b'B');
        let mut cursor = 0;
        for _ in 0..2 {
            while payload[cursor] != 0 {
                cursor += 1;
            }
            cursor += 1;
        }
        let formats = u16::from_be_bytes([payload[cursor], payload[cursor + 1]]) as usize;
        cursor += 2 + formats * 2;
        let count = u16::from_be_bytes([payload[cursor], payload[cursor + 1]]) as usize;
        cursor += 2;
        assert_eq!(count, 1);
        assert_eq!(&payload[cursor..cursor + 8], &[0, 0, 0, 4, 0, 0, 0, 42]);

        let _describe = read_tagged(&mut server);
        let _execute = read_tagged(&mut server);
        let _sync = read_tagged(&mut server);
        server.write_all(&frame(b'1', b"")).expect("ParseComplete");
        server.write_all(&frame(b'2', b"")).expect("BindComplete");
        server.write_all(&frame(b'T', &row_description("n"))).expect("RowDescription");
        server.write_all(&frame(b'D', &data_row("42"))).expect("DataRow");
        server.write_all(&frame(b'C', b"SELECT 1\0")).expect("CommandComplete");
        server.write_all(&frame(b'Z', b"I")).expect("ReadyForQuery");
    });

    let mut connection = Connection::open(&config(port)).expect("open");
    let param = Param::binary(&[0, 0, 0, 42]);
    let results = connection.execute_with_param_formats("SELECT $1::int4", &[param], &[23], &[]).expect("binary parameter");
    assert_eq!(results[0].rows, vec![vec![Some("42".to_owned())]]);
    handle.join().expect("server thread");
    let _ = connection.close();
}
