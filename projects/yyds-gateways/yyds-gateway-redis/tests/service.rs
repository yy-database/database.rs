use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use yyds_control::{LeaseError, LocalOwnership, NodeIdentity, NodeLease};
use yyds_gateway_redis::{resp::RequestLimits, service::RedisService};

fn owner() -> (PathBuf, Arc<NodeLease>) {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "yyds-redis-owner-{}-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let owner = Arc::new(NodeLease::acquire(&directory, NodeIdentity::new("cluster-a", "node-a").unwrap()).unwrap());
    (directory, owner)
}

fn identity() -> NodeIdentity {
    NodeIdentity::new("cluster-a", "node-a").unwrap()
}

fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(3)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
    stream
}

#[test]
fn service_keeps_node_owned_until_all_workers_stop() {
    let (directory, owner) = owner();
    let service = RedisService::start(Arc::clone(&owner), "127.0.0.1:0".parse().unwrap(), RequestLimits::default()).unwrap();
    let address = service.address();
    let mut client = connect(address);
    client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut pong = [0; 7];
    client.read_exact(&mut pong).unwrap();
    assert_eq!(&pong, b"+PONG\r\n");
    assert!(!service.is_finished());
    drop(owner);
    assert!(matches!(NodeLease::acquire(&directory, identity()), Err(LeaseError::Busy)));
    client.write_all(b"*2\r\n$4\r\nECHO\r\n$99\r\npartial").unwrap();
    let started = std::time::Instant::now();
    service.stop().unwrap();
    assert!(started.elapsed() < Duration::from_secs(3), "stop must interrupt the 30-second idle timeout");
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Available);
    assert!(TcpStream::connect_timeout(&address, Duration::from_secs(1)).is_err());
    let mut probe = [0];
    assert!(!matches!(client.read(&mut probe), Ok(received) if received > 0));
    assert!(NodeLease::acquire(&directory, identity()).is_ok());
}

#[test]
fn failed_bind_does_not_leak_node_ownership() {
    let (directory, owner) = owner();
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    assert!(RedisService::start(owner, occupied.local_addr().unwrap(), RequestLimits::default()).is_err());
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Available);
}

#[test]
fn unauthenticated_listener_rejects_nonloopback() {
    let (directory, owner) = owner();
    let error = RedisService::start(owner, "0.0.0.0:0".parse().unwrap(), RequestLimits::default()).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Available);
}

#[test]
fn gateways_share_one_identity_and_drop_closes_clients() {
    let (directory, owner) = owner();
    let first = RedisService::start(Arc::clone(&owner), "127.0.0.1:0".parse().unwrap(), RequestLimits::default()).unwrap();
    let second = RedisService::start(owner, "127.0.0.1:0".parse().unwrap(), RequestLimits::default()).unwrap();
    let mut client = connect(first.address());
    client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut pong = [0; 7];
    client.read_exact(&mut pong).unwrap();
    drop(first);
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Held);
    drop(second);
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Available);
}

#[test]
fn stop_cancels_backpressured_binary_echo() {
    let (directory, owner) = owner();
    let payload_bytes = 4 * 1024 * 1024;
    let limits =
        RequestLimits { max_frame_bytes: payload_bytes + 1024, max_bulk_bytes: payload_bytes, ..RequestLimits::default() };
    let service = RedisService::start(owner, "127.0.0.1:0".parse().unwrap(), limits).unwrap();
    let mut client = connect(service.address());
    write!(client, "*2\r\n$4\r\nECHO\r\n${payload_bytes}\r\n").unwrap();
    client.write_all(&vec![255; payload_bytes]).unwrap();
    client.write_all(b"\r\n").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let started = std::time::Instant::now();
    service.stop().unwrap();
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}

#[test]
fn connection_limit_is_enforced_and_all_clients_stop() {
    let (directory, owner) = owner();
    let service = RedisService::start(owner, "127.0.0.1:0".parse().unwrap(), RequestLimits::default()).unwrap();
    let mut clients = Vec::new();
    for _ in 0..64 {
        let mut client = connect(service.address());
        client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        let mut pong = [0; 7];
        client.read_exact(&mut pong).unwrap();
        assert_eq!(&pong, b"+PONG\r\n");
        clients.push(client);
    }
    let mut excess = connect(service.address());
    let mut response = Vec::new();
    excess.read_to_end(&mut response).unwrap();
    assert_eq!(response, b"-ERR maximum client count reached\r\n");
    let started = std::time::Instant::now();
    service.stop().unwrap();
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}
