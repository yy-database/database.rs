use std::{
    io::Read,
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use yyds_control::{LeaseError, LocalOwnership, NodeIdentity, NodeLease};
use yyds_gateway_mysql::service::MysqlService;

fn make_owner() -> (PathBuf, Arc<NodeLease>) {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "yyds-mysql-owner-{}-{}-{}",
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
    stream
}

#[test]
fn stop_interrupts_partial_startup_before_releasing_node_lease() {
    let (directory, owner) = make_owner();
    let service = MysqlService::start(owner, "127.0.0.1:0".parse().unwrap(), None).unwrap();
    let address = service.address();
    let mut client = connect(address);
    let mut header = [0; 4];
    client.read_exact(&mut header).unwrap();
    let mut handshake = vec![0; usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16)];
    client.read_exact(&mut handshake).unwrap();
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Held);

    let started = std::time::Instant::now();
    service.stop().unwrap();
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Available);
    assert!(TcpStream::connect_timeout(&address, Duration::from_secs(1)).is_err());
    drop(client);
}

#[test]
fn failed_bind_and_nonloopback_do_not_leak_node_lease() {
    let (directory, owner) = make_owner();
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    assert!(MysqlService::start(Arc::clone(&owner), occupied.local_addr().unwrap(), None).is_err());
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Held);
    drop(owner);

    let (directory, owner) = make_owner();
    let error = MysqlService::start(owner, "0.0.0.0:0".parse().unwrap(), None).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}

#[test]
fn multiple_protocol_listeners_can_share_the_same_node_identity() {
    let (directory, owner) = make_owner();
    let first = MysqlService::start(Arc::clone(&owner), "127.0.0.1:0".parse().unwrap(), None).unwrap();
    let second = MysqlService::start(owner, "127.0.0.1:0".parse().unwrap(), None).unwrap();
    drop(first);
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Held);
    drop(second);
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}
