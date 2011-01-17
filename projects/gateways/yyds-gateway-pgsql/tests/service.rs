use std::{
    io::Write,
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use yyds_control::{LeaseError, LocalOwnership, NodeIdentity, NodeLease};
use yyds_gateway_pgsql::service::PgsqlService;

fn make_owner() -> (PathBuf, Arc<NodeLease>) {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "yyds-pgsql-owner-{}-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let identity = NodeIdentity::new("cluster-a", "node-a").unwrap();
    let owner = Arc::new(NodeLease::acquire(&directory, identity).unwrap());
    (directory, owner)
}

fn identity() -> NodeIdentity {
    NodeIdentity::new("cluster-a", "node-a").unwrap()
}

fn connect(address: SocketAddr) -> TcpStream {
    TcpStream::connect_timeout(&address, Duration::from_secs(3)).unwrap()
}

#[test]
fn service_keeps_shared_node_owned_and_stop_interrupts_partial_startup() {
    let (directory, owner) = make_owner();
    let service = PgsqlService::start(Arc::clone(&owner), "127.0.0.1:0".parse().unwrap()).unwrap();
    let address = service.address();
    let mut client = connect(address);
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    client.write_all(b"\0\0").unwrap();
    drop(owner);
    assert!(matches!(NodeLease::acquire(&directory, identity()), Err(LeaseError::Busy)));

    let started = std::time::Instant::now();
    service.stop().unwrap();
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Available);
    assert!(TcpStream::connect_timeout(&address, Duration::from_secs(1)).is_err());
}

#[test]
fn failed_bind_and_nonloopback_do_not_leak_node_ownership() {
    let (directory, owner) = make_owner();
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    assert!(PgsqlService::start(Arc::clone(&owner), occupied.local_addr().unwrap()).is_err());
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Held);
    drop(owner);

    let (directory, owner) = make_owner();
    let error = PgsqlService::start(owner, "0.0.0.0:0".parse().unwrap()).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}

#[test]
fn independent_protocol_services_share_one_node_lease() {
    let (directory, owner) = make_owner();
    let first = PgsqlService::start(Arc::clone(&owner), "127.0.0.1:0".parse().unwrap()).unwrap();
    let second = PgsqlService::start(owner, "127.0.0.1:0".parse().unwrap()).unwrap();
    drop(first);
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Held);
    drop(second);
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}
