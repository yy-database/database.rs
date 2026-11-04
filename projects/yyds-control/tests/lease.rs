use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use yyds_control::{LeaseError, LocalOwnership, NodeIdentity, NodeLease};

fn directory() -> PathBuf {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "yyds-control-{}-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn identity() -> NodeIdentity {
    NodeIdentity::new("cluster-a", "node-1").unwrap()
}

#[test]
fn identity_validation() {
    for invalid in ["", "../escape", "with space", "案例", ".", "a/b", "a\\b"] {
        assert!(matches!(NodeIdentity::new(invalid, "node"), Err(LeaseError::InvalidIdentity)));
        assert!(matches!(NodeIdentity::new("cluster", invalid), Err(LeaseError::InvalidIdentity)));
    }
    assert!(NodeIdentity::new("a".repeat(129), "node").is_err());
    assert!(NodeIdentity::new("Cluster_1-A", "Node_2-B").is_ok());
}

#[test]
fn owns_directory_and_restarts_with_the_same_identity() {
    let directory = directory();
    let lease = NodeLease::acquire(&directory, identity()).unwrap();
    assert_eq!(lease.identity(), &identity());
    assert_eq!(lease.directory(), fs::canonicalize(&directory).unwrap());
    let original = fs::read(directory.join("node.json")).unwrap();
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Held);
    assert!(matches!(NodeLease::acquire(&directory, identity()), Err(LeaseError::Busy)));
    drop(lease);
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Available);
    let restarted = NodeLease::acquire(&directory, identity()).unwrap();
    assert_eq!(fs::read(directory.join("node.json")).unwrap(), original);
    assert!(directory.join("node.lock").exists());
    drop(restarted);
}

#[test]
fn mismatches_release_ownership_without_reassigning_identity() {
    let directory = directory();
    drop(NodeLease::acquire(&directory, identity()).unwrap());
    let original = fs::read(directory.join("node.json")).unwrap();
    for other in [NodeIdentity::new("cluster-b", "node-1").unwrap(), NodeIdentity::new("cluster-a", "node-2").unwrap()] {
        assert!(matches!(NodeLease::acquire(&directory, other), Err(LeaseError::IdentityMismatch)));
        assert_eq!(fs::read(directory.join("node.json")).unwrap(), original);
        assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Available);
    }
    assert!(NodeLease::acquire(&directory, identity()).is_ok());
}

#[test]
fn corrupt_identity_is_never_reinitialized() {
    let directory = directory();
    drop(NodeLease::acquire(&directory, identity()).unwrap());
    for bytes in [
        b"".to_vec(),
        b"{".to_vec(),
        vec![b'x'; 4097],
        br#"{"version":2,"identity":{"clusterId":"cluster-a","nodeId":"node-1"}}"#.to_vec(),
        br#"{"version":1,"identity":{"clusterId":"","nodeId":"node-1"}}"#.to_vec(),
        br#"{"version":1,"identity":{"clusterId":"cluster-a","nodeId":"node-1"},"extra":true}"#.to_vec(),
    ] {
        fs::write(directory.join("node.json"), &bytes).unwrap();
        assert!(matches!(NodeLease::acquire(&directory, identity()), Err(LeaseError::CorruptIdentity)));
        assert!(matches!(NodeLease::inspect(&directory), Err(LeaseError::CorruptIdentity)));
        assert_eq!(fs::read(directory.join("node.json")).unwrap(), bytes);
    }
}

#[test]
fn inspection_does_not_initialize_a_directory() {
    let directory = directory();
    assert!(NodeLease::inspect(&directory).is_err());
    assert!(!directory.exists());
    fs::create_dir_all(&directory).unwrap();
    assert!(NodeLease::inspect(&directory).is_err());
    assert!(!directory.join("node.lock").exists());
    assert!(!directory.join("node.json").exists());
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn lease_child() {
    let Some(directory) = std::env::var_os("YYDS_LEASE_TEST_DIR")
    else {
        return;
    };
    let _lease = NodeLease::acquire(directory, identity()).unwrap();
    println!("LEASE_READY");
    std::io::stdout().flush().unwrap();
    let mut command = String::new();
    std::io::stdin().read_line(&mut command).unwrap();
    assert_eq!(command.trim(), "stop");
}

fn child(directory: &PathBuf) -> ChildGuard {
    let mut child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "lease_child", "--nocapture"])
            .env("YYDS_LEASE_TEST_DIR", directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let output = child.0.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            if line.unwrap().contains("LEASE_READY") {
                let _ = sender.send(());
            }
        }
    });
    receiver.recv_timeout(Duration::from_secs(10)).expect("bounded child startup");
    child
}

#[test]
fn cross_process_exclusion_and_crash_release() {
    let directory = directory();
    let mut process = child(&directory);
    assert!(matches!(NodeLease::acquire(&directory, identity()), Err(LeaseError::Busy)));
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Held);
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    assert_eq!(NodeLease::inspect(&directory).unwrap().ownership, LocalOwnership::Available);
    assert!(NodeLease::acquire(&directory, identity()).is_ok());
}

#[test]
fn cross_process_orderly_stop_and_restart() {
    let directory = directory();
    let mut process = child(&directory);
    writeln!(process.0.stdin.take().unwrap(), "stop").unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(std::time::Instant::now() < deadline, "bounded child stop");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(NodeLease::inspect(&directory).unwrap().identity, identity());
    assert!(NodeLease::acquire(&directory, identity()).is_ok());
}
