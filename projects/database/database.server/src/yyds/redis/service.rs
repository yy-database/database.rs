//! Node-owned lifecycle for the bounded RESP2 protocol probe.

use std::{
    io::{self, Write},
    net::{SocketAddr, TcpListener},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use database_cluster::control::NodeLease;

use database_execution::cluster::KeyValueExecutor;

use super::{
    blocking::BlockingRegistry,
    connection::{KeyAccessTracker, serve_cancellable, serve_cancellable_with_executor_and_tracker},
    pubsub::PubSubRegistry,
    resp::RequestLimits,
    server_face::ClientRegistry,
};

const MAX_CLIENTS: usize = 64;

/// A loopback protocol listener attached to an existing node owner.
/// Dropping the service shuts down all connections and joins its workers.
#[derive(Debug)]
pub struct RedisService {
    address: SocketAddr,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<io::Result<()>>>,
}

impl RedisService {
    /// Starts a protocol probe without creating a new node or cluster identity.
    pub fn start(owner: Arc<NodeLease>, address: SocketAddr, limits: RequestLimits) -> io::Result<Self> {
        Self::start_inner(owner, address, limits, None)
    }

    /// Starts a Redis listener bound to an existing YYDS key/value executor.
    pub fn start_with_executor(
        owner: Arc<NodeLease>,
        address: SocketAddr,
        limits: RequestLimits,
        executor: Arc<dyn KeyValueExecutor>,
    ) -> io::Result<Self> {
        Self::start_inner(owner, address, limits, Some(executor))
    }

    fn start_inner(
        owner: Arc<NodeLease>,
        address: SocketAddr,
        limits: RequestLimits,
        executor: Option<Arc<dyn KeyValueExecutor>>,
    ) -> io::Result<Self> {
        if !address.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Redis requires loopback until authentication and TLS are implemented",
            ));
        }
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let stopping = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&stopping);
        let worker = thread::Builder::new().name("yyds-redis-listener".into()).spawn(move || {
            let _owner = owner;
            listen(listener, signal, limits, executor)
        })?;
        Ok(Self { address, stopping, worker: Some(worker) })
    }

    /// Actual bound address, including the assigned port when configured with zero.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Whether the accept worker has exited, not database or cluster health.
    pub fn is_finished(&self) -> bool {
        self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Interrupts sockets and waits for all owned threads to exit.
    pub fn stop(mut self) -> io::Result<()> {
        self.shutdown()
    }

    fn shutdown(&mut self) -> io::Result<()> {
        self.stopping.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| io::Error::other("Redis accept worker panicked"))??;
        }
        Ok(())
    }
}

impl Drop for RedisService {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[derive(Debug)]
struct Clients {
    workers: Vec<JoinHandle<()>>,
    stopping: Arc<AtomicBool>,
}

impl Clients {
    fn reap(&mut self) {
        let mut index = 0;
        while index < self.workers.len() {
            if self.workers[index].is_finished() {
                let worker = self.workers.swap_remove(index);
                let _ = worker.join();
            }
            else {
                index += 1;
            }
        }
    }
}

impl Drop for Clients {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn listen(
    listener: TcpListener,
    stopping: Arc<AtomicBool>,
    limits: RequestLimits,
    executor: Option<Arc<dyn KeyValueExecutor>>,
) -> io::Result<()> {
    // Access timestamps backing `OBJECT IDLETIME` are per object, so every connection of a
    // listener shares one tracker. They live in memory only and reset when the listener
    // restarts, matching an RDB load that does not persist access time.
    let tracker = Arc::new(KeyAccessTracker::new());
    // Blocking commands wait for pushes performed by other connections, so the wait table
    // has to be listener-scoped like the access tracker.
    let registry = Arc::new(BlockingRegistry::new());
    // Channel and pattern subscriptions are reached from the publishing connection, so the
    // pub/sub table is listener-scoped for the same reason.
    let pubsub = Arc::new(PubSubRegistry::new());
    // `CLIENT LIST` / `CLIENT KILL` / `CONFIG` / `INFO stats` are server-wide views, so the
    // connection table and its counters are listener-scoped like the tables above.
    let bound = listener.local_addr()?;
    let client_registry = Arc::new(ClientRegistry::new(bound.port(), bound.ip().to_string()));
    let mut clients = Clients { workers: Vec::new(), stopping: Arc::clone(&stopping) };
    while !stopping.load(Ordering::Acquire) {
        clients.reap();
        match listener.accept() {
            Ok((mut stream, _)) => {
                if stopping.load(Ordering::Acquire) {
                    break;
                }
                stream.set_nonblocking(false)?;
                if clients.workers.len() >= MAX_CLIENTS {
                    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
                    let _ = stream.write_all(b"-ERR maximum client count reached\r\n");
                    continue;
                }
                let cancellation = Arc::clone(&stopping);
                let connection_executor = executor.clone();
                let connection_tracker = Arc::clone(&tracker);
                let connection_registry = Arc::clone(&registry);
                let connection_pubsub = Arc::clone(&pubsub);
                let connection_clients = Arc::clone(&client_registry);
                let worker = thread::Builder::new().name("yyds-redis-client".into()).spawn(move || {
                    let _ = match connection_executor {
                        Some(executor) => serve_cancellable_with_executor_and_tracker(
                            stream,
                            limits,
                            cancellation,
                            executor,
                            connection_tracker,
                            connection_registry,
                            connection_pubsub,
                            Some(connection_clients),
                        ),
                        None => serve_cancellable(stream, limits, cancellation),
                    };
                })?;
                clients.workers.push(worker);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(5)),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
