//! Node-owned lifecycle for the MySQL protocol gateway.

use std::{
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use database_cluster::control::NodeLease;
use database_execution::cluster::KeyValueExecutor;
use database_query::{SqlCatalog, SqlSchemaRegistry};

use super::connection::{MysqlSqlRuntime, serve_cancellable};

const MAX_CLIENTS: usize = 64;

/// A loopback MySQL listener attached to an existing node owner.
#[derive(Debug)]
pub struct MysqlService {
    address: SocketAddr,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<io::Result<()>>>,
}

impl MysqlService {
    /// Starts a listener without creating a node or cluster identity.
    pub fn start(owner: Arc<NodeLease>, address: SocketAddr, runtime: Option<Arc<MysqlSqlRuntime>>) -> io::Result<Self> {
        if !address.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "MySQL requires loopback until authentication and TLS are implemented",
            ));
        }
        // 阻塞 `accept`：新连接一到达内核立刻唤醒监听线程。历史上用非阻塞轮询
        // 加固定 5ms 睡眠，会把每次连接建立的延迟钉在 0-5ms，而握手本身更便宜。
        let listener = TcpListener::bind(address)?;
        let address = listener.local_addr()?;
        let stopping = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&stopping);
        let worker = thread::Builder::new().name("yyds-mysql-listener".into()).spawn(move || {
            let _owner = owner;
            listen(listener, signal, runtime)
        })?;
        Ok(Self { address, stopping, worker: Some(worker) })
    }

    /// Starts a listener with a YYDS SQL runtime for SELECT, DML, and DDL execution.
    pub fn start_with_registry(
        owner: Arc<NodeLease>,
        address: SocketAddr,
        executor: Arc<dyn KeyValueExecutor>,
        schema: Arc<SqlSchemaRegistry>,
    ) -> io::Result<Self> {
        let runtime = Arc::new(MysqlSqlRuntime { schema, executor });
        Self::start(owner, address, Some(runtime))
    }

    /// Starts a listener seeded from a published catalog.
    ///
    /// DDL applies to an in-memory registry and is not persisted.
    pub fn start_with_executor(
        owner: Arc<NodeLease>,
        address: SocketAddr,
        executor: Arc<dyn KeyValueExecutor>,
        catalog: SqlCatalog,
    ) -> io::Result<Self> {
        Self::start_with_executor_lane(owner, address, executor, catalog, 0)
    }

    /// Starts a listener seeded from a published catalog in one DDL identity lane.
    ///
    /// MySQL uses lane 1 so its DDL tables never share a type id with the
    /// PostgreSQL gateway that was seeded from the same catalog.
    pub fn start_with_executor_lane(
        owner: Arc<NodeLease>,
        address: SocketAddr,
        executor: Arc<dyn KeyValueExecutor>,
        catalog: SqlCatalog,
        lane: u8,
    ) -> io::Result<Self> {
        let schema = SqlSchemaRegistry::from_catalog_with_lane(&catalog, lane).map_err(|error| io::Error::other(error.message))?;
        Self::start_with_registry(owner, address, executor, Arc::new(schema))
    }

    /// Actual bound address, including the assigned port when configured with zero.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Whether the accept worker has exited, not database or cluster health.
    pub fn is_finished(&self) -> bool {
        self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Interrupts connections and joins all service workers.
    pub fn stop(mut self) -> io::Result<()> {
        self.shutdown()
    }

    fn shutdown(&mut self) -> io::Result<()> {
        self.stopping.store(true, Ordering::Release);
        // 监听线程阻塞在 `accept`，用一个自身的空连接把它唤醒；唤醒连接随后
        // 在循环顶部看到停机标志被丢弃，不影响已建立的会话。
        let _ = TcpStream::connect_timeout(&self.address, Duration::from_millis(500));
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| io::Error::other("MySQL accept worker panicked"))??;
        }
        Ok(())
    }
}

impl Drop for MysqlService {
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

fn listen(listener: TcpListener, stopping: Arc<AtomicBool>, runtime: Option<Arc<MysqlSqlRuntime>>) -> io::Result<()> {
    let mut clients = Clients { workers: Vec::new(), stopping: Arc::clone(&stopping) };
    while !stopping.load(Ordering::Acquire) {
        clients.reap();
        match listener.accept() {
            Ok((stream, _)) => {
                if stopping.load(Ordering::Acquire) {
                    break;
                }
                if clients.workers.len() >= MAX_CLIENTS {
                    drop(stream);
                    continue;
                }
                let cancellation = Arc::clone(&stopping);
                let runtime = runtime.clone();
                let worker = thread::Builder::new().name("yyds-mysql-client".into()).spawn(move || {
                    let _ = serve_cancellable(stream, cancellation, runtime);
                })?;
                clients.workers.push(worker);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
