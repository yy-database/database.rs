//! Background periodic expiry reclamation.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use super::KeyValueExecutor;

/// Records inspected per shard per background pass.
///
/// Together with the interval this caps the sweeper's CPU share: at the default 100ms tick a
/// shard scans at most 10 * 256 = 2560 records per second, and an N-record shard is fully
/// inspected in roughly N / 2560 seconds. A pass that exhausts the budget resumes from its key
/// cursor on the next tick, so the tail of a large shard is never starved.
pub const DEFAULT_SWEEP_BUDGET_PER_SHARD: usize = 256;

/// Periodically reclaims expired records through a key-value executor.
///
/// The sweeper owns one worker thread that calls [`KeyValueExecutor::sweep_expired`] once
/// per `interval`, starting with an immediate first tick so records that elapsed while the
/// node was down are reclaimed at boot. A failed cycle is not fatal: reclamation goes
/// through each shard's durable commit boundary, so a failure can never corrupt a snapshot
/// and the worker simply retries on the next tick. Dropping the sweeper (or calling
/// [`ExpirySweeper::stop`]) signals the worker and joins it.
#[derive(Debug)]
pub struct ExpirySweeper {
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl ExpirySweeper {
    /// Starts a sweeper ticking every `interval` against `executor`.
    pub fn start(executor: Arc<dyn KeyValueExecutor>, interval: Duration) -> io::Result<Self> {
        let stopping = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&stopping);
        let worker = thread::Builder::new().name("cluster-expiry-sweeper".into()).spawn(move || {
            while !signal.load(Ordering::Acquire) {
                let _ = executor.sweep_expired(Some(DEFAULT_SWEEP_BUDGET_PER_SHARD));
                thread::sleep(interval);
            }
        })?;
        Ok(Self { stopping, worker: Some(worker) })
    }

    /// Stops the worker and waits for it to exit.
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for ExpirySweeper {
    fn drop(&mut self) {
        self.shutdown();
    }
}
