//! 阻塞命令的跨连接等待表。
//!
//! 监听器是「每连接一线程」模型，阻塞命令不能占用读循环去轮询存储，所以等待必须
//! 由一张监听器级共享表来协调：等待者按 `(逻辑库, 键)` 排队，推入数据的连接负责把
//! 该键的队首唤醒。队首判定在表内完成，因此同一键上的等待者严格按注册顺序被服务
//! （FIFO），而不是靠醒来后互相抢。
//!
//! 每次推入都会让该键的 `generation` 前进一格。等待者的判定是「我是队首，且
//! generation 与我登记时看到的不同」——两个条件都在同一把锁下读取，推入方也在锁内
//! 递增，所以不存在丢失唤醒的窗口。队首被服务后注销并广播一次，让下一个等待者立刻
//! 变队首重新检查；没有数据时它会把新的 generation 记下来继续睡。

use std::{
    collections::{HashMap, VecDeque},
    sync::{Condvar, Mutex},
    time::{Duration, Instant},
};

use database_types::cluster::Namespace;

/// 等待表内一个键的排队标识。
#[derive(Debug, Clone)]
pub(crate) struct Waiter {
    id: u64,
    namespace: Namespace,
    key: Vec<u8>,
    /// 登记时看到的 generation；醒来后只有它前进过才值得重新尝试。
    generation: u64,
}

/// `wait_any` 的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WaitOutcome {
    /// 至少一个键到了队首且自登记后有新数据，调用方应重新尝试取元素。
    Signaled,
    /// 截止时间先到。
    TimedOut,
}

/// 监听器级共享的阻塞等待表。
#[derive(Debug, Default)]
pub struct BlockingRegistry {
    state: Mutex<RegistryState>,
    signal: Condvar,
}

#[derive(Debug, Default)]
struct RegistryState {
    queues: HashMap<(Namespace, Vec<u8>), VecDeque<u64>>,
    generations: HashMap<(Namespace, Vec<u8>), u64>,
    next_id: u64,
}

impl BlockingRegistry {
    /// 创建一个空等待表，由监听器启动时创建并在所有连接间共享。
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 把一个等待者登记到 `(namespace, key)` 队尾，并记下当前 generation。
    pub(crate) fn register(&self, namespace: &Namespace, key: &[u8]) -> Waiter {
        let mut state = self.lock();
        let id = state.next_id;
        state.next_id += 1;
        let slot = (namespace.clone(), key.to_vec());
        let generation = *state.generations.get(&slot).unwrap_or(&0);
        state.queues.entry(slot).or_default().push_back(id);
        Waiter { id, namespace: namespace.clone(), key: key.to_vec(), generation }
    }

    /// 该等待者是否是它所在键的队首。只有队首有权取元素，FIFO 由此保证。
    pub(crate) fn is_head(&self, waiter: &Waiter) -> bool {
        let state = self.lock();
        state.queues.get(&(waiter.namespace.clone(), waiter.key.clone())).and_then(|queue| queue.front()) == Some(&waiter.id)
    }

    /// 注销一组等待者（一个客户端可能同时排在多个键上），并唤醒其余等待者重新判定队首。
    pub(crate) fn unregister_all(&self, waiters: &[Waiter]) {
        let mut state = self.lock();
        for waiter in waiters {
            let slot = (waiter.namespace.clone(), waiter.key.clone());
            if let Some(queue) = state.queues.get_mut(&slot) {
                if let Some(position) = queue.iter().position(|id| *id == waiter.id) {
                    queue.remove(position);
                }
                if queue.is_empty() {
                    state.queues.remove(&slot);
                }
            }
        }
        self.signal.notify_all();
    }

    /// 宣告 `(namespace, key)` 有新数据写入：generation 前进一格并广播。
    pub(crate) fn notify(&self, namespace: &Namespace, key: &[u8]) {
        let mut state = self.lock();
        *state.generations.entry((namespace.clone(), key.to_vec())).or_insert(0) += 1;
        self.signal.notify_all();
    }

    /// 睡到「某个等待者轮到自己且有新数据」或截止时间。
    ///
    /// `deadline` 为 `None` 表示无限等待，对应 Redis 的超时 `0`。返回 `Signaled` 前会把
    /// 相关等待者看到的 generation 推到当前值，避免调用方重新检查时立刻再次返回。
    pub(crate) fn wait_any(&self, waiters: &mut [Waiter], deadline: Option<Instant>) -> WaitOutcome {
        let mut state = self.lock();
        loop {
            let now = Instant::now();
            if deadline.is_some_and(|deadline| now >= deadline) {
                return WaitOutcome::TimedOut;
            }
            let mut signaled = false;
            for waiter in waiters.iter_mut() {
                let slot = (waiter.namespace.clone(), waiter.key.clone());
                let generation = *state.generations.get(&slot).unwrap_or(&0);
                let head = state.queues.get(&slot).and_then(|queue| queue.front()) == Some(&waiter.id);
                if head && generation != waiter.generation {
                    waiter.generation = generation;
                    signaled = true;
                }
            }
            if signaled {
                return WaitOutcome::Signaled;
            }
            state = match deadline {
                None => self.signal.wait(state).unwrap_or_else(|poisoned| poisoned.into_inner()),
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return WaitOutcome::TimedOut;
                    }
                    let remaining = deadline - now;
                    self.signal.wait_timeout(state, remaining).unwrap_or_else(|poisoned| poisoned.into_inner()).0
                }
            };
        }
    }
}

/// 把「超时 0」与有限时长区分开：`None` 表示无限等待。
pub(crate) fn timeout_deadline(timeout: Option<Duration>) -> Option<Instant> {
    timeout.and_then(|timeout| Instant::now().checked_add(timeout))
}
