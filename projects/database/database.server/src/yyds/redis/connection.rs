//! RESP2 / RESP3 gateway protocol with transaction queueing.
//!
//! A connection starts on RESP2 and switches to RESP3 when the client sends `HELLO 3`.
//! The negotiated version decides how null replies are spelled: RESP2 uses the null bulk
//! string `$-1\r\n` while RESP3 uses `_\r\n`, and a RESP3 parser rejects the RESP2 form,
//! so the version has to live on the connection rather than be assumed per reply.
//!
//! Key expiry lives in the KV layer: records carry a wall-clock Unix-millisecond
//! deadline, so TTLs are shared by every connection of a listener and survive a
//! listener restart.

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::TcpStream,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use database_execution::cluster::KeyValueExecutor;
use database_query::cluster_kv::{KeyValueAction, KeyValueCommand, KeyValueResult};
use database_storage::cluster_kv::now_unix_ms;
use database_types::cluster::{FenceToken, Namespace};

use super::{
    bind::{BindError, BoundAction, BoundCommand, SetCondition, SetExpiry, bind, bind_mget, bind_set},
    collections::{self, KeyKind},
    resp::{Request, RequestLimits, decode_request},
};

/// 协议绑定结果 → 执行层键值命令。
fn to_kv_command(command: BoundCommand) -> KeyValueCommand {
    KeyValueCommand {
        namespace: Namespace(command.namespace),
        key: command.key,
        action: match command.action {
            BoundAction::Get => KeyValueAction::Get,
            BoundAction::Exists => KeyValueAction::Exists,
            BoundAction::GetDelete => KeyValueAction::GetDelete,
            BoundAction::GetSet(value) => KeyValueAction::GetSet(value),
            BoundAction::Put(value) => KeyValueAction::Put(value),
            BoundAction::PutIfAbsent(value) => KeyValueAction::PutIfAbsent(value),
            BoundAction::PutIfPresent(value) => KeyValueAction::PutIfPresent(value),
            BoundAction::Delete => KeyValueAction::Delete,
            BoundAction::IncrementBy(delta) => KeyValueAction::IncrementBy(delta),
        },
    }
}

/// A `Request` whose arguments have been copied into owned buffers so it can outlive the
/// RESP2 frame that decoded it. Used inside the MULTI transaction queue where commands are
/// held and replayed later.
#[derive(Debug)]
struct OwnedRequest {
    arguments: Vec<Vec<u8>>,
}

impl OwnedRequest {
    /// Re-borrows the arguments as a `Request<'static>` pointing at our owned storage.
    fn as_request(&self) -> Request<'_> {
        Request { arguments: self.arguments.iter().map(|v| v.as_slice()).collect(), consumed: 0 }
    }
}

/// One command buffered inside a `MULTI` block.
#[derive(Debug)]
struct QueuedCommand {
    request: OwnedRequest,
}

/// One key pinned by `WATCH`, together with the value it had when the watch was taken.
///
/// The snapshot *is* the version used for optimistic locking: `EXEC` compares it against
/// the current value through the executor and abandons the transaction on any difference.
/// Comparing bytes (rather than a server-side revision counter) keeps the watch state
/// session-scoped without adding a revision read to the KV contract, at the cost of not
/// noticing a write that stores byte-identical content.
#[derive(Debug, Clone)]
struct WatchedKey {
    /// Namespace (`SELECT` target) the key was watched in. Watches are per-database, like
    /// upstream Redis.
    namespace: Namespace,
    /// Opaque key bytes.
    key: Vec<u8>,
    /// Value observed at watch time (`None` means the key was absent).
    snapshot: Option<Vec<u8>>,
}

/// Per-connection state carried across RESP2 frames.
///
/// Session-scoped by design: transaction queueing, the watch list and the client name
/// belong to a single connection. Key expiry is *not* here — it is stored with each KV
/// record and therefore shared by every connection and durable across restarts.
#[derive(Debug)]
struct ConnectionState {
    /// Protocol version negotiated through `HELLO`. Starts at 2 and becomes 3 once the
    /// client asks for it. Null and map replies are encoded with this version.
    protocol: u8,
    /// Whether the connection is currently inside a `MULTI` block.
    in_multi: bool,
    /// Commands queued inside the active `MULTI` block.
    queue: Vec<QueuedCommand>,
    /// Name assigned by the most recent `CLIENT SETNAME`, reported by
    /// `CLIENT GETNAME`. Session-scoped, like the upstream client name.
    client_name: Option<Vec<u8>>,
    /// Keys pinned by `WATCH`, in first-watch order. Cleared by `UNWATCH`, `DISCARD`
    /// and every `EXEC`, including an aborted one.
    watched: Vec<WatchedKey>,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self { protocol: 2, in_multi: false, queue: Vec::new(), client_name: None, watched: Vec::new() }
    }
}

impl ConnectionState {
    fn new() -> Self {
        Self::default()
    }
}

/// 进程内键访问计时器，供 `OBJECT IDLETIME` 使用。
///
/// 只保存每个键「最近一次命中」的单调时钟时刻，不写入 `.yykv`：`Record` 与 v3 落盘
/// 格式都不带访问元数据，塞进记录会改变落盘字节并破坏格式兼容。计时器因此随监听器
/// 进程存在，重启后归零——这与真机一致，Redis 的 RDB 同样不保存访问时间，加载后对象
/// 从零重新计时。
#[derive(Debug, Default)]
pub struct KeyAccessTracker {
    last_access: Mutex<HashMap<(Namespace, Vec<u8>), Instant>>,
}

impl KeyAccessTracker {
    /// 创建一个空计时器，通常由监听器启动时创建并在各连接间共享。
    pub fn new() -> Self {
        Self::default()
    }

    /// 记录一次命中，把该键的空闲起点推进到当前时刻。
    fn mark(&self, namespace: &Namespace, key: &[u8]) {
        let mut entries = self.last_access.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        entries.insert((namespace.clone(), key.to_vec()), Instant::now());
    }

    /// 键不再可见时丢弃记录，避免同名键重新创建后读出上一轮的空闲时长。
    fn forget(&self, namespace: &Namespace, key: &[u8]) {
        let mut entries = self.last_access.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        entries.remove(&(namespace.clone(), key.to_vec()));
    }

    /// 空闲秒数。没有记录的键按「刚刚加载」处理并返回 `0`，对应重启后的起始状态。
    fn idle_seconds(&self, namespace: &Namespace, key: &[u8]) -> u64 {
        let entries = self.last_access.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        entries.get(&(namespace.clone(), key.to_vec())).map_or(0, |last| last.elapsed().as_secs())
    }
}

/// 包装执行器，在每条键值命令返回后更新访问计时。
///
/// 读命中与写命中统一在这里刷新空闲起点，不必在每个协议分支里各写一次 `mark`。键变为
/// 不可见时丢弃记录。`OBJECT` 子命令改用未包装的执行器查键，因此保持真机的
/// `LOOKUP_NOTOUCH` 语义：查询空闲时间不会把空闲时间清零。
#[derive(Debug)]
struct TrackedExecutor<'a> {
    inner: &'a dyn KeyValueExecutor,
    tracker: &'a KeyAccessTracker,
}

impl TrackedExecutor<'_> {
    /// 依据一次键值命令的结果更新计时。执行失败时保持原记录，不猜测存储状态。
    fn observe(&self, namespace: &Namespace, key: &[u8], destination: Option<&[u8]>, result: &KeyValueResult) {
        match result {
            KeyValueResult::Get(Some(_))
            | KeyValueResult::GetSet(_)
            | KeyValueResult::Put { .. }
            // `PutIfAbsent` 返回 `None` 表示旧键仍在，条件写失败本身也是一次命中。
            | KeyValueResult::PutIfAbsent { .. }
            | KeyValueResult::Increment { .. }
            | KeyValueResult::Exists(true) => self.tracker.mark(namespace, key),
            KeyValueResult::PutIfPresent { revision: Some(_) } => self.tracker.mark(namespace, key),
            KeyValueResult::Get(None)
            | KeyValueResult::Exists(false)
            | KeyValueResult::GetDelete(_)
            | KeyValueResult::Delete { .. }
            | KeyValueResult::PutIfPresent { revision: None } => self.tracker.forget(namespace, key),
            KeyValueResult::Expiry { present, .. } => {
                if *present {
                    self.tracker.mark(namespace, key)
                }
                else {
                    self.tracker.forget(namespace, key)
                }
            }
            KeyValueResult::ExpiryChanged { applied } => {
                // 未生效时无法区分「键不存在」与「本来就没有过期时间」，保留原记录。
                if *applied {
                    self.tracker.mark(namespace, key)
                }
            }
            KeyValueResult::Rename { source_present, applied } => {
                if *applied {
                    self.tracker.forget(namespace, key);
                    if let Some(destination) = destination {
                        self.tracker.mark(namespace, destination);
                    }
                }
                else if *source_present {
                    self.tracker.mark(namespace, key);
                }
                else {
                    self.tracker.forget(namespace, key);
                }
            }
        }
    }
}

impl KeyValueExecutor for TrackedExecutor<'_> {
    fn execute(&self, fence: Option<&FenceToken>, command: KeyValueCommand) -> database_types::cluster::Result<KeyValueResult> {
        // 命令会被移交给内层执行器，计时所需字段先复制出来。
        let namespace = command.namespace.clone();
        let key = command.key.clone();
        let destination = match &command.action {
            KeyValueAction::Rename { destination, .. } => Some(destination.clone()),
            _ => None,
        };
        let result = self.inner.execute(fence, command);
        if let Ok(value) = &result {
            self.observe(&namespace, &key, destination.as_deref(), value);
        }
        result
    }

    fn execute_read_batch(
        &self,
        fence: Option<&FenceToken>,
        commands: &[KeyValueCommand],
    ) -> database_types::cluster::Result<Vec<KeyValueResult>> {
        let result = self.inner.execute_read_batch(fence, commands);
        if let Ok(results) = &result {
            for (command, value) in commands.iter().zip(results) {
                self.observe(&command.namespace, &command.key, None, value);
            }
        }
        result
    }

    fn execute_scan(
        &self,
        fence: Option<&FenceToken>,
        namespace: &Namespace,
        prefix: &[u8],
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> database_types::cluster::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // 扫描不指向单个键，保持原样透传。
        self.inner.execute_scan(fence, namespace, prefix, after, before, limit)
    }
}

/// Serves one bounded RESP2 connection with a 30-second I/O timeout.
/// Only PING, ECHO and QUIT are supported. Database commands require a binder.
pub fn serve_connection(stream: TcpStream, limits: RequestLimits) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    serve_io(stream, limits, None, Arc::new(KeyAccessTracker::new()))
}

/// Serves a node-owned connection with cooperative cancellation of all socket I/O.
pub fn serve_cancellable(stream: TcpStream, limits: RequestLimits, stopping: Arc<AtomicBool>) -> io::Result<()> {
    stream.set_nonblocking(true)?;
    serve_io(CancellableSocket { stream, stopping }, limits, None, Arc::new(KeyAccessTracker::new()))
}

/// Serves a node-owned RESP2 connection using the YYDS key/value execution contract.
pub fn serve_cancellable_with_executor(
    stream: TcpStream,
    limits: RequestLimits,
    stopping: Arc<AtomicBool>,
    executor: Arc<dyn KeyValueExecutor>,
) -> io::Result<()> {
    stream.set_nonblocking(true)?;
    serve_io(CancellableSocket { stream, stopping }, limits, Some(executor), Arc::new(KeyAccessTracker::new()))
}

/// Serves a node-owned RESP2 connection while sharing one access tracker with its listener.
///
/// The tracker holds `OBJECT IDLETIME` state, which is per-object in Redis and therefore
/// has to be shared by every connection of the same listener rather than recreated per
/// socket.
pub fn serve_cancellable_with_executor_and_tracker(
    stream: TcpStream,
    limits: RequestLimits,
    stopping: Arc<AtomicBool>,
    executor: Arc<dyn KeyValueExecutor>,
    tracker: Arc<KeyAccessTracker>,
) -> io::Result<()> {
    stream.set_nonblocking(true)?;
    serve_io(CancellableSocket { stream, stopping }, limits, Some(executor), tracker)
}

struct CancellableSocket {
    stream: TcpStream,
    stopping: Arc<AtomicBool>,
}

impl CancellableSocket {
    fn attempt<T>(&mut self, mut operation: impl FnMut(&mut TcpStream) -> io::Result<T>) -> io::Result<T> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if self.stopping.load(Ordering::Acquire) {
                return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "node service stopping"));
            }
            match operation(&mut self.stream) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "socket I/O timeout"));
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => return result,
            }
        }
    }
}

impl Read for CancellableSocket {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.attempt(|stream| stream.read(bytes))
    }
}

impl Write for CancellableSocket {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.attempt(|stream| stream.write(bytes))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.attempt(Write::flush)
    }
}

fn serve_io(
    mut stream: impl Read + Write,
    limits: RequestLimits,
    executor: Option<Arc<dyn KeyValueExecutor>>,
    tracker: Arc<KeyAccessTracker>,
) -> io::Result<()> {
    let mut pending = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut namespace = Namespace("redis:0".into());
    let mut state = ConnectionState::new();
    // 所有普通命令走包装执行器以刷新访问计时，`OBJECT` 与 `WATCH` 需要不记时地查键，
    // 因此另保留一个未包装的执行器视图。
    let tracked = executor.as_deref().map(|inner| TrackedExecutor { inner, tracker: tracker.as_ref() });
    let tracked_view: Option<&dyn KeyValueExecutor> = tracked.as_ref().map(|wrapper| wrapper as &dyn KeyValueExecutor);
    loop {
        loop {
            match decode_request(&pending, limits) {
                Ok(Some(request)) => {
                    let consumed = request.consumed;
                    let result = respond(
                        &mut stream as &mut dyn Write,
                        &request,
                        tracked_view,
                        executor.as_deref(),
                        tracker.as_ref(),
                        &mut namespace,
                        &mut state,
                    );
                    if !result? {
                        return Ok(());
                    }
                    pending.drain(..consumed);
                }
                Ok(None) => break,
                Err(_) => {
                    stream.write_all(b"-ERR Protocol error or request limit exceeded\r\n")?;
                    return Ok(());
                }
            }
        }
        let available = limits.max_frame_bytes.saturating_sub(pending.len()).min(chunk.len());
        if available == 0 {
            stream.write_all(b"-ERR Protocol error or request limit exceeded\r\n")?;
            return Ok(());
        }
        let received = match stream.read(&mut chunk[..available]) {
            Ok(received) => received,
            Err(error) if matches!(error.kind(), io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock) => return Ok(()),
            Err(error) => return Err(error),
        };
        if received == 0 {
            if !pending.is_empty() {
                stream.write_all(b"-ERR Truncated request\r\n")?
            }
            return Ok(());
        }
        pending.extend_from_slice(&chunk[..received]);
    }
}

/// Writes the Redis-shaped reply for a rejected `SET` option list.
fn write_set_error(stream: &mut dyn Write, error: BindError) -> io::Result<()> {
    match error {
        BindError::WrongArity => stream.write_all(b"-ERR wrong number of arguments for 'set' command\r\n"),
        BindError::Syntax => stream.write_all(b"-ERR syntax error\r\n"),
        BindError::InvalidInteger => stream.write_all(b"-ERR value is not an integer or out of range\r\n"),
        BindError::InvalidExpire => stream.write_all(b"-ERR invalid expire time in 'set' command\r\n"),
        BindError::UnsupportedOptions => stream.write_all(b"-ERR unsupported SET options\r\n"),
        BindError::EmptyCommand => stream.write_all(b"-ERR empty command\r\n"),
        BindError::UnsupportedCommand => stream.write_all(b"-ERR unsupported command\r\n"),
    }
}

/// Executes `SET key value [NX|XX] [EX <s>|PX <ms>] [KEEPTTL]`.
///
/// The write goes through [`KeyValueExecutor`] with one expiry-aware action per Redis
/// option matrix, so the value and its deadline are applied in a single executor step:
/// an explicit `EX` / `PX` stores the deadline, `KEEPTTL` preserves the deadline of the
/// replaced record and a plain overwrite clears it. A failed `NX` / `XX` neither writes
/// the value nor touches any deadline.
fn handle_set(
    stream: &mut dyn Write,
    request: &Request<'_>,
    executor: Option<&dyn KeyValueExecutor>,
    namespace: &Namespace,
    protocol: u8,
) -> io::Result<()> {
    let Some(executor) = executor
    else {
        stream.write_all(b"-ERR unsupported command\r\n")?;
        return Ok(());
    };
    let parsed = match bind_set(&request.arguments) {
        Ok(parsed) => parsed,
        Err(error) => return write_set_error(stream, error),
    };
    // SET 会无条件替换键，因此条件判断必须看到集合类型：`NX` 在 hash 上要回 nil，
    // `XX` 在 hash 上要照常替换。字符串命名空间为空时 `PutIfPresent` 会误报未命中，
    // 所以非字符串上的 XX 改走无条件写入。
    let existing = match resolve_kind(stream, executor, namespace, &parsed.key)? {
        Resolved::Rejected => return Ok(()),
        Resolved::Missing => None,
        Resolved::Kind(kind) => Some(kind),
    };
    let condition = match parsed.condition {
        SetCondition::IfAbsent if existing.is_some() => return write_null(stream, protocol),
        SetCondition::IfPresent if existing.is_none() => return write_null(stream, protocol),
        SetCondition::IfPresent if !matches!(existing, Some(KeyKind::String)) => SetCondition::Always,
        condition => condition,
    };
    // `KEEPTTL` 要继承原键的过期时间；字符串由执行器自己保留，集合键需要先读出来再补写。
    let inherited_expiry = match existing {
        Some(kind) if parsed.keep_ttl && kind != KeyKind::String => {
            let command = database_query::cluster_kv::KeyValueCommand {
                namespace: kind.namespace(namespace),
                key: parsed.key.clone(),
                action: database_query::cluster_kv::KeyValueAction::GetExpiry,
            };
            match executor.execute(None, command) {
                Ok(KeyValueResult::Expiry { present: true, expires_at }) => expires_at,
                _ => None,
            }
        }
        _ => None,
    };
    let action = match (condition, parsed.expiry) {
        (SetCondition::Always, Some(expiry)) => match set_deadline(expiry) {
            Some(expires_at) => database_query::cluster_kv::KeyValueAction::PutExpiring { value: parsed.value, expires_at },
            None => return write_set_error(stream, BindError::InvalidExpire),
        },
        // NX on an absent key has no existing deadline to keep, so plain PutIfAbsent is exact.
        (SetCondition::IfAbsent, Some(expiry)) => match set_deadline(expiry) {
            Some(expires_at) => database_query::cluster_kv::KeyValueAction::PutIfAbsentExpiring { value: parsed.value, expires_at },
            None => return write_set_error(stream, BindError::InvalidExpire),
        },
        (SetCondition::IfAbsent, None) => database_query::cluster_kv::KeyValueAction::PutIfAbsent(parsed.value),
        (SetCondition::IfPresent, Some(expiry)) => match set_deadline(expiry) {
            Some(expires_at) => database_query::cluster_kv::KeyValueAction::PutIfPresentExpiring { value: parsed.value, expires_at },
            None => return write_set_error(stream, BindError::InvalidExpire),
        },
        (SetCondition::IfPresent, None) if parsed.keep_ttl => {
            database_query::cluster_kv::KeyValueAction::PutIfPresentKeepingExpiry(parsed.value)
        }
        (SetCondition::IfPresent, None) => database_query::cluster_kv::KeyValueAction::PutIfPresent(parsed.value),
        (SetCondition::Always, None) if parsed.keep_ttl => database_query::cluster_kv::KeyValueAction::PutKeepingExpiry(parsed.value),
        (SetCondition::Always, None) => database_query::cluster_kv::KeyValueAction::Put(parsed.value),
    };
    let result =
        executor.execute(None, database_query::cluster_kv::KeyValueCommand { namespace: namespace.clone(), key: parsed.key.clone(), action });
    let stored = match result {
        Ok(KeyValueResult::Put { .. }) => true,
        Ok(KeyValueResult::PutIfAbsent { revision }) | Ok(KeyValueResult::PutIfPresent { revision }) => revision.is_some(),
        Ok(_) => {
            write_error(stream, "SET executor returned unexpected result")?;
            return Ok(());
        }
        Err(error) => {
            write_error(stream, &error.to_string())?;
            return Ok(());
        }
    };
    if !stored {
        // The condition failed: no write happened, so any existing deadline survives.
        write_null(stream, protocol)?;
        return Ok(());
    }
    if let Some(expires_at) = inherited_expiry {
        let action = database_query::cluster_kv::KeyValueAction::SetExpiry { expires_at };
        let command = database_query::cluster_kv::KeyValueCommand { namespace: namespace.clone(), key: parsed.key.clone(), action };
        if let Err(error) = executor.execute(None, command) {
            write_error(stream, &error.to_string())?;
            return Ok(());
        }
    }
    // 覆盖写成功后清掉旧的集合表示，保证一个逻辑键只有一种类型。
    if let Err(message) = clear_other_kinds(executor, namespace, &parsed.key) {
        write_error(stream, &message)?;
        return Ok(());
    }
    stream.write_all(b"+OK\r\n")
}

/// Absolute Unix-millisecond deadline for a SET expiry option, unless it overflows.
///
/// `EX` / `PX` are offsets from the server's current time; `EXAT` / `PXAT`
/// name the deadline directly (records carry wall-clock deadlines).
fn set_deadline(expiry: SetExpiry) -> Option<i64> {
    match expiry {
        SetExpiry::Seconds(seconds) => now_unix_ms().checked_add(i64::try_from(seconds.checked_mul(1000)?).ok()?),
        SetExpiry::Milliseconds(milliseconds) => now_unix_ms().checked_add(i64::try_from(milliseconds).ok()?),
        SetExpiry::AtSeconds(seconds) => i64::try_from(seconds).ok()?.checked_mul(1000),
        SetExpiry::AtMilliseconds(milliseconds) => i64::try_from(milliseconds).ok(),
    }
}

/// `CLIENT SETNAME` / `CLIENT GETNAME` / `CLIENT SETINFO`.
///
/// Only the subcommands connection pools send while opening a connection are
/// implemented. `SETINFO` is Redis 7.2 library metadata: the server keeps it for
/// `CLIENT LIST` output, which this gateway does not render, so it is accepted as
/// a non-persisted no-op. Everything else (`CLIENT ID`, `CLIENT KILL`,
/// `CLIENT NO-EVICT`, ...) stays explicitly unsupported.
fn handle_client(stream: &mut dyn Write, request: &Request<'_>, state: &mut ConnectionState) -> io::Result<()> {
    let Some(subcommand) = request.arguments.get(1)
    else {
        write_error(stream, "Unknown subcommand or wrong number of arguments for 'CLIENT'. Try CLIENT HELP.")?;
        return Ok(());
    };
    let count = request.arguments.len();
    if subcommand.eq_ignore_ascii_case(b"SETINFO") {
        if count != 4 {
            write_error(stream, "wrong number of arguments for 'client|setinfo' command")?;
            return Ok(());
        }
        let attribute = request.arguments[2];
        if attribute.eq_ignore_ascii_case(b"LIB-NAME") || attribute.eq_ignore_ascii_case(b"LIB-VER") {
            // Deliberately not stored: no `CLIENT LIST` projection exists yet.
            stream.write_all(b"+OK\r\n")?;
        }
        else {
            write_error(stream, &format!("Unrecognized option '{}'", String::from_utf8_lossy(attribute)))?;
        }
        return Ok(());
    }
    if subcommand.eq_ignore_ascii_case(b"SETNAME") {
        if count != 3 {
            write_error(stream, "wrong number of arguments for 'client|setname' command")?;
            return Ok(());
        }
        state.client_name = Some(request.arguments[2].to_vec());
        stream.write_all(b"+OK\r\n")?;
        return Ok(());
    }
    if subcommand.eq_ignore_ascii_case(b"GETNAME") {
        if count != 2 {
            write_error(stream, "wrong number of arguments for 'client|getname' command")?;
            return Ok(());
        }
        // Redis answers an empty bulk string, not a null bulk, when no name was set.
        return match &state.client_name {
            Some(name) => write_bulk(stream, name),
            None => stream.write_all(b"$0\r\n\r\n"),
        };
    }
    write_error(
        stream,
        &format!("Unknown subcommand or wrong number of arguments for '{}'. Try CLIENT HELP.", String::from_utf8_lossy(subcommand)),
    )
}

/// `WATCH key [key ...]` — pins each key's current value as the version `EXEC` validates.
///
/// A repeated `WATCH` of an already-pinned key keeps the original snapshot, and a missing
/// key is pinned as absent, so a later insert also aborts the transaction. Watching inside
/// `MULTI` is rejected without touching the existing watch list, like upstream Redis.
fn handle_watch(
    stream: &mut dyn Write,
    request: &Request<'_>,
    executor: Option<&dyn KeyValueExecutor>,
    namespace: &Namespace,
    state: &mut ConnectionState,
) -> io::Result<()> {
    let count = request.arguments.len();
    if count < 2 {
        stream.write_all(b"-ERR wrong number of arguments for 'watch' command\r\n")?;
        return Ok(());
    }
    if state.in_multi {
        stream.write_all(b"-ERR WATCH inside MULTI is not allowed\r\n")?;
        return Ok(());
    }
    let Some(executor) = executor
    else {
        stream.write_all(b"-ERR unsupported command\r\n")?;
        return Ok(());
    };
    for key in &request.arguments[1..] {
        if state.watched.iter().any(|entry| entry.namespace == *namespace && entry.key.as_slice() == *key) {
            continue;
        }
        let snapshot = match key_snapshot(executor, namespace, key) {
            Ok(snapshot) => snapshot,
            Err(message) => {
                write_error(stream, &message)?;
                return Ok(());
            }
        };
        state.watched.push(WatchedKey { namespace: namespace.clone(), key: key.to_vec(), snapshot });
    }
    stream.write_all(b"+OK\r\n")
}

/// `UNWATCH` — forgets every pinned key without touching the transaction queue.
fn handle_unwatch(stream: &mut dyn Write, count: usize, state: &mut ConnectionState) -> io::Result<()> {
    if count != 1 {
        stream.write_all(b"-ERR wrong number of arguments for 'unwatch' command\r\n")?;
        return Ok(());
    }
    state.watched.clear();
    stream.write_all(b"+OK\r\n")
}

/// 跨类型探测一个逻辑键的结果。
///
/// `Rejected` 表示错误回复已经写入连接，调用方不得再追加字节。
enum Resolved {
    Missing,
    Kind(KeyKind),
    Rejected,
}

/// 探测逻辑键当前类型；探测失败时写成 RESP 错误并返回 `Rejected`。
fn resolve_kind(
    stream: &mut dyn Write,
    executor: &dyn KeyValueExecutor,
    namespace: &Namespace,
    key: &[u8],
) -> io::Result<Resolved> {
    match collections::find_kind(executor, namespace, key) {
        Ok(Some(kind)) => Ok(Resolved::Kind(kind)),
        Ok(None) => Ok(Resolved::Missing),
        Err(message) => {
            write_error(stream, &message)?;
            Ok(Resolved::Rejected)
        }
    }
}

/// 删除同一逻辑键的其它类型表示。
///
/// `SET` / `SETEX` 会无条件替换键，而 Redis 的键一次只承载一种类型；写完字符串后必须
/// 清掉 hash/list 表示，否则 `TYPE`、`EXISTS` 与后续写命令会看到两个并存的值。
fn clear_other_kinds(executor: &dyn KeyValueExecutor, namespace: &Namespace, key: &[u8]) -> Result<(), String> {
    for kind in KeyKind::ALL {
        if kind == KeyKind::String {
            continue;
        }
        let command = KeyValueCommand { namespace: kind.namespace(namespace), key: key.to_vec(), action: KeyValueAction::Delete };
        match executor.execute(None, command) {
            Ok(KeyValueResult::Delete { .. }) => {}
            Ok(_) => return Err("delete returned an unexpected result".into()),
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
}

/// `OBJECT IDLETIME` / `OBJECT REFCOUNT` / `OBJECT FREQ`.
///
/// 三个子命令先按真机的 `LOOKUP_NOTOUCH` 口径查键：缺键回 null（RESP2 `$-1`、RESP3 `_`），
/// 查询本身不刷新访问计时。这里用未包装的执行器，因此 `OBJECT IDLETIME` 不会把空闲
/// 时间清零。
fn handle_object(
    stream: &mut dyn Write,
    request: &Request<'_>,
    probe: Option<&dyn KeyValueExecutor>,
    tracker: &KeyAccessTracker,
    namespace: &Namespace,
    protocol: u8,
) -> io::Result<()> {
    let count = request.arguments.len();
    let subcommand = request.arguments.get(1).copied().unwrap_or(b"");
    if count < 2 {
        stream.write_all(b"-ERR wrong number of arguments for 'object' command\r\n")?;
        return Ok(());
    }
    let known = subcommand.eq_ignore_ascii_case(b"IDLETIME")
        || subcommand.eq_ignore_ascii_case(b"REFCOUNT")
        || subcommand.eq_ignore_ascii_case(b"FREQ");
    if !known || count != 3 {
        write_error(
            stream,
            &format!(
                "Unknown subcommand or wrong number of arguments for '{}'. Try OBJECT HELP.",
                String::from_utf8_lossy(subcommand)
            ),
        )?;
        return Ok(());
    }
    let Some(executor) = probe
    else {
        stream.write_all(b"-ERR unsupported command\r\n")?;
        return Ok(());
    };
    let key = request.arguments[2];
    // 集合键也参与 `OBJECT`：先解析逻辑键落在哪个类型命名空间，再按该命名空间计时。
    let target = match resolve_kind(stream, executor, namespace, key)? {
        Resolved::Missing => return write_null(stream, protocol),
        Resolved::Kind(kind) => kind.namespace(namespace),
        Resolved::Rejected => return Ok(()),
    };
    if subcommand.eq_ignore_ascii_case(b"IDLETIME") {
        // 纯内存计时：不写 `.yykv`、不升格式版本，重启后从零开始，与真机 RDB 的行为一致。
        write!(stream, ":{}\r\n", tracker.idle_seconds(&target, key))
    }
    else if subcommand.eq_ignore_ascii_case(b"REFCOUNT") {
        // 这是由存储结构推出的结果，不是写死的常量：每条记录在 `Record` 里独占自己的
        // 值字节，既没有共享对象表也没有引用计数表，所以每个存在的键恰好只有一个引用。
        // 引入对象共享会改变表示与落盘口径，故不实现。
        stream.write_all(b":1\r\n")
    }
    else {
        // 网关没有淘汰策略配置面，恒为 LRU 口径：不做频率追踪，也不编造一个假计数。
        write_error(
            stream,
            "An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.",
        )
    }
}

fn respond(
    stream: &mut dyn Write,
    request: &Request<'_>,
    executor: Option<&dyn KeyValueExecutor>,
    probe: Option<&dyn KeyValueExecutor>,
    tracker: &KeyAccessTracker,
    namespace: &mut Namespace,
    state: &mut ConnectionState,
) -> io::Result<bool> {
    let command = request.arguments[0];
    let count = request.arguments.len();

    // ---- Transaction control: MULTI / EXEC / DISCARD. These are always handled directly
    // and never queued (Redis would reject a second MULTI inside a MULTI block anyway).

    if command.eq_ignore_ascii_case(b"MULTI") {
        if count != 1 {
            stream.write_all(b"-ERR wrong number of arguments for 'multi' command\r\n")?;
            return Ok(true);
        }
        if state.in_multi {
            stream.write_all(b"-ERR MULTI calls can not be nested\r\n")?;
            return Ok(true);
        }
        state.in_multi = true;
        state.queue.clear();
        stream.write_all(b"+OK\r\n")?;
        return Ok(true);
    }

    if command.eq_ignore_ascii_case(b"EXEC") {
        if count != 1 {
            stream.write_all(b"-ERR wrong number of arguments for 'exec' command\r\n")?;
            return Ok(true);
        }
        if !state.in_multi {
            stream.write_all(b"-ERR EXEC without MULTI\r\n")?;
            return Ok(true);
        }
        // Optimistic locking: any write to a watched key since WATCH abandons the whole
        // transaction before a single queued command runs. The reply is the nil array.
        if !state.watched.is_empty() {
            // 版本比对只读取值，不刷新访问计时，否则 `EXEC` 前的探测会把空闲时间清零。
            let modified = match probe {
                Some(probe) => watched_keys_modified(probe, &state.watched),
                None => Ok(true),
            };
            match modified {
                Ok(false) => {}
                Ok(true) => {
                    write_null_array(stream, state.protocol)?;
                    state.in_multi = false;
                    state.queue.clear();
                    state.watched.clear();
                    return Ok(true);
                }
                Err(message) => {
                    write_error(stream, &message)?;
                    state.in_multi = false;
                    state.queue.clear();
                    state.watched.clear();
                    return Ok(true);
                }
            }
        }
        execute_transaction(stream, executor, probe, tracker, namespace, state)?;
        return Ok(true);
    }

    if command.eq_ignore_ascii_case(b"DISCARD") {
        if count != 1 {
            stream.write_all(b"-ERR wrong number of arguments for 'discard' command\r\n")?;
            return Ok(true);
        }
        if !state.in_multi {
            stream.write_all(b"-ERR DISCARD without MULTI\r\n")?;
            return Ok(true);
        }
        state.in_multi = false;
        state.queue.clear();
        state.watched.clear();
        stream.write_all(b"+OK\r\n")?;
        return Ok(true);
    }

    // ---- Optimistic locking control: WATCH / UNWATCH. `WATCH` must run before `MULTI`,
    // while `UNWATCH` inside a `MULTI` is queued like any other command, as upstream does.
    if command.eq_ignore_ascii_case(b"WATCH") {
        handle_watch(stream, request, probe, namespace, state)?;
        return Ok(true);
    }

    if command.eq_ignore_ascii_case(b"UNWATCH") {
        if state.in_multi {
            if count != 1 {
                stream.write_all(b"-ERR wrong number of arguments for 'unwatch' command\r\n")?;
                return Ok(true);
            }
            let owned = OwnedRequest { arguments: request.arguments.iter().map(|arg| arg.to_vec()).collect() };
            state.queue.push(QueuedCommand { request: owned });
            stream.write_all(b"+QUEUED\r\n")?;
            return Ok(true);
        }
        handle_unwatch(stream, count, state)?;
        return Ok(true);
    }

    // ---- Inside a MULTI block: buffer every other command and return QUEUED.
    // Syntax / arity validation happens here so clients learn about bad commands at queue
    // time instead of at EXEC time. The actual runtime behaviour (wrong-type, NX failure,
    // etc.) is deferred to EXEC per Redis semantics.
    if state.in_multi {
        if let Err(error_bytes) = validate_command_for_queue(command, count, &request.arguments, executor, namespace) {
            stream.write_all(&error_bytes)?;
            return Ok(true);
        }
        let owned = OwnedRequest { arguments: request.arguments.iter().map(|arg| arg.to_vec()).collect() };
        state.queue.push(QueuedCommand { request: owned });
        stream.write_all(b"+QUEUED\r\n")?;
        return Ok(true);
    }

    // ---- Below is the normal, command-by-command execution path.

    if command.eq_ignore_ascii_case(b"HELLO") {
        // Modern clients (redis-py 8, ioredis, redis-cli) send `HELLO 3` on connect. The
        // answered version becomes the connection's protocol: subsequent null replies use
        // `_\r\n` under RESP3 and the null bulk string under RESP2. `HELLO` without an
        // argument reports the version already in force, like upstream Redis.
        let protocol = match request.arguments.get(1) {
            None => state.protocol,
            Some(raw) => match parse_integer(raw) {
                Some(2) => 2,
                Some(3) => 3,
                Some(_) => {
                    stream.write_all(b"-NOPROTO unsupported protocol version\r\n")?;
                    return Ok(true);
                }
                None => {
                    stream.write_all(b"-ERR Protocol version is not an integer or out of range\r\n")?;
                    return Ok(true);
                }
            },
        };
        let mut index = 2;
        while index < count {
            let option = request.arguments[index];
            if option.eq_ignore_ascii_case(b"AUTH") && index + 2 < count {
                // The local gateway has no credential model, so HELLO AUTH is accepted
                // for the same reason `yyds` accepts any MySQL user.
                index += 3;
            }
            else if option.eq_ignore_ascii_case(b"SETNAME") && index + 1 < count {
                index += 2;
            }
            else {
                stream.write_all(b"-ERR Syntax error in HELLO option\r\n")?;
                return Ok(true);
            }
        }
        write_hello(stream, protocol)?;
        state.protocol = protocol;
    }
    else if command.eq_ignore_ascii_case(b"PING") {
        match count {
            1 => stream.write_all(b"+PONG\r\n")?,
            2 => write_bulk(stream, request.arguments[1])?,
            _ => stream.write_all(b"-ERR wrong number of arguments for 'ping' command\r\n")?,
        }
    }
    else if command.eq_ignore_ascii_case(b"ECHO") {
        if count == 2 {
            write_bulk(stream, request.arguments[1])?
        }
        else {
            stream.write_all(b"-ERR wrong number of arguments for 'echo' command\r\n")?
        }
    }
    else if command.eq_ignore_ascii_case(b"QUIT") {
        if count == 1 {
            stream.write_all(b"+OK\r\n")?;
            return Ok(false);
        }
        stream.write_all(b"-ERR wrong number of arguments for 'quit' command\r\n")?;
    }
    else if command.eq_ignore_ascii_case(b"CLIENT") {
        handle_client(stream, request, state)?;
    }
    else if command.eq_ignore_ascii_case(b"SELECT") && executor.is_some() {
        if count != 2 {
            stream.write_all(b"-ERR wrong number of arguments for 'select' command\r\n")?;
        }
        else if let Some(database) = parse_database(request.arguments[1]) {
            *namespace = Namespace(format!("redis:{database}"));
            stream.write_all(b"+OK\r\n")?;
        }
        else {
            stream.write_all(b"-ERR DB index is out of range\r\n")?;
        }
    }
    else if command.eq_ignore_ascii_case(b"EXPIRE") || command.eq_ignore_ascii_case(b"PEXPIRE") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count != 3 {
            stream.write_all(b"-ERR wrong number of arguments for 'expire' command\r\n")?;
            return Ok(true);
        }
        let key = request.arguments[1];
        let delta = match parse_integer(request.arguments[2]) {
            Some(v) => v,
            None => {
                stream.write_all(b"-ERR value is not an integer or out of range\r\n")?;
                return Ok(true);
            }
        };
        // Non-positive TTLs delete the key, matching Redis.
        let target = match resolve_kind(stream, executor, namespace, key)? {
            Resolved::Rejected => return Ok(true),
            Resolved::Missing => {
                stream.write_all(b":0\r\n")?;
                return Ok(true);
            }
            Resolved::Kind(kind) => kind.namespace(namespace),
        };
        if delta <= 0 {
            let removed = match executor.execute(
                None,
                database_query::cluster_kv::KeyValueCommand {
                    namespace: target.clone(),
                    key: key.to_vec(),
                    action: database_query::cluster_kv::KeyValueAction::Delete,
                },
            ) {
                Ok(KeyValueResult::Delete { removed }) => removed,
                Ok(_) => {
                    write_error(stream, "EXPIRE executor returned unexpected result")?;
                    return Ok(true);
                }
                Err(error) => {
                    write_error(stream, &error.to_string())?;
                    return Ok(true);
                }
            };
            stream.write_all(if removed { b":1\r\n" } else { b":0\r\n" })?;
            return Ok(true);
        }
        let unit_ms: i64 = if command.eq_ignore_ascii_case(b"PEXPIRE") { 1 } else { 1000 };
        let expires_at = now_unix_ms() + delta.saturating_mul(unit_ms);
        let applied = match executor.execute(
            None,
            database_query::cluster_kv::KeyValueCommand {
                namespace: target.clone(),
                key: key.to_vec(),
                action: database_query::cluster_kv::KeyValueAction::SetExpiry { expires_at },
            },
        ) {
            Ok(KeyValueResult::ExpiryChanged { applied }) => applied,
            Ok(_) => {
                write_error(stream, "EXPIRE executor returned unexpected result")?;
                return Ok(true);
            }
            Err(error) => {
                write_error(stream, &error.to_string())?;
                return Ok(true);
            }
        };
        stream.write_all(if applied { b":1\r\n" } else { b":0\r\n" })?;
    }
    else if command.eq_ignore_ascii_case(b"EXPIREAT") || command.eq_ignore_ascii_case(b"PEXPIREAT") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count != 3 {
            stream.write_all(b"-ERR wrong number of arguments for 'expireat' command\r\n")?;
            return Ok(true);
        }
        let key = request.arguments[1];
        let at = match parse_integer(request.arguments[2]) {
            Some(v) => v,
            None => {
                stream.write_all(b"-ERR value is not an integer or out of range\r\n")?;
                return Ok(true);
            }
        };
        // Non-positive absolute deadlines delete the key, matching Redis.
        let target = match resolve_kind(stream, executor, namespace, key)? {
            Resolved::Rejected => return Ok(true),
            Resolved::Missing => {
                stream.write_all(b":0\r\n")?;
                return Ok(true);
            }
            Resolved::Kind(kind) => kind.namespace(namespace),
        };
        if at <= 0 {
            let removed = match executor.execute(
                None,
                database_query::cluster_kv::KeyValueCommand {
                    namespace: target.clone(),
                    key: key.to_vec(),
                    action: database_query::cluster_kv::KeyValueAction::Delete,
                },
            ) {
                Ok(KeyValueResult::Delete { removed }) => removed,
                Ok(_) => {
                    write_error(stream, "EXPIREAT executor returned unexpected result")?;
                    return Ok(true);
                }
                Err(error) => {
                    write_error(stream, &error.to_string())?;
                    return Ok(true);
                }
            };
            stream.write_all(if removed { b":1\r\n" } else { b":0\r\n" })?;
            return Ok(true);
        }
        let unit_ms: i64 = if command.eq_ignore_ascii_case(b"PEXPIREAT") { 1 } else { 1000 };
        let Some(expires_at) = at.checked_mul(unit_ms)
        else {
            stream.write_all(b"-ERR invalid expire time in 'expireat' command\r\n")?;
            return Ok(true);
        };
        let applied = match executor.execute(
            None,
            database_query::cluster_kv::KeyValueCommand {
                namespace: target.clone(),
                key: key.to_vec(),
                action: database_query::cluster_kv::KeyValueAction::SetExpiry { expires_at },
            },
        ) {
            Ok(KeyValueResult::ExpiryChanged { applied }) => applied,
            Ok(_) => {
                write_error(stream, "EXPIREAT executor returned unexpected result")?;
                return Ok(true);
            }
            Err(error) => {
                write_error(stream, &error.to_string())?;
                return Ok(true);
            }
        };
        stream.write_all(if applied { b":1\r\n" } else { b":0\r\n" })?;
    }
    else if command.eq_ignore_ascii_case(b"MGET") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        match bind_mget(&request.arguments, namespace.0.as_str()) {
            Ok(commands) => {
                let commands: Vec<KeyValueCommand> = commands.into_iter().map(to_kv_command).collect();
                // Expired records are filtered by the KV layer itself, so the batch read
                // already reports them as absent.
                match executor.execute_read_batch(None, &commands) {
                    Ok(results) => {
                        if results.len() != commands.len() || results.iter().any(|result| !matches!(result, KeyValueResult::Get(_))) {
                            write_error(stream, "MGET executor returned an invalid read batch")?;
                            return Ok(true);
                        }
                        // 字符串缺失的键可能以 hash/list 形式存在，真机对这种情况整条命令回
                        // WRONGTYPE，而不是把该位置当 nil。
                        for (command, result) in commands.iter().zip(&results) {
                            if !matches!(result, KeyValueResult::Get(None)) {
                                continue;
                            }
                            match collections::find_kind(executor, namespace, &command.key) {
                                Ok(Some(KeyKind::String)) | Ok(None) => {}
                                Ok(Some(_)) => {
                                    collections::write_wrong_type(stream)?;
                                    return Ok(true);
                                }
                                Err(message) => {
                                    write_error(stream, &message)?;
                                    return Ok(true);
                                }
                            }
                        }
                        write!(stream, "*{}\r\n", results.len())?;
                        for result in results {
                            match result {
                                KeyValueResult::Get(Some(value)) => write_bulk(stream, &value)?,
                                KeyValueResult::Get(None) => write_null(stream, state.protocol)?,
                                _ => unreachable!(),
                            }
                        }
                    }
                    Err(error) => write_error(stream, &error.to_string())?,
                }
            }
            Err(BindError::WrongArity) => stream.write_all(b"-ERR wrong number of arguments\r\n")?,
            Err(BindError::EmptyCommand) => stream.write_all(b"-ERR empty command\r\n")?,
            Err(BindError::UnsupportedCommand) => stream.write_all(b"-ERR unsupported command\r\n")?,
            _ => stream.write_all(b"-ERR invalid MGET command\r\n")?,
        }
    }
    else if command.eq_ignore_ascii_case(b"EXISTS") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count < 2 {
            stream.write_all(b"-ERR wrong number of arguments for 'exists' command\r\n")?;
            return Ok(true);
        }
        let mut present: i64 = 0;
        for key in &request.arguments[1..] {
            match resolve_kind(stream, executor, namespace, key)? {
                Resolved::Kind(_) => present += 1,
                Resolved::Missing => {}
                Resolved::Rejected => return Ok(true),
            }
        }
        write!(stream, ":{present}\r\n")?;
    }
    else if command.eq_ignore_ascii_case(b"TTL") || command.eq_ignore_ascii_case(b"PTTL") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count != 2 {
            stream.write_all(b"-ERR wrong number of arguments for 'ttl' command\r\n")?;
            return Ok(true);
        }
        let key = request.arguments[1];
        let target = match resolve_kind(stream, executor, namespace, key)? {
            Resolved::Rejected => return Ok(true),
            Resolved::Missing => {
                stream.write_all(b":-2\r\n")?;
                return Ok(true);
            }
            Resolved::Kind(kind) => kind.namespace(namespace),
        };
        // `GetExpiry` reads through the KV layer, so a record whose deadline has passed
        // is already reported as absent.
        let (present, expires_at) = match executor.execute(
            None,
            database_query::cluster_kv::KeyValueCommand {
                namespace: target,
                key: key.to_vec(),
                action: database_query::cluster_kv::KeyValueAction::GetExpiry,
            },
        ) {
            Ok(KeyValueResult::Expiry { present, expires_at }) => (present, expires_at),
            Ok(_) => {
                write_error(stream, "TTL executor returned unexpected result")?;
                return Ok(true);
            }
            Err(error) => {
                write_error(stream, &error.to_string())?;
                return Ok(true);
            }
        };
        let value = if !present {
            -2
        }
        else {
            match expires_at {
                None => -1,
                Some(deadline) => {
                    let remaining_ms = deadline - now_unix_ms();
                    if remaining_ms <= 0 {
                        -2
                    }
                    else if command.eq_ignore_ascii_case(b"PTTL") {
                        remaining_ms
                    }
                    else {
                        // Ceiling division without the unstable `div_ceil`.
                        (remaining_ms + 999) / 1000
                    }
                }
            }
        };
        write!(stream, ":{value}\r\n")?;
    }
    else if command.eq_ignore_ascii_case(b"TYPE") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count != 2 {
            stream.write_all(b"-ERR wrong number of arguments for 'type' command\r\n")?;
            return Ok(true);
        }
        let key = request.arguments[1];
        match resolve_kind(stream, executor, namespace, key)? {
            Resolved::Kind(kind) => stream.write_all(format!("+{}\r\n", kind.name()).as_bytes())?,
            Resolved::Missing => stream.write_all(b"+none\r\n")?,
            Resolved::Rejected => return Ok(true),
        }
    }
    else if command.eq_ignore_ascii_case(b"STRLEN") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count != 2 {
            stream.write_all(b"-ERR wrong number of arguments for 'strlen' command\r\n")?;
            return Ok(true);
        }
        let key = request.arguments[1];
        match resolve_kind(stream, executor, namespace, key)? {
            Resolved::Kind(KeyKind::String) | Resolved::Missing => {}
            Resolved::Kind(_) => {
                collections::write_wrong_type(stream)?;
                return Ok(true);
            }
            Resolved::Rejected => return Ok(true),
        }
        let value = match executor.execute(
            None,
            database_query::cluster_kv::KeyValueCommand {
                namespace: namespace.clone(),
                key: key.to_vec(),
                action: database_query::cluster_kv::KeyValueAction::Get,
            },
        ) {
            Ok(KeyValueResult::Get(Some(value))) => value.len() as i64,
            Ok(KeyValueResult::Get(None)) => 0,
            Ok(_) => {
                write_error(stream, "STRLEN executor returned unexpected result")?;
                return Ok(true);
            }
            Err(error) => {
                write_error(stream, &error.to_string())?;
                return Ok(true);
            }
        };
        write!(stream, ":{value}\r\n")?;
    }
    else if command.eq_ignore_ascii_case(b"APPEND") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count != 3 {
            stream.write_all(b"-ERR wrong number of arguments for 'append' command\r\n")?;
            return Ok(true);
        }
        let key = request.arguments[1];
        let append = request.arguments[2];
        match resolve_kind(stream, executor, namespace, key)? {
            Resolved::Kind(KeyKind::String) | Resolved::Missing => {}
            Resolved::Kind(_) => {
                collections::write_wrong_type(stream)?;
                return Ok(true);
            }
            Resolved::Rejected => return Ok(true),
        }
        let current = match executor.execute(
            None,
            database_query::cluster_kv::KeyValueCommand {
                namespace: namespace.clone(),
                key: key.to_vec(),
                action: database_query::cluster_kv::KeyValueAction::Get,
            },
        ) {
            Ok(KeyValueResult::Get(Some(v))) => v,
            Ok(KeyValueResult::Get(None)) => Vec::new(),
            Ok(_) => {
                write_error(stream, "APPEND executor returned unexpected result")?;
                return Ok(true);
            }
            Err(error) => {
                write_error(stream, &error.to_string())?;
                return Ok(true);
            }
        };
        let mut merged = current;
        merged.extend_from_slice(append);
        let new_len = merged.len() as i64;
        // APPEND keeps the TTL of the appended key. When the key is absent the record
        // simply has no expiry, so one expiry-preserving action covers both cases.
        match executor.execute(
            None,
            database_query::cluster_kv::KeyValueCommand {
                namespace: namespace.clone(),
                key: key.to_vec(),
                action: database_query::cluster_kv::KeyValueAction::PutKeepingExpiry(merged),
            },
        ) {
            Ok(KeyValueResult::Put { .. }) => write!(stream, ":{new_len}\r\n")?,
            Ok(_) => {
                write_error(stream, "APPEND executor returned unexpected result")?;
                return Ok(true);
            }
            Err(error) => {
                write_error(stream, &error.to_string())?;
                return Ok(true);
            }
        }
    }
    else if command.eq_ignore_ascii_case(b"SET") {
        handle_set(stream, request, executor, namespace, state.protocol)?;
    }
    else if command.eq_ignore_ascii_case(b"PERSIST") || command.eq_ignore_ascii_case(b"SETEX") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if command.eq_ignore_ascii_case(b"PERSIST") {
            if count != 2 {
                stream.write_all(b"-ERR wrong number of arguments for 'persist' command\r\n")?;
                return Ok(true);
            }
            let key = request.arguments[1];
            let target = match resolve_kind(stream, executor, namespace, key)? {
                Resolved::Rejected => return Ok(true),
                Resolved::Missing => {
                    stream.write_all(b":0\r\n")?;
                    return Ok(true);
                }
                Resolved::Kind(kind) => kind.namespace(namespace),
            };
            let applied = match executor.execute(
                None,
                database_query::cluster_kv::KeyValueCommand {
                    namespace: target,
                    key: key.to_vec(),
                    action: database_query::cluster_kv::KeyValueAction::ClearExpiry,
                },
            ) {
                Ok(KeyValueResult::ExpiryChanged { applied }) => applied,
                Ok(_) => {
                    write_error(stream, "PERSIST executor returned unexpected result")?;
                    return Ok(true);
                }
                Err(error) => {
                    write_error(stream, &error.to_string())?;
                    return Ok(true);
                }
            };
            stream.write_all(if applied { b":1\r\n" } else { b":0\r\n" })?;
        }
        else {
            if count != 4 {
                stream.write_all(b"-ERR wrong number of arguments for 'setex' command\r\n")?;
                return Ok(true);
            }
            let seconds = match parse_integer(request.arguments[2]) {
                Some(v) if v > 0 => v,
                _ => {
                    stream.write_all(b"-ERR invalid expire time in 'setex' command\r\n")?;
                    return Ok(true);
                }
            };
            let expires_at = now_unix_ms() + seconds.saturating_mul(1000);
            let result = executor.execute(
                None,
                database_query::cluster_kv::KeyValueCommand {
                    namespace: namespace.clone(),
                    key: request.arguments[1].to_vec(),
                    action: database_query::cluster_kv::KeyValueAction::PutExpiring { value: request.arguments[3].to_vec(), expires_at },
                },
            );
            match result {
                Ok(KeyValueResult::Put { .. }) => {
                    // SETEX 与 SET 一样无条件替换键，写完后清掉其它类型表示，保持一键一型。
                    if let Err(message) = clear_other_kinds(executor, namespace, request.arguments[1]) {
                        write_error(stream, &message)?;
                        return Ok(true);
                    }
                    stream.write_all(b"+OK\r\n")?;
                }
                Ok(_) => {
                    write_error(stream, "SETEX executor returned unexpected result")?;
                    return Ok(true);
                }
                Err(error) => {
                    write_error(stream, &error.to_string())?;
                    return Ok(true);
                }
            }
        }
    }
    else if command.eq_ignore_ascii_case(b"GETEX") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count < 2 {
            stream.write_all(b"-ERR wrong number of arguments for 'getex' command\r\n")?;
            return Ok(true);
        }
        /// One `GETEX` expiry option, kept unparsed until the key lookup decides whether
        /// its numeric argument is even relevant.
        #[derive(Clone, Copy)]
        enum ExpirySpec<'a> {
            Persist,
            Seconds(&'a [u8]),
            Milliseconds(&'a [u8]),
            SecondsAt(&'a [u8]),
            MillisecondsAt(&'a [u8]),
        }
        // Option structure (mutual exclusion and argument presence) is checked before the
        // lookup because Redis reports syntax errors even for a missing key. The numeric
        // value itself is checked after the lookup, so `GETEX missing EX not-a-number`
        // returns nil instead of an expiry error, as upstream does.
        let mut spec: Option<ExpirySpec<'_>> = None;
        let mut index = 2;
        while index < count {
            let option = request.arguments[index];
            if spec.is_some() {
                stream.write_all(b"-ERR syntax error\r\n")?;
                return Ok(true);
            }
            if option.eq_ignore_ascii_case(b"PERSIST") {
                spec = Some(ExpirySpec::Persist);
                index += 1;
            }
            else if option.eq_ignore_ascii_case(b"EX")
                || option.eq_ignore_ascii_case(b"PX")
                || option.eq_ignore_ascii_case(b"EXAT")
                || option.eq_ignore_ascii_case(b"PXAT")
            {
                let Some(raw) = request.arguments.get(index + 1)
                else {
                    stream.write_all(b"-ERR syntax error\r\n")?;
                    return Ok(true);
                };
                spec = Some(if option.eq_ignore_ascii_case(b"EX") {
                    ExpirySpec::Seconds(raw)
                }
                else if option.eq_ignore_ascii_case(b"PX") {
                    ExpirySpec::Milliseconds(raw)
                }
                else if option.eq_ignore_ascii_case(b"EXAT") {
                    ExpirySpec::SecondsAt(raw)
                }
                else {
                    ExpirySpec::MillisecondsAt(raw)
                });
                index += 2;
            }
            else {
                stream.write_all(b"-ERR syntax error\r\n")?;
                return Ok(true);
            }
        }
        let key = request.arguments[1];
        // GETEX is a string command: a hash/list key is WRONGTYPE rather than nil.
        match resolve_kind(stream, executor, namespace, key)? {
            Resolved::Kind(KeyKind::String) | Resolved::Missing => {}
            Resolved::Kind(_) => {
                collections::write_wrong_type(stream)?;
                return Ok(true);
            }
            Resolved::Rejected => return Ok(true),
        }
        let value = match executor.execute(
            None,
            database_query::cluster_kv::KeyValueCommand {
                namespace: namespace.clone(),
                key: key.to_vec(),
                action: database_query::cluster_kv::KeyValueAction::Get,
            },
        ) {
            Ok(KeyValueResult::Get(value)) => value,
            Ok(_) => {
                write_error(stream, "GETEX executor returned unexpected result")?;
                return Ok(true);
            }
            Err(error) => {
                write_error(stream, &error.to_string())?;
                return Ok(true);
            }
        };
        // An absent (or expired) key returns nil and nothing is touched, so the expiry
        // argument is deliberately left unparsed in that branch.
        let Some(value) = value
        else {
            write_null(stream, state.protocol)?;
            return Ok(true);
        };
        if let Some(spec) = spec {
            let action = match spec {
                ExpirySpec::Persist => database_query::cluster_kv::KeyValueAction::ClearExpiry,
                ExpirySpec::Seconds(raw)
                | ExpirySpec::Milliseconds(raw)
                | ExpirySpec::SecondsAt(raw)
                | ExpirySpec::MillisecondsAt(raw) => {
                    let Some(value) = parse_integer(raw)
                    else {
                        stream.write_all(b"-ERR value is not an integer or out of range\r\n")?;
                        return Ok(true);
                    };
                    if value <= 0 {
                        stream.write_all(b"-ERR invalid expire time in 'getex' command\r\n")?;
                        return Ok(true);
                    }
                    let expires_at = match spec {
                        ExpirySpec::Seconds(_) => value.checked_mul(1000).and_then(|delta_ms| now_unix_ms().checked_add(delta_ms)),
                        ExpirySpec::Milliseconds(_) => now_unix_ms().checked_add(value),
                        ExpirySpec::SecondsAt(_) => value.checked_mul(1000),
                        ExpirySpec::MillisecondsAt(_) => Some(value),
                        ExpirySpec::Persist => None,
                    };
                    let Some(expires_at) = expires_at
                    else {
                        stream.write_all(b"-ERR invalid expire time in 'getex' command\r\n")?;
                        return Ok(true);
                    };
                    database_query::cluster_kv::KeyValueAction::SetExpiry { expires_at }
                }
            };
            if let Err(error) =
                executor.execute(None, database_query::cluster_kv::KeyValueCommand { namespace: namespace.clone(), key: key.to_vec(), action })
            {
                write_error(stream, &error.to_string())?;
                return Ok(true);
            }
        }
        write_bulk(stream, &value)?;
    }
    else if collections::is_hash_command(command) {
        collections::handle_hash(stream, request, executor, namespace, state.protocol)?;
    }
    else if collections::is_list_command(command) {
        collections::handle_list(stream, request, executor, namespace, state.protocol)?;
    }
    else if collections::is_set_command(command) {
        collections::handle_set(stream, request, executor, namespace)?;
    }
    else if collections::is_zset_command(command) {
        collections::handle_zset(stream, request, executor, namespace, state.protocol)?;
    }
    else if command.eq_ignore_ascii_case(b"OBJECT") {
        handle_object(stream, request, probe, tracker, namespace, state.protocol)?;
    }
    else if command.eq_ignore_ascii_case(b"RENAME") || command.eq_ignore_ascii_case(b"RENAMENX") {
        // Session / token rotation: the record moves together with its remaining deadline in
        // one shard-local durable commit, so no read-write-delete window can lose it and the
        // TTL is never reset to "fresh" by the move.
        let rename_nx = command.eq_ignore_ascii_case(b"RENAMENX");
        let name = if rename_nx { "renamenx" } else { "rename" };
        if request.arguments.len() != 3 {
            write!(stream, "-ERR wrong number of arguments for '{name}' command\r\n")?;
            return Ok(true);
        }
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        let action = database_query::cluster_kv::KeyValueAction::Rename { destination: request.arguments[2].to_vec(), if_absent: rename_nx };
        let command = database_query::cluster_kv::KeyValueCommand { namespace: namespace.clone(), key: request.arguments[1].to_vec(), action };
        match executor.execute(None, command) {
            Ok(database_query::cluster_kv::KeyValueResult::Rename { source_present, applied }) => {
                if !source_present {
                    write_error(stream, "no such key")?;
                }
                else if rename_nx {
                    stream.write_all(if applied { b":1\r\n" } else { b":0\r\n" })?;
                }
                else {
                    stream.write_all(b"+OK\r\n")?;
                }
            }
            Ok(_) => write_error(stream, "RENAME executor returned unexpected result")?,
            Err(error) => write_error(stream, &error.to_string())?,
        }
    }
    else if command.eq_ignore_ascii_case(b"DEL") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count < 2 {
            stream.write_all(b"-ERR wrong number of arguments for 'del' command\r\n")?;
            return Ok(true);
        };
        // 一个逻辑键可能以字符串、hash 或 list 中的任意一种形式存在，删除要逐类型清理；
        // 只要其中一种表示被删掉，这次 `DEL` 就对它计 1，与真机的按键计数一致。
        let mut removed: i64 = 0;
        for key in &request.arguments[1..] {
            let mut deleted = false;
            for kind in KeyKind::ALL {
                let command = database_query::cluster_kv::KeyValueCommand {
                    namespace: kind.namespace(namespace),
                    key: key.to_vec(),
                    action: database_query::cluster_kv::KeyValueAction::Delete,
                };
                match executor.execute(None, command) {
                    Ok(KeyValueResult::Delete { removed: true }) => deleted = true,
                    Ok(KeyValueResult::Delete { removed: false }) => {}
                    Ok(_) => {
                        write_error(stream, "DEL executor returned unexpected result")?;
                        return Ok(true);
                    }
                    Err(error) => {
                        write_error(stream, &error.to_string())?;
                        return Ok(true);
                    }
                }
            }
            if deleted {
                removed += 1;
            }
        }
        write!(stream, ":{removed}\r\n")?;
    }
    else if let Some(executor) = executor {
        // --- Generic bind()-backed commands (GET, INCR, DECR, GETDEL, GETSET).
        // Expiry semantics are enforced by the KV layer: expired records read as absent
        // and each action carries its own expiry behaviour.
        match bind(&request.arguments, namespace.0.as_str()) {
            Ok(command) => {
                // 这些命令只对字符串有效：hash / list 键在真机上回 WRONGTYPE 而不是 nil。
                // 绑定成功后再探测，保证参数错误优先于类型错误。
                if matches!(
                    command.action,
                    BoundAction::Get | BoundAction::GetDelete | BoundAction::GetSet(_) | BoundAction::IncrementBy(_)
                ) {
                    match resolve_kind(stream, executor, namespace, &command.key)? {
                        Resolved::Kind(KeyKind::String) | Resolved::Missing => {}
                        Resolved::Kind(_) => {
                            collections::write_wrong_type(stream)?;
                            return Ok(true);
                        }
                        Resolved::Rejected => return Ok(true),
                    }
                }
                match executor.execute(None, to_kv_command(command)) {
                    Ok(result) => {
                        match result {
                            KeyValueResult::Get(Some(value)) => write_bulk(stream, &value)?,
                            KeyValueResult::Get(None) => write_null(stream, state.protocol)?,
                            KeyValueResult::Exists(exists) => stream.write_all(if exists { b":1\r\n" } else { b":0\r\n" })?,
                            KeyValueResult::GetDelete(Some(value)) => write_bulk(stream, &value)?,
                            KeyValueResult::GetDelete(None) => write_null(stream, state.protocol)?,
                            KeyValueResult::GetSet(Some(value)) => write_bulk(stream, &value)?,
                            KeyValueResult::GetSet(None) => write_null(stream, state.protocol)?,
                            KeyValueResult::Put { .. } => stream.write_all(b"+OK\r\n")?,
                            KeyValueResult::PutIfAbsent { revision: Some(_) } => stream.write_all(b"+OK\r\n")?,
                            KeyValueResult::PutIfAbsent { revision: None } => write_null(stream, state.protocol)?,
                            KeyValueResult::PutIfPresent { revision: Some(_) } => stream.write_all(b"+OK\r\n")?,
                            KeyValueResult::PutIfPresent { revision: None } => write_null(stream, state.protocol)?,
                            KeyValueResult::Increment { value } => write!(stream, ":{value}\r\n")?,
                            KeyValueResult::Delete { removed } => stream.write_all(if removed { b":1\r\n" } else { b":0\r\n" })?,
                            // Expiry and rename results only surface through their dedicated
                            // paths, never through the generic bind() commands.
                            KeyValueResult::Expiry { .. } | KeyValueResult::ExpiryChanged { .. } | KeyValueResult::Rename { .. } => {
                                write_error(stream, "unexpected result from bind command")?;
                                return Ok(true);
                            }
                        }
                    }
                    Err(error) => write_error(stream, &error.to_string())?,
                }
            }
            Err(BindError::WrongArity) => stream.write_all(b"-ERR wrong number of arguments\r\n")?,
            Err(BindError::Syntax) => stream.write_all(b"-ERR syntax error\r\n")?,
            Err(BindError::UnsupportedOptions) => stream.write_all(b"-ERR unsupported SET options\r\n")?,
            Err(BindError::InvalidInteger) => stream.write_all(b"-ERR value is not an integer or out of range\r\n")?,
            Err(BindError::InvalidExpire) => stream.write_all(b"-ERR invalid expire time in 'set' command\r\n")?,
            Err(BindError::EmptyCommand) => stream.write_all(b"-ERR empty command\r\n")?,
            Err(BindError::UnsupportedCommand) => stream.write_all(b"-ERR unsupported command\r\n")?,
        }
    }
    else {
        stream.write_all(b"-ERR unsupported command\r\n")?;
    }
    Ok(true)
}

/// Runs every queued command under a fresh per-command `ConnectionState` so that MULTI
/// control frames cannot nest into the transaction. Each command's RESP output is captured
/// into a buffer and then emitted wrapped in a Redis multi-bulk array header.
///
/// This is a per-command replay over [`KeyValueExecutor::execute`], not an atomic commit
/// boundary: each queued command is applied on its own, a mid-transaction failure does not
/// roll back the commands already applied, and the executor's durable `commit_batch` path is
/// deliberately not used. Upstream Redis queues and executes commands sequentially the same
/// way, so no all-or-nothing durability is promised on this surface.
fn execute_transaction(
    stream: &mut dyn Write,
    executor: Option<&dyn KeyValueExecutor>,
    probe: Option<&dyn KeyValueExecutor>,
    tracker: &KeyAccessTracker,
    namespace: &mut Namespace,
    state: &mut ConnectionState,
) -> io::Result<()> {
    let count = state.queue.len();
    let queued = std::mem::take(&mut state.queue);
    let protocol = state.protocol;
    state.in_multi = false;
    // `EXEC` always releases the watched keys, whether the queue was non-empty or not.
    state.watched.clear();

    write!(stream, "*{count}\r\n")?;

    for queued in queued {
        let owned = queued.request;
        let inner_request = owned.as_request();
        let mut buf = Vec::<u8>::new();
        let mut inner_namespace = namespace.clone();
        // Give each queued command a fresh state so nested MULTI/EXEC/DISCARD semantics
        // do not pollute the parent transaction. The negotiated protocol is inherited, so
        // replayed nulls keep the shape the client handshaked for. Key expiry lives in the
        // KV records, so queued commands see and write it through the executor like any
        // other state.
        let mut inner_state = ConnectionState { protocol, ..ConnectionState::new() };
        let _ = respond(
            &mut buf as &mut dyn Write,
            &inner_request,
            executor,
            probe,
            tracker,
            &mut inner_namespace,
            &mut inner_state,
        );
        stream.write_all(&buf)?;
    }
    Ok(())
}

/// Lightweight arity / syntax check applied to every command before it is queued. Errors
/// returned here are written back to the client immediately (not deferred to EXEC). Returns
/// `Err(bytes)` with the exact RESP error to emit when syntax is wrong.
fn validate_command_for_queue(
    command: &[u8],
    count: usize,
    arguments: &[&[u8]],
    executor: Option<&dyn KeyValueExecutor>,
    namespace: &Namespace,
) -> Result<(), Vec<u8>> {
    // PING: 1 or 2 args
    if command.eq_ignore_ascii_case(b"PING") {
        if count > 2 {
            return Err(b"-ERR wrong number of arguments for 'ping' command\r\n".to_vec());
        }
        return Ok(());
    }
    // ECHO: exactly 2
    if command.eq_ignore_ascii_case(b"ECHO") {
        if count != 2 {
            return Err(b"-ERR wrong number of arguments for 'echo' command\r\n".to_vec());
        }
        return Ok(());
    }
    // QUIT: exactly 1
    if command.eq_ignore_ascii_case(b"QUIT") {
        if count != 1 {
            return Err(b"-ERR wrong number of arguments for 'quit' command\r\n".to_vec());
        }
        return Ok(());
    }
    // SELECT: exactly 2, needs executor
    if command.eq_ignore_ascii_case(b"SELECT") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count != 2 {
            return Err(b"-ERR wrong number of arguments for 'select' command\r\n".to_vec());
        }
        if parse_database(arguments[1]).is_none() {
            return Err(b"-ERR DB index is out of range\r\n".to_vec());
        }
        return Ok(());
    }
    // EXPIRE / PEXPIRE: exactly 3, need executor, integer check
    if command.eq_ignore_ascii_case(b"EXPIRE") || command.eq_ignore_ascii_case(b"PEXPIRE") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count != 3 {
            return Err(b"-ERR wrong number of arguments for 'expire' command\r\n".to_vec());
        }
        if parse_integer(arguments[2]).is_none() {
            return Err(b"-ERR value is not an integer or out of range\r\n".to_vec());
        }
        return Ok(());
    }
    // EXPIREAT / PEXPIREAT: exactly 3, need executor, integer check
    if command.eq_ignore_ascii_case(b"EXPIREAT") || command.eq_ignore_ascii_case(b"PEXPIREAT") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count != 3 {
            return Err(b"-ERR wrong number of arguments for 'expireat' command\r\n".to_vec());
        }
        if parse_integer(arguments[2]).is_none() {
            return Err(b"-ERR value is not an integer or out of range\r\n".to_vec());
        }
        return Ok(());
    }
    // RENAME / RENAMENX: exactly 3, need executor. Source presence and destination
    // conflicts are runtime semantics and stay deferred to EXEC.
    if command.eq_ignore_ascii_case(b"RENAME") || command.eq_ignore_ascii_case(b"RENAMENX") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count != 3 {
            let name = if command.eq_ignore_ascii_case(b"RENAMENX") { "renamenx" } else { "rename" };
            return Err(format!("-ERR wrong number of arguments for '{name}' command\r\n").into_bytes());
        }
        return Ok(());
    }
    // GETEX: at least 2, needs executor, at most one well-formed expiry option
    if command.eq_ignore_ascii_case(b"GETEX") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count < 2 {
            return Err(b"-ERR wrong number of arguments for 'getex' command\r\n".to_vec());
        }
        let mut seen_option = false;
        let mut index = 2;
        while index < count {
            let option = arguments[index];
            if seen_option {
                return Err(b"-ERR syntax error\r\n".to_vec());
            }
            if option.eq_ignore_ascii_case(b"PERSIST") {
                seen_option = true;
                index += 1;
            }
            else if option.eq_ignore_ascii_case(b"EX")
                || option.eq_ignore_ascii_case(b"PX")
                || option.eq_ignore_ascii_case(b"EXAT")
                || option.eq_ignore_ascii_case(b"PXAT")
            {
                let Some(raw) = arguments.get(index + 1)
                else {
                    return Err(b"-ERR syntax error\r\n".to_vec());
                };
                if parse_integer(raw).is_none() {
                    return Err(b"-ERR value is not an integer or out of range\r\n".to_vec());
                }
                seen_option = true;
                index += 2;
            }
            else {
                return Err(b"-ERR syntax error\r\n".to_vec());
            }
        }
        return Ok(());
    }
    // OBJECT: at least a subcommand, needs executor. The subcommand name and the key
    // argument are runtime checks and stay deferred to EXEC, like upstream.
    if command.eq_ignore_ascii_case(b"OBJECT") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count < 2 {
            return Err(b"-ERR wrong number of arguments for 'object' command\r\n".to_vec());
        }
        return Ok(());
    }
    // TTL / PTTL / TYPE / STRLEN: exactly 2
    if command.eq_ignore_ascii_case(b"TTL")
        || command.eq_ignore_ascii_case(b"PTTL")
        || command.eq_ignore_ascii_case(b"TYPE")
        || command.eq_ignore_ascii_case(b"STRLEN")
    {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count != 2 {
            return Err(b"-ERR wrong number of arguments\r\n".to_vec());
        }
        return Ok(());
    }
    // MGET: at least 2
    if command.eq_ignore_ascii_case(b"MGET") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count < 2 {
            return Err(b"-ERR wrong number of arguments for 'mget' command\r\n".to_vec());
        }
        return Ok(());
    }
    // EXISTS: at least 2
    if command.eq_ignore_ascii_case(b"EXISTS") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count < 2 {
            return Err(b"-ERR wrong number of arguments for 'exists' command\r\n".to_vec());
        }
        return Ok(());
    }
    // APPEND: exactly 3
    if command.eq_ignore_ascii_case(b"APPEND") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count != 3 {
            return Err(b"-ERR wrong number of arguments for 'append' command\r\n".to_vec());
        }
        return Ok(());
    }
    // PERSIST: exactly 2
    if command.eq_ignore_ascii_case(b"PERSIST") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count != 2 {
            return Err(b"-ERR wrong number of arguments for 'persist' command\r\n".to_vec());
        }
        return Ok(());
    }
    // SETEX: exactly 4 with a positive integer TTL
    if command.eq_ignore_ascii_case(b"SETEX") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count != 4 {
            return Err(b"-ERR wrong number of arguments for 'setex' command\r\n".to_vec());
        }
        if parse_integer(arguments[2]).is_none_or(|seconds| seconds <= 0) {
            return Err(b"-ERR invalid expire time in 'setex' command\r\n".to_vec());
        }
        return Ok(());
    }
    // CLIENT: only the session-local subcommands are queueable.
    if command.eq_ignore_ascii_case(b"CLIENT") {
        let Some(subcommand) = arguments.get(1)
        else {
            return Err(b"-ERR Unknown subcommand or wrong number of arguments for 'CLIENT'. Try CLIENT HELP.\r\n".to_vec());
        };
        if subcommand.eq_ignore_ascii_case(b"SETINFO") {
            let valid = count == 4 && (arguments[2].eq_ignore_ascii_case(b"LIB-NAME") || arguments[2].eq_ignore_ascii_case(b"LIB-VER"));
            if !valid {
                return Err(b"-ERR wrong number of arguments for 'client|setinfo' command\r\n".to_vec());
            }
            return Ok(());
        }
        if subcommand.eq_ignore_ascii_case(b"SETNAME") {
            if count != 3 {
                return Err(b"-ERR wrong number of arguments for 'client|setname' command\r\n".to_vec());
            }
            return Ok(());
        }
        if subcommand.eq_ignore_ascii_case(b"GETNAME") {
            if count != 2 {
                return Err(b"-ERR wrong number of arguments for 'client|getname' command\r\n".to_vec());
            }
            return Ok(());
        }
        return Err(b"-ERR unsupported command\r\n".to_vec());
    }
    // Hash / list value commands: they need the executor. Their arity and type checks
    // are runtime semantics and stay deferred to EXEC, like upstream queueing.
    if collections::is_hash_command(command)
        || collections::is_list_command(command)
        || collections::is_set_command(command)
        || collections::is_zset_command(command)
    {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        return Ok(());
    }
    // DEL: one or more keys, mirrors the `-2` arity of the upstream command.
    if command.eq_ignore_ascii_case(b"DEL") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count < 2 {
            return Err(b"-ERR wrong number of arguments for 'del' command\r\n".to_vec());
        }
        return Ok(());
    }
    // Fall back to bind()'s own syntax checks.
    match bind(arguments, namespace.0.as_str()) {
        Ok(_) => Ok(()),
        Err(BindError::WrongArity) => Err(b"-ERR wrong number of arguments\r\n".to_vec()),
        Err(BindError::Syntax) => Err(b"-ERR syntax error\r\n".to_vec()),
        Err(BindError::UnsupportedOptions) => Err(b"-ERR unsupported SET options\r\n".to_vec()),
        Err(BindError::InvalidInteger) => Err(b"-ERR value is not an integer or out of range\r\n".to_vec()),
        Err(BindError::InvalidExpire) => Err(b"-ERR invalid expire time in 'set' command\r\n".to_vec()),
        Err(BindError::EmptyCommand) => Err(b"-ERR empty command\r\n".to_vec()),
        Err(BindError::UnsupportedCommand) => Err(b"-ERR unsupported command\r\n".to_vec()),
    }
}

fn parse_database(bytes: &[u8]) -> Option<u8> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let database = bytes.iter().try_fold(0_u8, |value, digit| value.checked_mul(10)?.checked_add(digit - b'0'))?;
    (database < 16).then_some(database)
}

fn parse_integer(bytes: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(bytes).ok()?;
    let value = text.parse::<i64>().ok()?;
    // Reject "canonical form" — Redis rejects "+1", " 1", leading-zero-but-same-string, etc.
    // For simplicity we just accept any round-tripable i64.
    if value.to_string() != text {
        return None;
    }
    Some(value)
}

pub(crate) fn write_error(stream: &mut dyn Write, message: &str) -> io::Result<()> {
    let sanitized =
        message.chars().map(|character| if matches!(character, '\r' | '\n') { ' ' } else { character }).take(512).collect::<String>();
    write!(stream, "-ERR {sanitized}\r\n")
}

pub(crate) fn write_bulk(stream: &mut dyn Write, bytes: &[u8]) -> io::Result<()> {
    write!(stream, "${}\r\n", bytes.len())?;
    stream.write_all(bytes)?;
    stream.write_all(b"\r\n")
}

/// Writes the null reply in the version the connection negotiated.
///
/// RESP2 spells a missing bulk value `$-1\r\n`. RESP3 replaced that form with the
/// dedicated null type `_\r\n`, and a RESP3 parser treats `$-1` as a protocol error,
/// so the choice has to follow the connection state instead of a fixed literal.
pub(crate) fn write_null(stream: &mut dyn Write, protocol: u8) -> io::Result<()> {
    stream.write_all(if protocol == 3 { b"_\r\n" as &[u8] } else { b"$-1\r\n" })
}

/// Writes the nil array `EXEC` replies with when a watched key changed.
///
/// RESP2 spells it `*-1\r\n` (the null array, not the null bulk that `write_null`
/// emits). RESP3 replaced it with the dedicated null type `_\r\n`.
pub(crate) fn write_null_array(stream: &mut dyn Write, protocol: u8) -> io::Result<()> {
    stream.write_all(if protocol == 3 { b"_\r\n" as &[u8] } else { b"*-1\r\n" })
}

/// Reads the current value of one key, which is the version compared by `WATCH`.
fn key_snapshot(executor: &dyn KeyValueExecutor, namespace: &Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let command = KeyValueCommand { namespace: namespace.clone(), key: key.to_vec(), action: KeyValueAction::Get };
    match executor.execute(None, command) {
        Ok(KeyValueResult::Get(value)) => Ok(value),
        Ok(_) => Err("WATCH executor returned unexpected result".into()),
        Err(error) => Err(error.to_string()),
    }
}

/// Compares every watched snapshot against the current value.
///
/// `true` means at least one watched key was written (or expired) after the watch was
/// taken, so the caller must abandon the transaction instead of executing it.
fn watched_keys_modified(executor: &dyn KeyValueExecutor, watched: &[WatchedKey]) -> Result<bool, String> {
    for entry in watched {
        if key_snapshot(executor, &entry.namespace, &entry.key)? != entry.snapshot {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Writes the `HELLO` reply in the protocol version the client asked for:
/// a RESP3 map for `HELLO 3`, or the same key/value pairs as a flat RESP2 array.
fn write_hello(stream: &mut dyn Write, protocol: u8) -> io::Result<()> {
    stream.write_all(if protocol == 3 { b"%7\r\n" as &[u8] } else { b"*14\r\n" })?;
    write_bulk(stream, b"server")?;
    write_bulk(stream, b"yyds")?;
    write_bulk(stream, b"version")?;
    write_bulk(stream, env!("CARGO_PKG_VERSION").as_bytes())?;
    write_bulk(stream, b"proto")?;
    write!(stream, ":{protocol}\r\n")?;
    write_bulk(stream, b"id")?;
    stream.write_all(b":0\r\n")?;
    write_bulk(stream, b"mode")?;
    write_bulk(stream, b"standalone")?;
    write_bulk(stream, b"role")?;
    write_bulk(stream, b"master")?;
    write_bulk(stream, b"modules")?;
    stream.write_all(b"*0\r\n")
}
