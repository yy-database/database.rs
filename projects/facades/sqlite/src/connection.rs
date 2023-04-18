//! 连接层:数据库行为的权威归属。
//!
//! [`Connection`] 拥有真实连接、文件访问、只读判定、预编译语句、参数槽位、
//! 读写分类、提交边界与备份。宿主(N-API、PyO3、WebAssembly)只能把这些权威
//! 信息映射成各自的 JS/Python 形状,不得在宿主层重复实现这些语义:
//!
//! - 语句切分与词法来自 [`crate::sql::parse`],宿主不再自己扫分号;
//! - 参数槽位来自 [`crate::sql::parameter_slots`],宿主不再自己数 `?`;
//! - 读写判定来自 [`crate::sql::Statement::writes`],宿主不再用关键词猜;
//! - 文件落盘只发生在提交边界(autocommit 写语句成功后、`COMMIT` 成功后),
//!   由存储层 [`crate::SqliteWriter::save`] 完成,宿主不再写临时 snapshot
//!   再改名冒充备份。

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions, TryLockError},
    io::{ErrorKind, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    SqliteWriter,
    sql::{Cell, Engine, ExecResult, SqlError, Statement, TransactionAction, engine::LazyCursor, parameter_slots},
};

use database_storage::sqlite::journal;
use database_udf::{HostMicroDefinition, HostRuntimeAdapter};

/// 把存储层错误映射为结构化的 SQL 拒绝。
fn storage_error(error: impl std::fmt::Display) -> SqlError {
    SqlError::new("storage", error.to_string())
}

/// 文件写连接的跨进程互斥锁。
///
/// 锁落在数据库文件旁的 `<数据库文件>.lock` 上,由操作系统的 advisory file
/// lock(`flock` / `LockFileEx`)承担:同一文件同一时刻只允许一个写连接持有,
/// 无论持有者是不是本进程。操作系统在进程退出时自动释放,因此崩溃不会留下
/// 永久占用;锁文件里的 PID 只用于超时诊断,不参与存活判定。
struct WriterLock {
    /// 持有 OS 锁的句柄;drop 即释放。
    _file: File,
}

impl WriterLock {
    /// 数据库文件对应的锁文件路径。
    fn sidecar(database: &Path) -> PathBuf {
        let mut path = database.as_os_str().to_os_string();
        path.push(".lock");
        PathBuf::from(path)
    }

    /// 获取写锁;被占用时按 `timeout` 重试,超时以 `busy` 拒绝。
    fn acquire(database: &Path, timeout: Duration) -> Result<Self, SqlError> {
        let path = Self::sidecar(database);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| SqlError::new("storage", format!("cannot open lock file `{}`: {error}", path.display())))?;
        let deadline = Instant::now() + timeout;
        let mut wait = Duration::from_millis(1);
        loop {
            match file.try_lock() {
                Ok(()) => {
                    record_owner(&file);
                    return Ok(Self { _file: file });
                }
                Err(TryLockError::WouldBlock) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(SqlError::new("busy", busy_message(&path)));
                    }
                    std::thread::sleep(wait.min(deadline - now));
                    wait = (wait * 2).min(Duration::from_millis(20));
                }
                Err(TryLockError::Error(error)) => {
                    return Err(SqlError::new("storage", format!("cannot lock database `{}`: {error}", database.display())));
                }
            }
        }
    }
}

/// 把持有者 PID 写进锁文件,供超时诊断;写失败不影响加锁结果。
fn record_owner(file: &File) {
    let mut handle = file;
    if handle.seek(SeekFrom::Start(0)).is_err() || file.set_len(0).is_err() {
        return;
    }
    let _ = handle.write_all(format!("{}\n", std::process::id()).as_bytes());
    let _ = handle.flush();
}

/// 锁被占用时的 `busy` 文案,尽力带上锁文件里记录的 PID。
fn busy_message(path: &Path) -> String {
    match fs::read_to_string(path).ok().and_then(|text| text.trim().parse::<u32>().ok()) {
        Some(pid) => format!("database is locked by process {pid} (SQLITE_BUSY)"),
        None => "database is locked (SQLITE_BUSY)".to_string(),
    }
}

/// 提交时发布新镜像并处理回滚日志的方式。
///
/// `Delete` 与 `Truncate` 在替换主文件前写出真实格式的 rollback journal:
/// 提交中断后,重新打开(本绑定或外部 SQLite 引擎)会回滚到旧镜像。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum JournalMode {
    /// 只做原子改名发布,不生成回滚日志(本绑定的默认行为)。
    #[default]
    Preserve,
    /// 发布前写 `<db>-journal`,发布成功后删除该文件。
    Delete,
    /// 发布前写 `<db>-journal`,发布成功后把它截断为 0 字节。
    Truncate,
}

impl JournalMode {
    /// 解析 `journalMode` 名称。
    ///
    /// `preserve`、`delete`、`truncate` 之外(含 `wal`)一律以 `unsupported`
    /// 拒绝,不静默降级。
    pub fn parse(name: &str) -> Result<Self, SqlError> {
        match name.trim().to_ascii_lowercase().as_str() {
            "preserve" => Ok(Self::Preserve),
            "delete" => Ok(Self::Delete),
            "truncate" => Ok(Self::Truncate),
            other => Err(SqlError::new("unsupported", format!("journalMode `{other}` is not supported (use preserve|delete|truncate)"))),
        }
    }

    /// 选项名称,与 [`JournalMode::parse`] 互逆。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preserve => "preserve",
            Self::Delete => "delete",
            Self::Truncate => "truncate",
        }
    }
}

/// 打开连接的方式。
#[derive(Clone, Debug)]
pub struct ConnectionOptions {
    /// 数据库文件路径;`None` 表示纯内存连接。
    pub filename: Option<PathBuf>,
    /// 以字节镜像打开,优先于读取 `filename`。
    pub data: Option<Vec<u8>>,
    /// 只读:任何会改变镜像的语句在执行前即被拒绝。
    pub read_only: bool,
    /// 文件不存在时是否创建;只读连接隐含不创建。
    pub create: bool,
    /// 新建数据库的页大小,`None` 用 4096。
    pub page_size: Option<u32>,
    /// `PRAGMA synchronous`;`false` 时落盘跳过持久化刷新。
    pub synchronous: bool,
    /// 提交时如何发布新镜像与处理回滚日志。
    pub journal_mode: JournalMode,
    /// 写连接在锁被另一个写连接占用时的重试时长(毫秒),`0` 表示只尝试一次。
    ///
    /// 只读连接、纯内存连接与字节镜像连接不参与加锁,该值对它们无意义。
    pub busy_timeout_ms: u64,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            filename: None,
            data: None,
            read_only: false,
            create: true,
            page_size: None,
            synchronous: true,
            journal_mode: JournalMode::Preserve,
            busy_timeout_ms: 0,
        }
    }
}

/// 一条已经解析好的语句及其权威元数据。
///
/// 解析只发生一次:`run` 反复执行同一 AST,不再重新词法。槽位、读写分类都
/// 来自 AST,与执行时绑定的槽位永远一致。
#[derive(Debug)]
pub struct Prepared {
    /// 提交的 SQL 原文。
    sql: String,
    /// 解析出的唯一语句。
    statement: Statement,
    /// 零基槽位索引与书写原文,按索引升序。
    slots: Vec<(usize, String)>,
    /// 语句是否会改变存储的数据库镜像。
    is_write: bool,
    /// prepare 时刻由引擎目录推导出的结果列标签;`None` 表示语句没有行集
    /// 结果,或目录不足以在不执行的前提下确定表头。
    columns: Option<Vec<String>>,
}

impl Prepared {
    /// 提交的 SQL 原文。
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// 语句种类名(`insert`、`select` 等),与执行结果报告同源。
    pub fn statement_name(&self) -> &'static str {
        self.statement.name()
    }

    /// 权威参数槽位:索引与书写原文。
    pub fn slots(&self) -> &[(usize, String)] {
        &self.slots
    }

    /// 绑定所需的最小参数个数。
    pub fn parameter_count(&self) -> usize {
        self.slots.last().map(|(index, _)| index + 1).unwrap_or(0)
    }

    /// 语句是否会改变存储的数据库镜像。
    pub fn is_write(&self) -> bool {
        self.is_write
    }

    /// 结果列标签,prepare 时刻由引擎目录推导,不执行语句。
    ///
    /// 行集语句给出与成功执行后 `ExecResult.columns` 逐字节一致的表头:
    /// `SELECT`(含视图、派生表、CTE、复合查询与 `*` 展开)、带 `RETURNING`
    /// 的 `INSERT`/`UPDATE`/`DELETE`、`EXPLAIN` 与读形 `PRAGMA`。
    /// `None` 如实表示两种情况:语句本就没有行集结果(无 `RETURNING` 的
    /// DML、DDL、事务控制、写形 `PRAGMA`),或目录不足以确定表头(缺表、
    /// 缺列等——这类语句执行时本就会报错,错误语义与执行期一致)。
    /// 标签在 prepare 时刻的目录上定格;此后 schema 再变化,以执行期结果
    /// 为准,本方法不重新推导。
    pub fn columns(&self) -> Option<Vec<String>> {
        self.columns.clone()
    }
}

/// 打开游标时返回的句柄与结果列标签。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorOpen {
    /// 连接内的游标句柄;后续 [`Connection::cursor_next`] 与
    /// [`Connection::cursor_close`] 都以它定位游标。
    pub handle: u64,
    /// 本次执行产生的结果列标签。
    pub columns: Vec<String>,
    /// `true` 表示游标按表 b-tree 页顺序惰性扫描,`false` 表示结果集在
    /// 打开时已经全部物化。只有简单单表 `SELECT` 会走惰性路径,`JOIN`、
    /// 聚合、`DISTINCT`、窗口、非 rowid 顺序的 `ORDER BY` 等仍是物化的。
    pub lazy: bool,
}

/// 一次 [`Connection::cursor_next`] 返回的有界行窗口。
#[derive(Clone, Debug, PartialEq)]
pub struct CursorBatch {
    /// 本窗口的行,保持结果顺序。
    pub rows: Vec<Vec<Cell>>,
    /// 游标是否已经到达结果集末尾。
    pub done: bool,
}

/// 连接内的一个结果集来源。
///
/// 简单单表扫描走 [`CursorRows::Lazy`],按 b-tree 页顺序从打开时的整页
/// 快照分批解码;其余语句在打开时一次性物化,`cursor_next` 只按窗口切片。
enum CursorRows {
    /// 完整结果集,按执行顺序。
    Materialized { rows: Vec<Vec<Cell>>, position: usize },
    /// 打开时固定的页快照上的惰性表扫描。
    Lazy(LazyCursor),
}

/// 连接内的一个已执行结果集及其读取位置。
struct Cursor {
    /// 行集来源:惰性扫描或打开时物化。
    rows: CursorRows,
    /// 打开时所属事务的代数;`None` 表示在 autocommit 下打开。
    ///
    /// 事务结束时 [`Connection::transaction_epoch`] 前进,旧代数的游标随
    /// 之以 `cursor-expired` 拒绝,与 SQLite 在事务结束后让语句失效一致。
    transaction_epoch: Option<u64>,
}

/// 一个真实连接:引擎句柄、文件归属与提交边界都在这里。
///
/// 文件写连接在打开时取得跨进程写锁并持有到 [`Connection::close`] 或句柄
/// 释放;只读连接不参与加锁,打开时把文件一次性读进内存,因此不会被写连接
/// 阻塞。写连接发布镜像时先写同目录临时文件再改名覆盖,读连接要么读到旧的
/// 完整镜像,要么读到新的完整镜像。`delete`/`truncate` 模式在改名前进写出
/// 回滚日志,提交中断后重新打开会回滚到旧镜像。
pub struct Connection {
    /// 底层执行引擎,持有 format-3 写者与目录。
    engine: Engine,
    /// 落盘目标;`None` 为纯内存。
    filename: Option<PathBuf>,
    /// 只读标志。
    read_only: bool,
    /// 镜像自上次落盘后是否发生过变化。
    dirty: bool,
    /// 文件写连接的跨进程写锁;只读、内存与字节镜像连接为 `None`。
    lock: Option<WriterLock>,
    /// `close` 之后不再接受语句。
    closed: bool,
    /// 提交时如何发布新镜像并处理回滚日志。
    journal_mode: JournalMode,
    /// 落盘时是否 `fsync`;回滚日志与恢复写回共用同一强度。
    synchronous: bool,
    /// 本连接打开的结果游标表,以句柄为键。
    cursors: BTreeMap<u64, Cursor>,
    /// 下一个游标句柄。
    next_cursor: u64,
    /// 事务代数:每次事务结束(提交或回滚)前进一格。
    transaction_epoch: u64,
    /// 仅测试使用的发布失败注入,用于在真实崩溃窗口上取证。
    #[cfg(test)]
    fail_point: Option<FailPoint>,
}

/// 测试用的发布失败点,模拟进程在对应步骤后立刻退出。
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailPoint {
    /// 回滚日志已落盘、主文件尚未发布。
    AfterJournal,
    /// 主文件已发布、日志尚未清理。
    AfterPublish,
}

impl Connection {
    /// 按选项打开连接:读已有文件、从字节镜像打开、创建新文件或纯内存。
    ///
    /// 新建的文件库在打开成功后立即落一份一致镜像,与 SQLite 创建即有文件的
    /// 行为一致;缺失文件以 `not-found` 拒绝。
    ///
    /// 文件写连接先取得跨进程写锁再读文件,因此两个写连接不可能同时以同一份
    /// 旧镜像起步;锁被占用时按 `busy_timeout_ms` 重试,超时以 `busy` 拒绝。
    /// 只读连接与字节镜像连接不加锁。
    ///
    /// 打开已有文件时先回放热 `-journal`:写连接把回滚结果写回主文件后再清理
    /// 日志,只读连接只在内存里使用回滚结果、不改动磁盘。
    pub fn open(options: ConnectionOptions) -> Result<Self, SqlError> {
        let mut created = false;
        let mut lock = None;
        let writer = if let Some(pages) = &options.data {
            SqliteWriter::from_pages(pages.clone()).map_err(storage_error)?
        }
        else if let Some(path) = &options.filename {
            if !options.read_only && (path.exists() || options.create) {
                lock = Some(WriterLock::acquire(path, Duration::from_millis(options.busy_timeout_ms))?);
            }
            if path.exists() {
                let pages = load_recovered(path, options.read_only, options.journal_mode, options.synchronous)?;
                SqliteWriter::from_pages(pages).map_err(storage_error)?
            }
            else if options.create && !options.read_only {
                created = true;
                SqliteWriter::create(options.page_size.unwrap_or(4096) as usize).map_err(storage_error)?
            }
            else {
                return Err(SqlError::new("not-found", format!("database file `{}` does not exist", path.display())));
            }
        }
        else {
            SqliteWriter::create(options.page_size.unwrap_or(4096) as usize).map_err(storage_error)?
        };
        let mut engine = Engine::from_writer(writer)?;
        engine.set_synchronous(options.synchronous);
        let journal_mode = options.journal_mode;
        let synchronous = options.synchronous;
        let mut connection = Self {
            engine,
            filename: options.filename,
            read_only: options.read_only,
            dirty: false,
            lock,
            closed: false,
            journal_mode,
            synchronous,
            cursors: BTreeMap::new(),
            next_cursor: 0,
            transaction_epoch: 0,
            #[cfg(test)]
            fail_point: None,
        };
        if created {
            // 新建库没有旧镜像可回滚;残留日志只可能来自更早的失败创建,先清掉。
            if let Some(path) = &connection.filename {
                journal::finish(path, false).map_err(storage_error)?;
            }
            connection.publish()?;
        }
        Ok(connection)
    }

    /// 当前数据库镜像,可直接写成 `.sqlite3` 文件。
    pub fn snapshot(&self) -> &[u8] {
        self.engine.snapshot()
    }

    /// 镜像头里的 format-3 库版本。
    pub fn sqlite_version(&self) -> Result<String, SqlError> {
        self.engine.sqlite_version()
    }

    /// 注册一个宿主自定义标量函数;解析与求值都经本连接的引擎,注册语义
    /// 见 [`Engine::register_host_function`]。
    pub fn register_host_function(&mut self, definition: HostMicroDefinition, adapter: Arc<dyn HostRuntimeAdapter>) -> Result<(), SqlError> {
        self.engine.register_host_function(definition, adapter)
    }

    /// 解析一次 SQL,得到可反复执行的预编译语句。
    ///
    /// 恰好一条语句:空输入与多语句分别以 `empty-statement` 和
    /// `multiple-statements` 拒绝,与 [`Engine::execute_one`] 的合同一致。
    /// 结果列标签在本时刻的目录上推导;目录解析失败(缺表、缺列)不改变
    /// 拒绝口径——`prepare` 仍然成功,标签如实为 `None`,错误留给执行期按
    /// 既有语义抛出。
    pub fn prepare(&self, sql: &str) -> Result<Prepared, SqlError> {
        let statements = self.engine.parse(sql)?;
        let statement = match statements.len() {
            1 => statements.into_iter().next().expect("length checked"),
            0 => return Err(SqlError::new("empty-statement", "expected exactly one statement but found 0")),
            count => return Err(SqlError::new("multiple-statements", format!("expected exactly one statement but found {count}"))),
        };
        let slots = parameter_slots(&statement);
        let is_write = statement.writes();
        let columns = self.engine.describe(&statement).ok().flatten();
        Ok(Prepared { sql: sql.to_string(), statement, slots, is_write, columns })
    }

    /// 执行一条预编译语句,绑定零基位置参数。
    ///
    /// 只读连接在任何会写镜像的语句执行前拒绝;参数少于槽位数时以
    /// `bind-parameter` 拒绝;成功后按提交边界决定是否落盘。
    pub fn run(&mut self, prepared: &Prepared, parameters: &[Cell]) -> Result<ExecResult, SqlError> {
        self.guard_open()?;
        self.guard_write(prepared.is_write())?;
        let required = prepared.parameter_count();
        if parameters.len() < required {
            return Err(SqlError::new("bind-parameter", format!("statement expects {required} parameters, got {}", parameters.len()))
                .in_statement(prepared.statement_name()));
        }
        let is_write = prepared.is_write();
        let was_in_transaction = self.engine.in_transaction();
        let result = self.engine.execute_statement(&prepared.statement, parameters)?;
        self.advance_epoch_if_transaction_ended(was_in_transaction);
        self.after_statement(is_write, &prepared.statement)?;
        Ok(result)
    }

    /// 执行一段分号分隔的脚本,逐条语句应用只读拒绝与提交边界。
    pub fn execute(&mut self, sql: &str, parameters: &[Cell]) -> Result<Vec<ExecResult>, SqlError> {
        self.guard_open()?;
        let statements = self.engine.parse(sql)?;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            let is_write = statement.writes();
            self.guard_write(is_write)?;
            let required = parameter_slots(&statement).last().map(|(index, _)| index + 1).unwrap_or(0);
            if parameters.len() < required {
                return Err(SqlError::new("bind-parameter", format!("statement expects {required} parameters, got {}", parameters.len()))
                    .in_statement(statement.name()));
            }
            let was_in_transaction = self.engine.in_transaction();
            let result = self.engine.execute_statement(&statement, parameters)?;
            self.advance_epoch_if_transaction_ended(was_in_transaction);
            self.after_statement(is_write, &statement)?;
            results.push(result);
        }
        Ok(results)
    }

    /// 执行一条预编译语句,把结果集放进连接内的游标表,返回句柄与表头。
    ///
    /// 简单单表扫描在打开时只固定一份整页快照,后续 [`Connection::cursor_next`]
    /// 按 b-tree 页顺序分批解码,`CursorOpen::lazy` 为 `true`。其余语句与
    /// [`Connection::run`] 共用同一条执行路径、只读拒绝、参数检查与提交
    /// 边界,结果集在打开时一次性物化,`CursorOpen::lazy` 为 `false`。
    ///
    /// 事务内打开的游标记录当前事务代数;事务提交或回滚后调用
    /// [`Connection::cursor_next`] 会以 `cursor-expired` 拒绝。
    pub fn open_cursor(&mut self, prepared: &Prepared, parameters: &[Cell]) -> Result<CursorOpen, SqlError> {
        self.guard_open()?;
        self.guard_write(prepared.is_write())?;
        let required = prepared.parameter_count();
        if parameters.len() < required {
            return Err(SqlError::new("bind-parameter", format!("statement expects {required} parameters, got {}", parameters.len()))
                .in_statement(prepared.statement_name()));
        }
        let transaction_epoch = self.engine.in_transaction().then_some(self.transaction_epoch);
        let handle = self.next_cursor;
        self.next_cursor = self.next_cursor.wrapping_add(1);
        if let Some(lazy) = self.engine.open_lazy_cursor(&prepared.statement, parameters)? {
            let columns = lazy.columns().to_vec();
            self.cursors.insert(handle, Cursor { rows: CursorRows::Lazy(lazy), transaction_epoch });
            return Ok(CursorOpen { handle, columns, lazy: true });
        }
        let was_in_transaction = self.engine.in_transaction();
        let ExecResult { columns, rows, .. } = self.engine.execute_statement(&prepared.statement, parameters)?;
        self.advance_epoch_if_transaction_ended(was_in_transaction);
        self.after_statement(prepared.is_write(), &prepared.statement)?;
        let transaction_epoch = self.engine.in_transaction().then_some(self.transaction_epoch);
        self.cursors.insert(handle, Cursor { rows: CursorRows::Materialized { rows, position: 0 }, transaction_epoch });
        Ok(CursorOpen { handle, columns, lazy: false })
    }

    /// 从游标读取至多 `limit` 行,返回本窗口与是否已到末尾。
    ///
    /// `done` 为真表示结果集已经取完,游标仍可调用 [`Connection::cursor_close`]
    /// 释放。未知句柄以 `cursor-not-found` 拒绝,事务已结束的句柄以
    /// `cursor-expired` 拒绝,已关闭的连接以 `db-closed` 拒绝。
    pub fn cursor_next(&mut self, handle: u64, limit: usize) -> Result<CursorBatch, SqlError> {
        self.guard_open()?;
        let epoch = self.transaction_epoch;
        let cursor = self.cursors.get_mut(&handle).ok_or_else(|| SqlError::new("cursor-not-found", format!("cursor {handle} is not open")))?;
        if let Some(opened) = cursor.transaction_epoch
            && opened != epoch
        {
            return Err(SqlError::new("cursor-expired", "cursor was opened inside a transaction that has already ended"));
        }
        match &mut cursor.rows {
            CursorRows::Materialized { rows, position } => {
                let remaining = rows.len() - *position;
                let end = *position + limit.min(remaining);
                let window = rows[*position..end].to_vec();
                *position = end;
                Ok(CursorBatch { rows: window, done: *position == rows.len() })
            }
            CursorRows::Lazy(lazy) => {
                let batch = self.engine.lazy_cursor_next(lazy, limit)?;
                Ok(CursorBatch { rows: batch.rows, done: batch.done })
            }
        }
    }

    /// 释放一个游标。未知句柄视为已释放,便于宿主在 `finally` 里安全清理。
    pub fn cursor_close(&mut self, handle: u64) -> Result<(), SqlError> {
        self.guard_open()?;
        self.cursors.remove(&handle);
        Ok(())
    }

    /// 当前连接仍持有的游标数量(测试与诊断用,不是作者面 API)。
    #[doc(hidden)]
    pub fn cursor_count(&self) -> usize {
        self.cursors.len()
    }

    /// 事务结束时让代数前进,使事务内打开的游标失效。
    fn advance_epoch_if_transaction_ended(&mut self, was_in_transaction: bool) {
        if was_in_transaction && !self.engine.in_transaction() {
            self.transaction_epoch = self.transaction_epoch.wrapping_add(1);
        }
    }

    /// 把当前镜像完整写入 `path`,即真正的备份。
    ///
    /// 只读连接允许备份:它读取镜像、写出到另一个文件,不改变本连接。
    pub fn backup(&mut self, path: &Path) -> Result<(), SqlError> {
        self.engine.save(path)
    }

    /// 关闭连接:已提交但未落盘的改动先落盘;事务未结束时不落盘,
    /// 与 SQLite 在关闭时回滚未提交事务的行为一致。
    ///
    /// 关闭同时释放跨进程写锁,另一个写连接随后即可打开同一文件;再次调用
    /// `close` 为空操作,关闭后的连接以 `db-closed` 拒绝语句。
    pub fn close(&mut self) -> Result<(), SqlError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let published = if self.dirty && !self.engine.in_transaction() {
            self.dirty = false;
            self.publish()
        }
        else {
            Ok(())
        };
        self.lock = None;
        self.cursors.clear();
        published
    }

    /// 是否有显式事务或保存点作用域打开。
    pub fn in_transaction(&self) -> bool {
        self.engine.in_transaction()
    }

    /// 本连接最近一次成功插入的 rowid。
    pub fn last_insert_rowid(&self) -> i64 {
        self.engine.last_insert_rowid()
    }

    /// 配置的页大小。
    pub fn page_size(&self) -> u32 {
        self.engine.page_size()
    }

    /// 连接是否为只读。
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// 落盘目标路径。
    pub fn filename(&self) -> Option<&Path> {
        self.filename.as_deref()
    }

    /// `PRAGMA synchronous` 透传。
    pub fn set_synchronous(&mut self, on: bool) {
        self.synchronous = on;
        self.engine.set_synchronous(on);
    }

    /// 本连接提交时使用的日志模式。
    pub fn journal_mode(&self) -> JournalMode {
        self.journal_mode
    }

    /// 只读连接拒绝任何会写镜像的语句,在执行前拒绝。
    fn guard_write(&self, is_write: bool) -> Result<(), SqlError> {
        if self.read_only && is_write {
            return Err(SqlError::new("read-only", "cannot modify a read-only database"));
        }
        Ok(())
    }

    /// 已关闭的连接不再接受语句。
    fn guard_open(&self) -> Result<(), SqlError> {
        if self.closed {
            return Err(SqlError::new("db-closed", "database connection is closed"));
        }
        Ok(())
    }

    /// 成功执行一条语句后应用提交边界。
    ///
    /// 写语句弄脏镜像;只要显式事务没有打开(autocommit、`COMMIT` 之后、
    /// 最外层 `RELEASE` 之后),脏镜像立即由存储层落盘。`ROLLBACK` 把镜像
    /// 恢复到上次落盘的状态,脏标志随之清除。
    fn after_statement(&mut self, is_write: bool, statement: &Statement) -> Result<(), SqlError> {
        if is_write {
            self.dirty = true;
        }
        if matches!(statement, Statement::Transaction { action: TransactionAction::Rollback }) {
            self.dirty = false;
        }
        if self.dirty && !self.engine.in_transaction() {
            self.dirty = false;
            self.publish()?;
        }
        Ok(())
    }

    /// 把当前镜像写到本连接的文件。纯内存连接为空操作。
    ///
    /// `preserve` 只走原子发布:先写同目录临时文件,再改名覆盖目标,读连接
    /// 不会观察到写了一半的镜像。`delete`/`truncate` 先把将被替换的旧镜像
    /// 写成 `<db>-journal`,再发布新镜像,最后删除或截断日志。
    fn publish(&mut self) -> Result<(), SqlError> {
        let Some(path) = self.filename.clone()
        else {
            return Ok(());
        };
        match self.journal_mode {
            JournalMode::Preserve => self.engine.save_atomic(&path),
            JournalMode::Delete | JournalMode::Truncate => self.publish_journaled(&path),
        }
    }

    /// 写日志 → 发布 → 清日志。任何一步失败都保留前一阶段已落盘的产物,
    /// 让下一次打开按日志回滚。
    fn publish_journaled(&mut self, path: &Path) -> Result<(), SqlError> {
        let journaled = path.exists();
        if journaled {
            let previous = fs::read(path).map_err(storage_error)?;
            journal::write(path, &previous, self.synchronous).map_err(storage_error)?;
        }
        #[cfg(test)]
        if self.fail_point == Some(FailPoint::AfterJournal) {
            return Err(SqlError::new("injected", "failure point after journal"));
        }
        self.engine.save_atomic(path)?;
        #[cfg(test)]
        if self.fail_point == Some(FailPoint::AfterPublish) {
            return Err(SqlError::new("injected", "failure point after publish"));
        }
        if journaled {
            journal::finish(path, self.journal_mode == JournalMode::Truncate).map_err(storage_error)?;
        }
        Ok(())
    }
}

/// 读取主文件并回放热日志。
///
/// 写连接把回滚后的镜像原子写回主文件,再按模式清理日志;只读连接只在内存
/// 里返回回滚结果,不改动磁盘上的任何文件。
fn load_recovered(path: &Path, read_only: bool, journal_mode: JournalMode, synchronous: bool) -> Result<Vec<u8>, SqlError> {
    let main = fs::read(path).map_err(storage_error)?;
    let journal_path = journal::sidecar(path);
    let journal_bytes = match fs::read(&journal_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(main),
        Err(error) => return Err(storage_error(error)),
    };
    let Some(recovered) = journal::apply(&main, &journal_bytes)
    else {
        return Ok(main);
    };
    if !read_only {
        restore_atomic(path, &recovered, synchronous)?;
        journal::finish(path, journal_mode == JournalMode::Truncate).map_err(storage_error)?;
    }
    Ok(recovered)
}

/// 把回滚结果原子写回主文件,使清理日志前的磁盘状态已是旧镜像。
fn restore_atomic(path: &Path, image: &[u8], synchronous: bool) -> Result<(), SqlError> {
    let mut temporary = path.as_os_str().to_os_string();
    temporary.push(format!(".{}.recover", std::process::id()));
    let temporary = PathBuf::from(temporary);
    let result = (|| -> std::io::Result<()> {
        if synchronous {
            let mut file = File::create(&temporary)?;
            file.write_all(image)?;
            file.sync_all()?;
        }
        else {
            fs::write(&temporary, image)?;
        }
        fs::rename(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(storage_error)
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Connection")
            .field("filename", &self.filename)
            .field("read_only", &self.read_only)
            .field("dirty", &self.dirty)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    /// 每次调用生成唯一临时目录,返回其中的数据库路径。
    fn temp_path(label: &str) -> PathBuf {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock is ahead of unix epoch").as_nanos();
        let directory = std::env::temp_dir().join(format!("sqlite-connection-{label}-{stamp}-{}", SEQUENCE.fetch_add(1, Ordering::SeqCst)));
        fs::create_dir_all(&directory).expect("temp directory");
        directory.join("db.sqlite")
    }

    /// 当前进程的常驻内存字节数,用于对照物化与惰性扫描的内存增量。
    ///
    /// 测试只在能读到真实 RSS 的平台上比较;其他平台返回 `None`,行数与
    /// 顺序断言仍然执行。
    fn resident_bytes() -> Option<u64> {
        #[cfg(windows)]
        {
            #[repr(C)]
            struct ProcessMemoryCounters {
                cb: u32,
                page_fault_count: u32,
                peak_working_set_size: usize,
                working_set_size: usize,
                quota_peak_paged_pool_usage: usize,
                quota_paged_pool_usage: usize,
                quota_peak_non_paged_pool_usage: usize,
                quota_non_paged_pool_usage: usize,
                pagefile_usage: usize,
                peak_pagefile_usage: usize,
            }
            #[link(name = "psapi")]
            unsafe extern "system" {
                fn GetCurrentProcess() -> isize;
                fn GetProcessMemoryInfo(process: isize, counters: *mut ProcessMemoryCounters, cb: u32) -> i32;
            }
            let mut counters = ProcessMemoryCounters {
                cb: std::mem::size_of::<ProcessMemoryCounters>() as u32,
                page_fault_count: 0,
                peak_working_set_size: 0,
                working_set_size: 0,
                quota_peak_paged_pool_usage: 0,
                quota_paged_pool_usage: 0,
                quota_peak_non_paged_pool_usage: 0,
                quota_non_paged_pool_usage: 0,
                pagefile_usage: 0,
                peak_pagefile_usage: 0,
            };
            // SAFETY: `counters` is a valid, correctly sized writable buffer and
            // `GetCurrentProcess` returns a pseudo handle for this process.
            let ok = unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, counters.cb) };
            return (ok != 0).then_some(counters.working_set_size as u64);
        }
        #[cfg(target_os = "linux")]
        {
            let status = fs::read_to_string("/proc/self/status").ok()?;
            for line in status.lines() {
                if let Some(rest) = line.strip_prefix("VmRSS:") {
                    let kib = rest.trim().trim_end_matches("kB").trim().parse::<u64>().ok()?;
                    return Some(kib * 1024);
                }
            }
            None
        }
        #[cfg(not(any(windows, target_os = "linux")))]
        {
            None
        }
    }

    /// 以只读连接重新打开文件,返回 `SELECT` 的行,验证磁盘上的真实内容。
    fn rows_on_disk(path: &Path, sql: &str) -> Vec<Vec<Cell>> {
        let mut reader = Connection::open(ConnectionOptions { filename: Some(path.to_path_buf()), read_only: true, ..Default::default() })
            .expect("reopen file");
        let results = reader.execute(sql, &[]).expect("read committed image");
        results.last().expect("at least one result").rows.clone()
    }

    #[test]
    fn opening_a_missing_file_creates_and_publishes_it() {
        let path = temp_path("create");
        assert!(!path.exists());
        let mut connection = Connection::open(ConnectionOptions { filename: Some(path.clone()), ..Default::default() }).expect("create opens");
        assert!(path.exists(), "a created file database carries its initial image");
        connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect("create publishes");
        assert_eq!(rows_on_disk(&path, "SELECT a FROM t").len(), 0);
        fs::remove_dir_all(path.parent().expect("parent directory")).expect("cleanup");
    }

    #[test]
    fn missing_file_without_create_is_not_found() {
        let path = temp_path("missing");
        let error = Connection::open(ConnectionOptions { filename: Some(path.join("absent.sqlite")), create: false, ..Default::default() })
            .expect_err("file is absent");
        assert_eq!(error.code, "not-found");
        fs::remove_dir_all(path.parent().expect("parent directory")).expect("cleanup");
    }

    #[test]
    fn prepare_reports_authoritative_slots() {
        let connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        let prepared = connection.prepare("INSERT INTO t VALUES (?, :name, ?)").expect("parses");
        assert_eq!(prepared.slots(), &[(0, "?".to_string()), (1, ":name".to_string()), (2, "?".to_string())]);
        assert_eq!(prepared.parameter_count(), 3);
        assert!(prepared.is_write());
        assert_eq!(prepared.statement_name(), "insert");

        // `VACUUM INTO` 的目标是普通参数槽位;只有不带 `INTO` 的 `VACUUM` 写镜像。
        let into = connection.prepare("VACUUM INTO ?").expect("vacuum parses");
        assert_eq!(into.slots(), &[(0, "?".to_string())]);
        assert_eq!(into.statement_name(), "vacuum");
        assert!(!into.is_write(), "VACUUM INTO reads the source and writes another file");
        assert!(connection.prepare("VACUUM").expect("plain vacuum parses").is_write());
        assert!(connection.prepare("VACUUM").expect("plain vacuum parses").columns().is_none());
    }

    #[test]
    fn prepare_enforces_exactly_one_statement() {
        let connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        assert_eq!(connection.prepare("").expect_err("empty input").code, "empty-statement");
        assert_eq!(connection.prepare("SELECT 1; SELECT 2").expect_err("two statements").code, "multiple-statements");
    }

    /// 连接层注册的宿主函数在 `prepare` 与 `execute` 两条路径上都可解析求值。
    #[test]
    fn host_functions_registered_on_the_connection_reach_prepare_and_execute() {
        use database_udf::Result as UdfResult;
        use database_udf::{HostFunctionHandle, HostMicroDefinition, UdfError, UdfType, UdfValue};

        struct Constant;
        impl HostRuntimeAdapter for Constant {
            fn invoke_scalar(&self, _handle: &HostFunctionHandle, _args: &[UdfValue]) -> UdfResult<UdfValue> {
                Ok(UdfValue::I64(42))
            }

            fn invoke_batch(&self, _handle: &HostFunctionHandle, _batches: &[Vec<UdfValue>]) -> UdfResult<Vec<UdfValue>> {
                Err(UdfError::UnsupportedEffect)
            }
        }

        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        let definition =
            HostMicroDefinition::from_scalar("answer", 1, Vec::new(), UdfType::I64, 7, "host/answer", 1, [0u8; 32]).expect("definition");
        connection.register_host_function(definition, Arc::new(Constant)).expect("register host function");
        let prepared = connection.prepare("SELECT answer()").expect("host function parses");
        assert_eq!(connection.run(&prepared, &[]).expect("prepared run").rows, vec![vec![Cell::Integer(42)]]);
        assert_eq!(connection.execute("SELECT ANSWER()", &[]).expect("script run")[0].rows, vec![vec![Cell::Integer(42)]]);
    }

    #[test]
    fn run_rejects_missing_parameters() {
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect("table exists");
        let prepared = connection.prepare("INSERT INTO t VALUES (?, ?)").expect("parses");
        let error = connection.run(&prepared, &[Cell::Integer(1)]).expect_err("one parameter short");
        assert_eq!(error.code, "bind-parameter");
        assert_eq!(error.statement, Some("insert"));
    }

    #[test]
    fn read_only_rejects_writes_before_execution() {
        let mut connection = Connection::open(ConnectionOptions { read_only: true, ..Default::default() }).expect("read-only connection");
        let error = connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect_err("write rejected");
        assert_eq!(error.code, "read-only");
        let prepared = connection.prepare("INSERT INTO t VALUES (1)").expect("parses");
        assert_eq!(connection.run(&prepared, &[]).expect_err("write rejected").code, "read-only");
        let rows = connection.execute("SELECT 1", &[]).expect("reads still work").last().expect("result").rows.clone();
        assert_eq!(rows, vec![vec![Cell::Integer(1)]]);
    }

    /// 建表并一次性插入 `rows` 行 `(id, label)`,行内容可独立预言。
    fn seed_rows(connection: &mut Connection, rows: usize) {
        connection.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT NOT NULL)", &[]).expect("create");
        let mut sql = String::from("INSERT INTO t (id, label) VALUES ");
        for id in 1..=rows {
            if id > 1 {
                sql.push(',');
            }
            sql.push_str(&format!("({id}, 'row-{id}')"));
        }
        connection.execute(&sql, &[]).expect("seed");
    }

    #[test]
    fn cursors_read_bounded_windows_in_order() {
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        seed_rows(&mut connection, 1000);
        let prepared = connection.prepare("SELECT id, label FROM t ORDER BY id").expect("parses");
        let opened = connection.open_cursor(&prepared, &[]).expect("cursor opens");
        assert_eq!(opened.columns, vec!["id".to_string(), "label".to_string()]);

        let mut seen = 0_i64;
        loop {
            let batch = connection.cursor_next(opened.handle, 256).expect("window");
            assert!(batch.rows.len() <= 256, "the window is bounded");
            for row in &batch.rows {
                seen += 1;
                assert_eq!(row, &vec![Cell::Integer(seen), Cell::Text(format!("row-{seen}"))]);
            }
            if batch.done {
                break;
            }
        }
        assert_eq!(seen, 1000);

        connection.cursor_close(opened.handle).expect("close");
        connection.cursor_close(opened.handle).expect("close is idempotent");
        assert_eq!(connection.cursor_count(), 0);
        assert_eq!(connection.cursor_next(opened.handle, 1).expect_err("released").code, "cursor-not-found");
    }

    #[test]
    fn two_cursors_on_one_connection_read_independently() {
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        seed_rows(&mut connection, 10);
        let ascending = connection.prepare("SELECT id FROM t ORDER BY id").expect("parses");
        let descending = connection.prepare("SELECT id FROM t ORDER BY id DESC").expect("parses");
        let first = connection.open_cursor(&ascending, &[]).expect("first cursor");
        let second = connection.open_cursor(&descending, &[]).expect("second cursor");

        assert_eq!(connection.cursor_next(first.handle, 3).expect("first window").rows, vec![
            vec![Cell::Integer(1)],
            vec![Cell::Integer(2)],
            vec![Cell::Integer(3)]
        ]);
        assert_eq!(connection.cursor_next(second.handle, 3).expect("second window").rows, vec![
            vec![Cell::Integer(10)],
            vec![Cell::Integer(9)],
            vec![Cell::Integer(8)]
        ]);
        assert_eq!(connection.cursor_next(first.handle, 3).expect("first continues").rows, vec![
            vec![Cell::Integer(4)],
            vec![Cell::Integer(5)],
            vec![Cell::Integer(6)]
        ]);
        assert_eq!(connection.cursor_next(second.handle, 20).expect("second finishes").done, true);
        connection.cursor_close(first.handle).expect("close first");
        connection.cursor_close(second.handle).expect("close second");
    }

    #[test]
    fn a_transaction_ending_invalidates_cursors_opened_inside_it() {
        for control in ["ROLLBACK", "COMMIT"] {
            let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
            seed_rows(&mut connection, 10);
            let prepared = connection.prepare("SELECT id FROM t ORDER BY id").expect("parses");
            connection.execute("BEGIN", &[]).expect("begin");
            let opened = connection.open_cursor(&prepared, &[]).expect("cursor opens inside the transaction");
            assert_eq!(connection.cursor_next(opened.handle, 2).expect("rows before the end").rows.len(), 2);
            connection.execute(control, &[]).expect("transaction ends");
            let error = connection.cursor_next(opened.handle, 2).expect_err("the cursor is stale");
            assert_eq!(error.code, "cursor-expired");
            connection.cursor_close(opened.handle).expect("the stale cursor still closes");
            assert_eq!(connection.cursor_count(), 0);
        }
    }

    #[test]
    fn an_autocommit_cursor_survives_a_later_transaction() {
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        seed_rows(&mut connection, 10);
        let prepared = connection.prepare("SELECT id FROM t ORDER BY id").expect("parses");
        let opened = connection.open_cursor(&prepared, &[]).expect("autocommit cursor");
        connection.execute("BEGIN", &[]).expect("begin");
        connection.execute("COMMIT", &[]).expect("commit");
        assert_eq!(connection.cursor_next(opened.handle, 3).expect("still valid").rows[0], vec![Cell::Integer(1)]);
    }

    #[test]
    fn closing_the_connection_invalidates_open_cursors() {
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        seed_rows(&mut connection, 10);
        let prepared = connection.prepare("SELECT id FROM t ORDER BY id").expect("parses");
        let opened = connection.open_cursor(&prepared, &[]).expect("cursor opens");
        connection.close().expect("close");
        assert_eq!(connection.cursor_next(opened.handle, 1).expect_err("closed").code, "db-closed");
    }

    /// 建表并分批插入 `rows` 行 `(id, row-id)`,供大表游标用例使用。
    fn seed_large_rows(connection: &mut Connection, rows: usize) {
        connection.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT NOT NULL)", &[]).expect("create");
        let mut start = 1;
        while start <= rows {
            let end = (start + 999).min(rows);
            let mut sql = String::from("INSERT INTO t (id, label) VALUES ");
            for id in start..=end {
                if id > start {
                    sql.push(',');
                }
                sql.push_str(&format!("({id}, 'row-{id}')"));
            }
            connection.execute(&sql, &[]).expect("seed chunk");
            start = end + 1;
        }
    }

    /// 50k 行简单扫描:惰性游标逐批读取,行数/顺序与物化路径一致,常驻内存
    /// 增量显著低于一次性 `all()`。
    #[test]
    fn a_large_simple_scan_streams_lazily_and_uses_less_memory_than_all() {
        let rows = 50_000usize;
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        seed_large_rows(&mut connection, rows);
        let prepared = connection.prepare("SELECT * FROM t").expect("parses");
        let baseline = resident_bytes();

        let opened = connection.open_cursor(&prepared, &[]).expect("lazy cursor opens");
        assert!(opened.lazy, "a plain single-table scan must stream lazily");
        assert_eq!(opened.columns, vec!["id".to_string(), "label".to_string()]);
        let mut seen = 0usize;
        loop {
            let batch = connection.cursor_next(opened.handle, 256).expect("window");
            assert!(batch.rows.len() <= 256, "the lazy window is bounded");
            for row in &batch.rows {
                seen += 1;
                assert_eq!(row, &vec![Cell::Integer(seen as i64), Cell::Text(format!("row-{seen}"))]);
            }
            if batch.done {
                break;
            }
        }
        assert_eq!(seen, rows, "the lazy cursor returns every row in rowid order");
        connection.cursor_close(opened.handle).expect("close");
        let lazy_rss = resident_bytes();

        // Oracle: the same statement through the materialized path.
        let materialized = connection.execute("SELECT * FROM t", &[]).expect("materialized scan").pop().expect("one result");
        assert_eq!(materialized.rows.len(), rows);
        for (index, row) in materialized.rows.iter().enumerate() {
            assert_eq!(row, &vec![Cell::Integer(index as i64 + 1), Cell::Text(format!("row-{}", index + 1))]);
        }
        let materialized_rss = resident_bytes();

        if let (Some(baseline), Some(lazy), Some(materialized)) = (baseline, lazy_rss, materialized_rss) {
            let lazy_delta = lazy.saturating_sub(baseline);
            let materialized_delta = materialized.saturating_sub(baseline);
            println!("cursor lazy RSS delta: {lazy_delta} bytes; all() RSS delta: {materialized_delta} bytes");
            assert!(
                lazy_delta < materialized_delta,
                "the lazy cursor must use less resident memory than all(): lazy {lazy_delta}, materialized {materialized_delta}"
            );
        }
    }

    /// 打开游标后的写入不影响游标:它继续读打开时刻的一致快照。
    #[test]
    fn a_lazy_cursor_keeps_the_snapshot_taken_at_open() {
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        seed_rows(&mut connection, 20);
        let prepared = connection.prepare("SELECT id, label FROM t").expect("parses");
        let opened = connection.open_cursor(&prepared, &[]).expect("cursor opens");
        assert!(opened.lazy);
        assert_eq!(connection.cursor_next(opened.handle, 5).expect("first window").rows, (1..=5)
            .map(|id| vec![Cell::Integer(id), Cell::Text(format!("row-{id}"))])
            .collect::<Vec<_>>());

        connection.execute("UPDATE t SET label = 'changed' WHERE id = 1", &[]).expect("update after open");
        connection.execute("INSERT INTO t (id, label) VALUES (21, 'row-21')", &[]).expect("insert after open");

        let mut seen = 5usize;
        loop {
            let batch = connection.cursor_next(opened.handle, 4).expect("snapshot window");
            for row in &batch.rows {
                seen += 1;
                assert_eq!(row, &vec![Cell::Integer(seen as i64), Cell::Text(format!("row-{seen}"))]);
            }
            if batch.done {
                break;
            }
        }
        assert_eq!(seen, 20, "the cursor still sees exactly the rows present at open");
        connection.cursor_close(opened.handle).expect("close");

        let fresh = connection.execute("SELECT id, label FROM t", &[]).expect("fresh statement").pop().expect("one result").rows;
        assert_eq!(fresh.len(), 21, "later statements see the writes");
        assert_eq!(fresh[0], vec![Cell::Integer(1), Cell::Text("changed".to_string())]);
    }

    /// `JOIN`、`GROUP BY`、非 rowid 顺序的 `ORDER BY`、`DISTINCT` 与
    /// `LIMIT` 一律走既有物化路径,结果与直接执行一致。
    #[test]
    fn complex_statements_stay_on_the_materialized_cursor_path() {
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        seed_rows(&mut connection, 10);
        connection.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, tag TEXT NOT NULL)", &[]).expect("create u");
        connection
            .execute("INSERT INTO u (id, tag) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h'), (9, 'i'), (10, 'j')", &[])
            .expect("seed u");
        for sql in [
            "SELECT t.id, u.tag FROM t JOIN u ON t.id = u.id",
            "SELECT label, count(*) FROM t GROUP BY label",
            "SELECT id FROM t ORDER BY label DESC",
            "SELECT DISTINCT label FROM t",
            "SELECT id FROM t LIMIT 5",
        ] {
            let prepared = connection.prepare(sql).unwrap_or_else(|error| panic!("prepare `{sql}`: {error}"));
            let opened = connection.open_cursor(&prepared, &[]).unwrap_or_else(|error| panic!("open `{sql}`: {error}"));
            assert!(!opened.lazy, "`{sql}` must stay on the materialized path");
            let mut rows = Vec::new();
            loop {
                let batch = connection.cursor_next(opened.handle, 3).unwrap_or_else(|error| panic!("next `{sql}`: {error}"));
                rows.extend(batch.rows);
                if batch.done {
                    break;
                }
            }
            let expected = connection.execute(sql, &[]).unwrap_or_else(|error| panic!("run `{sql}`: {error}")).pop().expect("one result").rows;
            assert_eq!(rows, expected, "cursor rows of `{sql}`");
            connection.cursor_close(opened.handle).expect("close");
        }
    }

    #[test]
    fn autocommit_write_publishes_to_the_file() {
        let path = temp_path("publish");
        let mut connection =
            Connection::open(ConnectionOptions { filename: Some(path.clone()), ..Default::default() }).expect("file connection");
        connection.execute("CREATE TABLE t (a INTEGER, b TEXT)", &[]).expect("create publishes");
        let prepared = connection.prepare("INSERT INTO t VALUES (?, ?)").expect("parses");
        connection.run(&prepared, &[Cell::Integer(7), Cell::Text("seven".to_string())]).expect("insert publishes");
        assert_eq!(rows_on_disk(&path, "SELECT a, b FROM t"), vec![vec![Cell::Integer(7), Cell::Text("seven".to_string())]]);
        fs::remove_dir_all(&path.parent().expect("parent directory")).expect("cleanup");
    }

    #[test]
    fn transaction_defers_publish_until_commit() {
        let path = temp_path("commit");
        let mut connection =
            Connection::open(ConnectionOptions { filename: Some(path.clone()), ..Default::default() }).expect("file connection");
        connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect("create publishes");
        connection.execute("BEGIN", &[]).expect("transaction opens");
        connection.execute("INSERT INTO t VALUES (1)", &[]).expect("insert defers");
        assert!(rows_on_disk(&path, "SELECT a FROM t").is_empty(), "the file must not see uncommitted rows");
        assert!(connection.in_transaction());
        connection.execute("COMMIT", &[]).expect("commit publishes");
        assert_eq!(rows_on_disk(&path, "SELECT a FROM t"), vec![vec![Cell::Integer(1)]]);
        fs::remove_dir_all(&path.parent().expect("parent directory")).expect("cleanup");
    }

    #[test]
    fn rollback_leaves_the_file_unchanged() {
        let path = temp_path("rollback");
        let mut connection =
            Connection::open(ConnectionOptions { filename: Some(path.clone()), ..Default::default() }).expect("file connection");
        connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect("create publishes");
        connection.execute("INSERT INTO t VALUES (1)", &[]).expect("first row publishes");
        connection.execute("BEGIN", &[]).expect("transaction opens");
        connection.execute("INSERT INTO t VALUES (2)", &[]).expect("second row defers");
        connection.execute("ROLLBACK", &[]).expect("rollback discards");
        assert_eq!(rows_on_disk(&path, "SELECT a FROM t"), vec![vec![Cell::Integer(1)]]);
        fs::remove_dir_all(&path.parent().expect("parent directory")).expect("cleanup");
    }

    #[test]
    fn close_keeps_an_open_transaction_out_of_the_file() {
        let path = temp_path("close");
        let mut connection =
            Connection::open(ConnectionOptions { filename: Some(path.clone()), ..Default::default() }).expect("file connection");
        connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect("create publishes");
        connection.execute("BEGIN", &[]).expect("transaction opens");
        connection.execute("INSERT INTO t VALUES (1)", &[]).expect("insert defers");
        connection.close().expect("close without publish");
        assert!(rows_on_disk(&path, "SELECT a FROM t").is_empty(), "an open transaction must not reach the file");
        fs::remove_dir_all(&path.parent().expect("parent directory")).expect("cleanup");
    }

    #[test]
    fn backup_writes_the_current_image_to_another_file() {
        let directory = temp_path("backup");
        let target = directory.with_file_name("copy.sqlite");
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect("table exists");
        connection.execute("INSERT INTO t VALUES (9)", &[]).expect("row exists");
        connection.backup(&target).expect("backup");
        assert_eq!(rows_on_disk(&target, "SELECT a FROM t"), vec![vec![Cell::Integer(9)]]);
        fs::remove_dir_all(directory.parent().expect("parent directory")).expect("cleanup");
    }

    #[test]
    fn sqlite_version_reports_the_format_three_library_stamp() {
        let connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        assert_eq!(connection.sqlite_version().expect("header carries a version"), crate::ENGINE_LIBRARY_VERSION);
    }

    /// 对照用例:一批覆盖各类行集语句的 SQL,`prepare` 推导的列标签必须与
    /// 执行后 `ExecResult.columns` 逐字节相等。任何一条不等都说明 describe
    /// 路径与执行路径的标签推导漂移了。
    #[test]
    fn prepared_columns_match_the_executed_header() {
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        for setup in [
            "CREATE TABLE t1 (a INTEGER, b TEXT)",
            "CREATE TABLE t2 (a INTEGER, y TEXT)",
            "CREATE TABLE sp (\"A\" TEXT, bb INTEGER)",
            "CREATE TABLE wo (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
            "INSERT INTO t1 VALUES (1, 'one'), (2, 'two')",
            "INSERT INTO t2 VALUES (1, 'x'), (3, 'z')",
            "INSERT INTO sp VALUES ('s', 7)",
            "INSERT INTO wo VALUES ('k', 1)",
            "CREATE VIEW v1 AS SELECT a, b FROM t1",
            "CREATE VIEW v2 (c, d) AS SELECT a, a FROM t1",
        ] {
            connection.execute(setup, &[]).unwrap_or_else(|error| panic!("setup `{setup}`: {error}"));
        }
        let queries = [
            "SELECT 1",
            "SELECT a, b FROM t1",
            "SELECT A, bb FROM sp",
            "SELECT a AS one, b FROM t1",
            "SELECT a+1, upper(b) FROM t1",
            "SELECT * FROM t1",
            "SELECT * FROM t1, t2",
            "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a",
            "SELECT * FROM t1 JOIN t2 USING (a)",
            "SELECT * FROM t1 NATURAL JOIN t2",
            "SELECT * FROM wo",
            "SELECT count(*) FROM t1",
            "SELECT rowid, a FROM t1",
            "SELECT a, a FROM t1",
            "SELECT 1 UNION SELECT 2",
            "SELECT a FROM t1 UNION ALL SELECT a FROM t2 ORDER BY 1",
            "WITH c AS (SELECT a FROM t1) SELECT * FROM c",
            "WITH c(x) AS (SELECT a FROM t1) SELECT x FROM c",
            "WITH c AS (SELECT a AS z FROM t1) SELECT z FROM c",
            "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) SELECT n FROM r",
            "SELECT * FROM v1",
            "SELECT * FROM v2",
            "SELECT * FROM (SELECT a, b FROM t1) AS d",
            "SELECT * FROM (SELECT a, a FROM t1)",
            "SELECT * FROM sqlite_master",
            "SELECT b FROM t1 WHERE b IS NOT NULL ORDER BY 1",
            "SELECT DISTINCT b FROM t1",
            "SELECT b, count(*) FROM t1 GROUP BY b",
            "SELECT a FROM t1 GROUP BY a HAVING count(*) > 0",
            "SELECT a, row_number() OVER (ORDER BY a) FROM t1",
            "SELECT CASE WHEN a > 1 THEN 'big' ELSE 'small' END FROM t1",
            "SELECT b FROM t1 LIMIT 1 OFFSET 1",
            "PRAGMA table_info(t1)",
            "PRAGMA table_xinfo(t1)",
            "PRAGMA index_list(t1)",
            "PRAGMA foreign_keys",
            "PRAGMA database_list",
            "PRAGMA integrity_check",
            "EXPLAIN SELECT a FROM t1",
            "EXPLAIN QUERY PLAN SELECT a FROM t1",
            "INSERT INTO t1 VALUES (10, 'ten') RETURNING a, b",
            "INSERT INTO t1 VALUES (11, 'eleven') RETURNING *",
            "UPDATE t1 SET b = 'changed' WHERE a = 10 RETURNING a",
            "DELETE FROM t1 WHERE a = 11 RETURNING b",
        ];
        for query in queries {
            let prepared = connection.prepare(query).unwrap_or_else(|error| panic!("prepare `{query}`: {error}"));
            let results = connection.execute(query, &[]).unwrap_or_else(|error| panic!("run `{query}`: {error}"));
            let executed = results.last().expect("one result").columns.clone();
            assert_eq!(prepared.columns(), Some(executed), "columns of `{query}`");
        }
    }

    /// 非行集语句(无 `RETURNING` 的 DML、DDL、事务控制、写形 `PRAGMA`)
    /// 在 prepare 期如实返回 `None`。
    #[test]
    fn prepared_columns_report_none_without_a_rowset() {
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        connection.execute("CREATE TABLE t1 (a INTEGER, b TEXT)", &[]).expect("table exists");
        connection.execute("INSERT INTO t1 VALUES (1, 'one')", &[]).expect("row exists");
        for query in [
            "INSERT INTO t1 VALUES (2, 'a')",
            "UPDATE t1 SET b = 'z'",
            "DELETE FROM t1 WHERE a = 2",
            "CREATE TABLE other (i INTEGER)",
            "DROP TABLE other",
            "BEGIN",
            "COMMIT",
            "PRAGMA foreign_keys = ON",
            "PRAGMA user_version = 7",
        ] {
            let prepared = connection.prepare(query).unwrap_or_else(|error| panic!("prepare `{query}`: {error}"));
            assert!(prepared.columns().is_none(), "no rowset for `{query}`");
            connection.execute(query, &[]).unwrap_or_else(|error| panic!("run `{query}`: {error}"));
        }
    }

    /// 目录解析失败不改变拒绝口径:`prepare` 仍然成功、标签如实为 `None`,
    /// 缺表与缺列错误留在执行期,错误码与既有行为一致。
    #[test]
    fn prepare_keeps_run_time_errors_out_of_the_columns_path() {
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        connection.execute("CREATE TABLE t1 (a INTEGER)", &[]).expect("table exists");
        let prepared = connection.prepare("SELECT * FROM missing_table").expect("prepare still succeeds");
        assert_eq!(prepared.columns(), None, "an unknown source has no derivable header");
        let error = connection.execute("SELECT * FROM missing_table", &[]).expect_err("run reports the missing table");
        assert_eq!(error.code, "unknown-table");
        let prepared = connection.prepare("SELECT bogus FROM t1").expect("prepare still succeeds");
        assert_eq!(prepared.columns(), None, "an unknown projection column has no derivable header");
        let error = connection.execute("SELECT bogus FROM t1", &[]).expect_err("run reports the missing column");
        assert_eq!(error.code, "unknown-column");
    }

    /// 诚实性红线:`prepare` 绝不执行语句——预编译一条 `INSERT` 不会写入任何行。
    #[test]
    fn prepare_does_not_execute_the_statement() {
        let mut connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect("table exists");
        let prepared = connection.prepare("INSERT INTO t VALUES (1)").expect("parses");
        assert!(prepared.is_write());
        let rows = connection.execute("SELECT a FROM t", &[]).expect("read back").last().expect("result").rows.clone();
        assert!(rows.is_empty(), "prepare must not insert");
    }

    /// 只读连接同样能在 prepare 期拿到表头;写语句的 `RETURNING` 标签照常
    /// 推导,拒绝仍发生在执行期,与既有只读语义一致。
    #[test]
    fn read_only_connections_still_describe_prepared_statements() {
        let path = temp_path("describe-readonly");
        {
            let mut writer =
                Connection::open(ConnectionOptions { filename: Some(path.clone()), ..Default::default() }).expect("file connection");
            writer.execute("CREATE TABLE t (a INTEGER, b TEXT)", &[]).expect("create publishes");
            writer.execute("INSERT INTO t VALUES (1, 'one')", &[]).expect("insert publishes");
        }
        let mut reader = Connection::open(ConnectionOptions { filename: Some(path.clone()), read_only: true, ..Default::default() })
            .expect("read-only connection");
        let prepared = reader.prepare("SELECT * FROM t").expect("parses");
        assert_eq!(prepared.columns(), Some(vec!["a".to_string(), "b".to_string()]));
        let executed = reader.run(&prepared, &[]).expect("read runs").columns;
        assert_eq!(prepared.columns(), Some(executed));
        let insert = reader.prepare("INSERT INTO t VALUES (2, 'two') RETURNING a").expect("parses");
        assert_eq!(insert.columns(), Some(vec!["a".to_string()]));
        assert_eq!(reader.run(&insert, &[]).expect_err("write rejected").code, "read-only");
        fs::remove_dir_all(path.parent().expect("parent directory")).expect("cleanup");
    }

    #[test]
    fn journal_mode_parses_only_the_supported_names() {
        assert_eq!(JournalMode::parse("preserve").expect("preserve"), JournalMode::Preserve);
        assert_eq!(JournalMode::parse("DELETE").expect("delete"), JournalMode::Delete);
        assert_eq!(JournalMode::parse(" truncate ").expect("truncate"), JournalMode::Truncate);
        assert_eq!(JournalMode::parse("wal").expect_err("wal is not implemented").code, "unsupported");
        assert_eq!(JournalMode::parse("off").expect_err("off is not a rollback journal").code, "unsupported");
        assert_eq!(JournalMode::Delete.as_str(), "delete");
    }

    /// 目录里不得留下发布或恢复用的临时文件。
    fn assert_no_temporary_files(path: &Path) {
        for entry in fs::read_dir(path.parent().expect("parent directory")).expect("list directory") {
            let name = entry.expect("entry").file_name().to_string_lossy().into_owned();
            assert!(!name.contains(".tmp") && !name.contains(".recover"), "leftover temporary file: {name}");
        }
    }

    #[test]
    fn delete_journal_is_removed_after_every_commit() {
        let path = temp_path("journal-delete");
        let mut connection = Connection::open(ConnectionOptions {
            filename: Some(path.clone()),
            journal_mode: JournalMode::Delete,
            ..Default::default()
        })
        .expect("file connection");
        let journal = database_storage::sqlite::journal::sidecar(&path);
        connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect("create publishes");
        assert!(!journal.exists(), "a finished commit leaves no journal in delete mode");
        connection.execute("INSERT INTO t VALUES (1)", &[]).expect("insert publishes");
        assert!(!journal.exists(), "a finished commit leaves no journal in delete mode");
        assert_eq!(rows_on_disk(&path, "SELECT a FROM t"), vec![vec![Cell::Integer(1)]]);
        connection.close().expect("close");
        assert_no_temporary_files(&path);
        fs::remove_dir_all(path.parent().expect("parent directory")).expect("cleanup");
    }

    #[test]
    fn truncate_journal_stays_as_a_zero_byte_file_after_commit() {
        let path = temp_path("journal-truncate");
        let mut connection = Connection::open(ConnectionOptions {
            filename: Some(path.clone()),
            journal_mode: JournalMode::Truncate,
            ..Default::default()
        })
        .expect("file connection");
        let journal = database_storage::sqlite::journal::sidecar(&path);
        connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect("create publishes");
        connection.execute("INSERT INTO t VALUES (7)", &[]).expect("insert publishes");
        assert!(journal.exists(), "truncate mode keeps a zero-byte sidecar");
        assert_eq!(fs::metadata(&journal).expect("journal metadata").len(), 0);
        assert_eq!(rows_on_disk(&path, "SELECT a FROM t"), vec![vec![Cell::Integer(7)]]);
        connection.close().expect("close");
        assert_no_temporary_files(&path);
        fs::remove_dir_all(path.parent().expect("parent directory")).expect("cleanup");
    }

    /// 主文件已发布、日志尚未清理时崩溃:重新打开必须回滚到旧镜像。
    #[test]
    fn a_hot_journal_rolls_back_a_published_commit_on_reopen() {
        let path = temp_path("journal-after-publish");
        let mut connection = Connection::open(ConnectionOptions {
            filename: Some(path.clone()),
            journal_mode: JournalMode::Delete,
            ..Default::default()
        })
        .expect("file connection");
        connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect("create publishes");
        connection.execute("INSERT INTO t VALUES (1)", &[]).expect("insert publishes");
        let before = fs::read(&path).expect("previous image");
        let journal = database_storage::sqlite::journal::sidecar(&path);

        connection.fail_point = Some(FailPoint::AfterPublish);
        let error = connection.execute("INSERT INTO t VALUES (2)", &[]).expect_err("injected crash");
        assert_eq!(error.code, "injected");
        drop(connection);

        assert!(journal.exists(), "the journal outlives the interrupted commit");
        assert_ne!(fs::read(&path).expect("published image"), before, "the publish really replaced the main file");

        // A read-only reopen replays the journal in memory and leaves the disk alone.
        let mut reader = Connection::open(ConnectionOptions { filename: Some(path.clone()), read_only: true, ..Default::default() })
            .expect("read-only reopen");
        assert_eq!(reader.execute("SELECT a FROM t", &[]).expect("read")[0].rows, vec![vec![Cell::Integer(1)]]);
        drop(reader);
        assert!(journal.exists(), "a read-only connection must not delete the journal");

        // A writable reopen restores the old image first, then clears the journal.
        let mut reopened = Connection::open(ConnectionOptions {
            filename: Some(path.clone()),
            journal_mode: JournalMode::Delete,
            ..Default::default()
        })
        .expect("writable reopen");
        assert_eq!(reopened.execute("SELECT a FROM t", &[]).expect("read")[0].rows, vec![vec![Cell::Integer(1)]]);
        assert!(!journal.exists(), "recovery clears the journal before the next commit");
        assert_eq!(fs::read(&path).expect("restored image"), before);
        reopened.close().expect("close");
        assert_no_temporary_files(&path);
        fs::remove_dir_all(path.parent().expect("parent directory")).expect("cleanup");
    }

    /// 日志已落盘、主文件尚未发布时崩溃:主文件保持旧镜像,恢复不产生半镜像。
    #[test]
    fn a_journal_without_a_published_image_keeps_the_old_main_file() {
        let path = temp_path("journal-after-journal");
        let mut connection = Connection::open(ConnectionOptions {
            filename: Some(path.clone()),
            journal_mode: JournalMode::Truncate,
            ..Default::default()
        })
        .expect("file connection");
        connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect("create publishes");
        connection.execute("INSERT INTO t VALUES (1)", &[]).expect("insert publishes");
        let before = fs::read(&path).expect("previous image");

        connection.fail_point = Some(FailPoint::AfterJournal);
        assert_eq!(connection.execute("INSERT INTO t VALUES (2)", &[]).expect_err("injected crash").code, "injected");
        drop(connection);

        let journal = database_storage::sqlite::journal::sidecar(&path);
        assert!(fs::metadata(&journal).expect("journal exists").len() > 0, "the journal holds the old page images");
        assert_eq!(fs::read(&path).expect("main file untouched"), before, "no half image reached the main file");

        let mut reopened = Connection::open(ConnectionOptions {
            filename: Some(path.clone()),
            journal_mode: JournalMode::Truncate,
            ..Default::default()
        })
        .expect("writable reopen");
        assert_eq!(reopened.execute("SELECT a FROM t", &[]).expect("read")[0].rows, vec![vec![Cell::Integer(1)]]);
        assert_eq!(fs::metadata(&journal).expect("journal exists").len(), 0, "recovery honours truncate mode");
        reopened.close().expect("close");
        assert_no_temporary_files(&path);
        fs::remove_dir_all(path.parent().expect("parent directory")).expect("cleanup");
    }

    /// 外部 SQLite 引擎必须能识别本绑定写出的 rollback journal 并完成回滚。
    #[test]
    fn the_reference_engine_rolls_back_a_hot_journal_written_by_this_binding() {
        let python = reference_python().expect("a Python interpreter with sqlite3 is required for the interop test");
        let path = temp_path("journal-interop");
        let mut connection = Connection::open(ConnectionOptions {
            filename: Some(path.clone()),
            journal_mode: JournalMode::Delete,
            ..Default::default()
        })
        .expect("file connection");
        connection.execute("CREATE TABLE t (a INTEGER)", &[]).expect("create publishes");
        connection.execute("INSERT INTO t VALUES (1)", &[]).expect("insert publishes");
        connection.fail_point = Some(FailPoint::AfterPublish);
        assert_eq!(connection.execute("INSERT INTO t VALUES (2)", &[]).expect_err("injected crash").code, "injected");
        drop(connection);

        let script = r#"
import sqlite3, sys
db = sqlite3.connect(sys.argv[1])
assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",), "integrity_check"
assert db.execute("SELECT count(*) FROM t").fetchone() == (1,), "the hot journal restores the previous image"
db.close()
"#;
        let output = std::process::Command::new(python)
            .args(["-c", script, path.to_str().expect("utf-8 path")])
            .output()
            .expect("spawn upstream sqlite");
        assert!(output.status.success(), "upstream SQLite rejected the rollback journal: {}", String::from_utf8_lossy(&output.stderr));
        assert!(!database_storage::sqlite::journal::sidecar(&path).exists(), "SQLite removes the journal after rollback");

        let mut reopened = Connection::open(ConnectionOptions {
            filename: Some(path.clone()),
            journal_mode: JournalMode::Delete,
            ..Default::default()
        })
        .expect("reopen after the external rollback");
        assert_eq!(reopened.execute("SELECT a FROM t", &[]).expect("read")[0].rows, vec![vec![Cell::Integer(1)]]);
        fs::remove_dir_all(path.parent().expect("parent directory")).expect("cleanup");
    }

    /// 第一个装有 `sqlite3` 模块的解释器，与 facade 其他互操作用例同一探测口径。
    fn reference_python() -> Option<String> {
        ["python3", "python"].into_iter().find_map(|candidate| {
            std::process::Command::new(candidate)
                .args(["-c", "import sqlite3"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
                .then(|| candidate.to_string())
        })
    }

    #[test]
    fn a_second_writer_is_rejected_with_busy_after_the_timeout() {
        let path = temp_path("busy");
        let mut first = Connection::open(ConnectionOptions { filename: Some(path.clone()), ..Default::default() }).expect("first writer");
        first.execute("CREATE TABLE t (a INTEGER)", &[]).expect("create publishes");

        let started = Instant::now();
        let error = Connection::open(ConnectionOptions { filename: Some(path.clone()), busy_timeout_ms: 150, ..Default::default() })
            .expect_err("the file is already locked");
        assert_eq!(error.code, "busy");
        assert!(error.message.contains("SQLITE_BUSY"), "{}", error.message);
        assert!(started.elapsed() >= Duration::from_millis(120), "the busy handler must honour the timeout");
        assert!(started.elapsed() < Duration::from_secs(5), "the busy handler must not wait past the timeout");

        // An immediate attempt with no timeout is rejected at once.
        let started = Instant::now();
        let error = Connection::open(ConnectionOptions { filename: Some(path.clone()), ..Default::default() })
            .expect_err("a zero timeout makes one attempt");
        assert_eq!(error.code, "busy");
        assert!(started.elapsed() < Duration::from_millis(200));

        // A read-only connection never waits for the writer lock.
        let mut reader =
            Connection::open(ConnectionOptions { filename: Some(path.clone()), read_only: true, ..Default::default() }).expect("reader");
        let rows = reader.execute("SELECT a FROM t", &[]).expect("read while the writer holds the lock");
        assert!(rows.last().expect("result").rows.is_empty());
        drop(reader);

        // Closing releases the lock and refuses further statements.
        first.close().expect("close publishes and releases");
        assert_eq!(first.execute("SELECT 1", &[]).expect_err("closed connection").code, "db-closed");
        let second = Connection::open(ConnectionOptions { filename: Some(path.clone()), ..Default::default() }).expect("lock released");
        drop(second);
        fs::remove_dir_all(path.parent().expect("parent directory")).expect("cleanup");
    }

    /// 交叉进程测试的持锁端:由 `cross_process_writers_are_mutually_exclusive`
    /// 以子进程方式启动,拿到写锁后写就绪标记,等待停止标记后直接退出进程
    /// (不调用 `close`),由操作系统回收 advisory 锁。
    #[test]
    #[ignore = "spawned as a child process by the cross-process lock test"]
    fn cross_process_lock_holder() {
        let database = PathBuf::from(std::env::var("YYDB_SQLITE_LOCK_DB").expect("lock holder database path"));
        let ready = PathBuf::from(std::env::var("YYDB_SQLITE_LOCK_READY").expect("lock holder ready marker"));
        let stop = PathBuf::from(std::env::var("YYDB_SQLITE_LOCK_STOP").expect("lock holder stop marker"));
        let mut connection = Connection::open(ConnectionOptions { filename: Some(database), ..Default::default() }).expect("lock holder opens");
        connection.execute("CREATE TABLE held (a INTEGER)", &[]).expect("create publishes");
        connection.execute("INSERT INTO held VALUES (7)", &[]).expect("insert publishes");
        fs::write(&ready, std::process::id().to_string()).expect("ready marker");
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline && !stop.exists() {
            std::thread::sleep(Duration::from_millis(10));
        }
        // No `close`, no destructors: the OS releases the advisory lock at exit.
        std::process::exit(0);
    }

    #[test]
    fn cross_process_writers_are_mutually_exclusive() {
        use std::process::{Command, Stdio};

        let directory = temp_path("cross-process").parent().expect("parent directory").to_path_buf();
        let database = directory.join("db.sqlite");
        let ready = directory.join("ready");
        let stop = directory.join("stop");
        let executable = std::env::current_exe().expect("test executable");
        let mut child = Command::new(executable)
            .args(["--exact", "connection::tests::cross_process_lock_holder", "--ignored"])
            .env("YYDB_SQLITE_LOCK_DB", &database)
            .env("YYDB_SQLITE_LOCK_READY", &ready)
            .env("YYDB_SQLITE_LOCK_STOP", &stop)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn lock holder");
        let deadline = Instant::now() + Duration::from_secs(20);
        while !ready.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "the lock holder must publish its row before the parent races it");

        // Another process holding the lock is a `busy` rejection here, and the
        // diagnostic names the holder.
        let error = Connection::open(ConnectionOptions { filename: Some(database.clone()), ..Default::default() })
            .expect_err("a second process must not share the write lock");
        assert_eq!(error.code, "busy");
        assert!(error.message.contains("SQLITE_BUSY"), "{}", error.message);
        // The timeout really waits for the lock before rejecting.
        let started = Instant::now();
        let error = Connection::open(ConnectionOptions { filename: Some(database.clone()), busy_timeout_ms: 200, ..Default::default() })
            .expect_err("the holder keeps the lock");
        assert_eq!(error.code, "busy");
        assert!(started.elapsed() >= Duration::from_millis(150));

        // Readers are never blocked by the write lock and see the published image.
        let mut reader =
            Connection::open(ConnectionOptions { filename: Some(database.clone()), read_only: true, ..Default::default() }).expect("reader");
        let rows = reader.execute("SELECT a FROM held", &[]).expect("read while another process writes");
        assert_eq!(rows.last().expect("result").rows, vec![vec![Cell::Integer(7)]]);
        drop(reader);

        fs::write(&stop, b"stop").expect("stop marker");
        let status = child.wait().expect("lock holder exits");
        assert!(status.success());

        // The lock file records the owner for diagnostics; it survives the
        // crash and is overwritten by the next writer.
        let mut lock_path = database.as_os_str().to_os_string();
        lock_path.push(".lock");
        let recorded = fs::read_to_string(PathBuf::from(lock_path)).expect("the lock file records its last owner");
        assert_eq!(recorded.trim(), child.id().to_string(), "the lock file must record the holder PID");

        // The holder never called `close`: the OS still released the lock, so a
        // crash leaves no stale lock behind and the next writer proceeds.
        let writer = Connection::open(ConnectionOptions { filename: Some(database.clone()), ..Default::default() })
            .expect("a crashed holder must not keep the database locked");
        drop(writer);
        fs::remove_dir_all(&directory).expect("cleanup");
    }
}
