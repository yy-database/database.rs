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

use std::path::{Path, PathBuf};

use crate::{
    SqliteWriter,
    sql::{Cell, Engine, ExecResult, SqlError, Statement, TransactionAction, parameter_slots, parse},
};

/// 把存储层错误映射为结构化的 SQL 拒绝。
fn storage_error(error: impl std::fmt::Display) -> SqlError {
    SqlError::new("storage", error.to_string())
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
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self { filename: None, data: None, read_only: false, create: true, page_size: None, synchronous: true }
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

    /// 结果列标签。
    ///
    /// 引擎在执行时才计算结果头,预编译阶段拿不到,因此如实返回 `None`。
    /// TODO(oak-sql):引擎长出 describe 步骤后返回真实表头,不得偷偷执行来凑。
    pub fn columns(&self) -> Option<Vec<String>> {
        None
    }
}

/// 一个真实连接:引擎句柄、文件归属与提交边界都在这里。
pub struct Connection {
    /// 底层执行引擎,持有 format-3 写者与目录。
    engine: Engine,
    /// 落盘目标;`None` 为纯内存。
    filename: Option<PathBuf>,
    /// 只读标志。
    read_only: bool,
    /// 镜像自上次落盘后是否发生过变化。
    dirty: bool,
}

impl Connection {
    /// 按选项打开连接:读已有文件、从字节镜像打开、创建新文件或纯内存。
    ///
    /// 新建的文件库在打开成功后立即落一份一致镜像,与 SQLite 创建即有文件的
    /// 行为一致;缺失文件以 `not-found` 拒绝。
    pub fn open(options: ConnectionOptions) -> Result<Self, SqlError> {
        let mut created = false;
        let writer = if let Some(pages) = &options.data {
            SqliteWriter::from_pages(pages.clone()).map_err(storage_error)?
        }
        else if let Some(path) = &options.filename {
            if path.exists() {
                SqliteWriter::open(path).map_err(storage_error)?
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
        let mut connection = Self { engine, filename: options.filename, read_only: options.read_only, dirty: false };
        if created {
            connection.publish()?;
        }
        Ok(connection)
    }

    /// 当前数据库镜像,可直接写成 `.sqlite3` 文件。
    pub fn snapshot(&self) -> &[u8] {
        self.engine.snapshot()
    }

    /// 解析一次 SQL,得到可反复执行的预编译语句。
    ///
    /// 恰好一条语句:空输入与多语句分别以 `empty-statement` 和
    /// `multiple-statements` 拒绝,与 [`Engine::execute_one`] 的合同一致。
    pub fn prepare(&self, sql: &str) -> Result<Prepared, SqlError> {
        let statements = parse(sql)?;
        let statement = match statements.len() {
            1 => statements.into_iter().next().expect("length checked"),
            0 => return Err(SqlError::new("empty-statement", "expected exactly one statement but found 0")),
            count => return Err(SqlError::new("multiple-statements", format!("expected exactly one statement but found {count}"))),
        };
        let slots = parameter_slots(&statement);
        let is_write = statement.writes();
        Ok(Prepared { sql: sql.to_string(), statement, slots, is_write })
    }

    /// 执行一条预编译语句,绑定零基位置参数。
    ///
    /// 只读连接在任何会写镜像的语句执行前拒绝;参数少于槽位数时以
    /// `bind-parameter` 拒绝;成功后按提交边界决定是否落盘。
    pub fn run(&mut self, prepared: &Prepared, parameters: &[Cell]) -> Result<ExecResult, SqlError> {
        self.guard_write(prepared.is_write())?;
        let required = prepared.parameter_count();
        if parameters.len() < required {
            return Err(SqlError::new("bind-parameter", format!("statement expects {required} parameters, got {}", parameters.len()))
                .in_statement(prepared.statement_name()));
        }
        let is_write = prepared.is_write();
        let result = self.engine.execute_statement(&prepared.statement, parameters)?;
        self.after_statement(is_write, &prepared.statement)?;
        Ok(result)
    }

    /// 执行一段分号分隔的脚本,逐条语句应用只读拒绝与提交边界。
    pub fn execute(&mut self, sql: &str, parameters: &[Cell]) -> Result<Vec<ExecResult>, SqlError> {
        let statements = parse(sql)?;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            let is_write = statement.writes();
            self.guard_write(is_write)?;
            let required = parameter_slots(&statement).last().map(|(index, _)| index + 1).unwrap_or(0);
            if parameters.len() < required {
                return Err(SqlError::new("bind-parameter", format!("statement expects {required} parameters, got {}", parameters.len()))
                    .in_statement(statement.name()));
            }
            let result = self.engine.execute_statement(&statement, parameters)?;
            self.after_statement(is_write, &statement)?;
            results.push(result);
        }
        Ok(results)
    }

    /// 把当前镜像完整写入 `path`,即真正的备份。
    ///
    /// 只读连接允许备份:它读取镜像、写出到另一个文件,不改变本连接。
    pub fn backup(&mut self, path: &Path) -> Result<(), SqlError> {
        self.engine.save(path)
    }

    /// 关闭连接:已提交但未落盘的改动先落盘;事务未结束时不落盘,
    /// 与 SQLite 在关闭时回滚未提交事务的行为一致。
    pub fn close(&mut self) -> Result<(), SqlError> {
        if self.dirty && !self.engine.in_transaction() {
            self.dirty = false;
            self.publish()?;
        }
        Ok(())
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
        self.engine.set_synchronous(on);
    }

    /// 只读连接拒绝任何会写镜像的语句,在执行前拒绝。
    fn guard_write(&self, is_write: bool) -> Result<(), SqlError> {
        if self.read_only && is_write {
            return Err(SqlError::new("read-only", "cannot modify a read-only database"));
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
    fn publish(&mut self) -> Result<(), SqlError> {
        if let Some(path) = &self.filename {
            self.engine.save(path)?;
        }
        Ok(())
    }
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
    }

    #[test]
    fn prepare_enforces_exactly_one_statement() {
        let connection = Connection::open(ConnectionOptions::default()).expect("in-memory connection");
        assert_eq!(connection.prepare("").expect_err("empty input").code, "empty-statement");
        assert_eq!(connection.prepare("SELECT 1; SELECT 2").expect_err("two statements").code, "multiple-statements");
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
}
