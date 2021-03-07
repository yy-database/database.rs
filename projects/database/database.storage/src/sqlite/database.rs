//! Read-only SQLite main-file snapshots and blank database materialization.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::OnceLock,
};

use database_types::{Error, Result};

use database_formats::sqlite::{
    ENGINE_LIBRARY_VERSION_NUMBER, blank_database, decode_library_version, stamp_library_version, validate_database,
};

use crate::sqlite::{
    schema::{
        SchemaObject, index_rows_from_objects, named_table_from_objects, named_table_range_from_objects, read_schema,
        without_rowid_rows_from_objects,
    },
    table::{TableLimits, TableRow},
};

/// Storage mode for [`SqliteDatabase`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Storage {
    Memory,
    File(PathBuf),
}

/// A SQLite main-file snapshot, not a transactional database engine.
pub struct SqliteDatabase {
    storage: Storage,
    page_size: usize,
    pages: Vec<u8>,
    /// 首次成功解析后的 `sqlite_master` 对象缓存。镜像在 `open` 之后只读
    /// ([`SqliteDatabase::restamp_engine_version`] 只改写 header 的版本字段,
    /// 不触碰 b-tree 区),缓存与字节始终一致。解析失败不缓存,错误仍按
    /// 调用现场返回。缓存解析使用默认限额,调用方传入的 `limits` 只约束
    /// 数据页扫描。
    schema_cache: OnceLock<Vec<SchemaObject>>,
}

impl SqliteDatabase {
    /// Opens an in-memory SQLite database with a blank format-3 page.
    pub fn open_in_memory() -> Result<Self> {
        let pages = blank_database()?;
        let page_size = validate_database(&pages)?;
        Ok(Self { storage: Storage::Memory, page_size, pages, schema_cache: OnceLock::new() })
    }

    /// Reads an existing snapshot or exclusively creates a blank file.
    ///
    /// 与主文件并列的 `-wal` 或热 `-journal` 会在内存中重放后再校验，磁盘上的
    /// 原文件保持不变。
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            let mut bytes = fs::read(&path).map_err(Error::Io)?;
            if let Some(wal) = read_sidecar(&path, "-wal") {
                if let Some(recovered) = crate::sqlite::wal::apply(&bytes, &wal) {
                    bytes = recovered;
                }
            }
            else if let Some(journal) = read_sidecar(&path, "-journal") {
                if let Some(recovered) = crate::sqlite::journal::apply(&bytes, &journal) {
                    bytes = recovered;
                }
            }
            let page_size = validate_database(&bytes)?;
            return Ok(Self { storage: Storage::File(path), page_size, pages: bytes, schema_cache: OnceLock::new() });
        }

        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(Error::Io)?;
            }
        }

        let pages = blank_database()?;
        let page_size = validate_database(&pages)?;
        let mut file = OpenOptions::new().write(true).create_new(true).open(&path).map_err(Error::Io)?;
        file.write_all(&pages).map_err(Error::Io)?;
        file.sync_all().map_err(Error::Io)?;
        Ok(Self { storage: Storage::File(path), page_size, pages, schema_cache: OnceLock::new() })
    }

    /// Returns the configured database path for file-backed databases.
    pub fn path(&self) -> Option<&Path> {
        match &self.storage {
            Storage::Memory => None,
            Storage::File(path) => Some(path),
        }
    }

    /// Returns the page size decoded from the database header.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Returns the raw database bytes owned by this handle.
    pub fn pages(&self) -> &[u8] {
        &self.pages
    }

    /// 首次访问时解析并缓存 `sqlite_master` 对象;后续调用直接命中缓存,
    /// 不再重读页 1、不再重解码每条 CREATE 文本。
    fn cached_schema(&self) -> Result<&[SchemaObject]> {
        if let Some(objects) = self.schema_cache.get() {
            return Ok(objects);
        }
        let objects = read_schema(&self.pages, TableLimits::default())?;
        Ok(self.schema_cache.get_or_init(|| objects))
    }

    /// Reads the main-file schema without parsing or executing SQL definitions.
    /// 结果来自缓存;`limits` 仅保留签名兼容,缓存解析固定使用默认限额。
    pub fn schema(&self, _limits: TableLimits) -> Result<Vec<SchemaObject>> {
        self.cached_schema().map(|objects| objects.to_vec())
    }

    /// Reads raw records from an ordinary rowid table by its stored name.
    pub fn table(&self, name: &str, limits: TableLimits) -> Result<Vec<TableRow>> {
        let objects = self.cached_schema()?;
        named_table_from_objects(&self.pages, objects, name, limits, 0)
    }

    /// 按 rowid 点查一行:沿 B 树下降并剪枝,只物化命中的记录。
    /// 表不存在时返回与 [`SqliteDatabase::table`] 一致的结构化错误;
    /// 视图不支持点查(其行不是存储记录)。限额使用默认值。
    pub fn row_by_rowid(&self, name: &str, rowid: i64) -> Result<Option<TableRow>> {
        let objects = self.cached_schema()?;
        let mut rows = named_table_range_from_objects(&self.pages, objects, name, rowid, rowid, TableLimits::default())?;
        Ok(rows.pop())
    }

    /// 读取 rowid 闭区间 `[lower, upper]` 内的全部记录,按 rowid 升序。
    /// 与 [`SqliteDatabase::table`] 同一套名字解析与损坏检测,但只物化
    /// 区间内的载荷,并按界剪枝下降。
    pub fn table_range(&self, name: &str, lower: i64, upper: i64, limits: TableLimits) -> Result<Vec<TableRow>> {
        let objects = self.cached_schema()?;
        named_table_range_from_objects(&self.pages, objects, name, lower, upper, limits)
    }

    /// Reads raw records from a WITHOUT ROWID table in primary-key order.
    pub fn table_without_rowid(&self, name: &str, limits: TableLimits) -> Result<Vec<Vec<u8>>> {
        let objects = self.cached_schema()?;
        without_rowid_rows_from_objects(&self.pages, objects, name, limits)
    }

    /// Reads raw key payloads from a secondary index in key order.
    pub fn index(&self, name: &str, limits: TableLimits) -> Result<Vec<Vec<u8>>> {
        let objects = self.cached_schema()?;
        index_rows_from_objects(&self.pages, objects, name, limits)
    }

    /// Health probe for disguise tooling and connector smoke tests.
    pub fn ping(&self) -> Result<i32> {
        validate_database(&self.pages)?;
        Ok(1)
    }

    /// Returns the SQLite library version stamped in this snapshot header.
    pub fn sqlite_version(&self) -> Result<String> {
        decode_library_version(&self.pages)
    }

    /// Rejects file writes until a transactional pager is available.
    pub fn flush(&self) -> Result<()> {
        match &self.storage {
            Storage::Memory => Ok(()),
            Storage::File(_) => Err(Error::Unsupported("sqlite snapshot writes require a transactional pager")),
        }
    }

    /// Re-stamps the format-3 library version field with the cluster engine version.
    pub fn restamp_engine_version(&mut self) -> Result<()> {
        validate_database(&self.pages)?;
        stamp_library_version(&mut self.pages, ENGINE_LIBRARY_VERSION_NUMBER);
        Ok(())
    }
}

fn read_sidecar(path: &Path, suffix: &str) -> Option<Vec<u8>> {
    let mut candidate = path.as_os_str().to_os_string();
    candidate.push(suffix);
    fs::read(candidate).ok().filter(|bytes| !bytes.is_empty())
}

/// Reads an existing SQLite file without keeping it open.
pub fn read_existing(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    let path = path.as_ref();
    let mut file = File::open(path).map_err(Error::Io)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(Error::Io)?;
    validate_database(&bytes)?;
    Ok(bytes)
}
