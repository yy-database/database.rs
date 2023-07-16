//! SQLite format-3 writer.
//!
//! Produces byte-exact SQLite main files that upstream SQLite implementations
//! open directly: page allocation, rowid b-tree mutation with splits, index
//! b-trees, freeblocks, the freelist, overflow pages and in-memory
//! transactions. The writer keeps the whole file image in memory and emits it
//! with [`SqliteWriter::save`]; it never speaks SQL.
//!
//! Unsupported on purpose (reported, not faked): WITHOUT ROWID tables,
//! composite PRIMARY KEYs, AUTOINCREMENT, DROP, ALTER, views, triggers, WAL
//! output and UTF-16 database creation.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use database_formats::sqlite::{
    RecordValue, TextEncoding, WriteValue, blank_with_page_size, bump_change_counter, decode_record, decode_varint, encode_record,
    encode_varint_into, read_freelist_count, read_freelist_trunk, read_schema_cookie, set_freelist_count, set_freelist_trunk, set_page_count,
    set_schema_cookie, validate_database,
};
use database_types::{Error, Result};

use crate::sqlite::{
    SchemaObject, TableLimits, TableRow, read_index_table, read_schema, read_schema_with_rowids, read_table, read_table_head, read_table_range,
};

/// 文本比较所用的排序规则。
///
/// 只实现 SQLite 内置的 `BINARY`、`NOCASE`、`RTRIM` 三种：`NOCASE` 仅折叠
/// ASCII 的 A-Z，`RTRIM` 只去掉尾随的 0x20，其余字节逐字节比较，与上游
/// 内置排序规则一致。索引 b-tree 的键序和唯一性判定都按这里的分支执行，
/// facade 的表达式比较复用同一个实现，避免两处语义漂移。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Collation {
    /// 逐字节比较，SQLite 的默认排序规则。
    #[default]
    Binary,
    /// ASCII 大小写不敏感比较。
    Nocase,
    /// 去掉尾随空格后逐字节比较。
    Rtrim,
}

impl Collation {
    /// 按名称解析内置排序规则；未知名称返回 `None`。
    pub fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "binary" => Some(Self::Binary),
            "nocase" => Some(Self::Nocase),
            "rtrim" => Some(Self::Rtrim),
            _ => None,
        }
    }

    /// 排序规则在 SQL 中的名称。
    pub fn name(self) -> &'static str {
        match self {
            Self::Binary => "BINARY",
            Self::Nocase => "NOCASE",
            Self::Rtrim => "RTRIM",
        }
    }

    /// 按本排序规则比较两段文本。
    pub fn compare_text(self, left: &str, right: &str) -> std::cmp::Ordering {
        match self {
            Self::Binary => left.as_bytes().cmp(right.as_bytes()),
            Self::Nocase => nocase_cmp(left.as_bytes(), right.as_bytes()),
            Self::Rtrim => left.trim_end_matches(' ').as_bytes().cmp(right.trim_end_matches(' ').as_bytes()),
        }
    }
}

/// ASCII 大小写折叠后的逐字节比较。
fn nocase_cmp(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let mut index = 0usize;
    loop {
        match (left.get(index), right.get(index)) {
            (Some(left), Some(right)) => {
                let ordering = fold_ascii(*left).cmp(&fold_ascii(*right));
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
        }
        index += 1;
    }
}

fn fold_ascii(byte: u8) -> u8 {
    if byte.is_ascii_uppercase() { byte + 32 } else { byte }
}

const PAGE_INDEX_INTERIOR: u8 = 2;
const PAGE_TABLE_INTERIOR: u8 = 5;
const PAGE_INDEX_LEAF: u8 = 10;
const PAGE_TABLE_LEAF: u8 = 13;

/// One column of a table created by [`SqliteWriter::create_table`].
#[derive(Clone, Debug)]
pub struct ColumnSpec {
    /// Column name as stored in the schema.
    pub name: String,
    /// Declared type text (`INTEGER`, `TEXT`, `REAL`, `BLOB`, ...).
    pub declared_type: String,
    /// Reject explicit NULL inserts.
    pub not_null: bool,
    /// Applied when the insert omits the column.
    pub default_value: Option<WriteValue>,
    /// `DEFAULT` clause exactly as written, without the outer quotes of a
    /// parenthesised expression; `PRAGMA table_info` echoes this text.
    pub default_text: Option<String>,
    /// Single-column PRIMARY KEY.
    pub primary_key: bool,
    /// Single-column UNIQUE constraint.
    pub unique: bool,
    /// `PRIMARY KEY AUTOINCREMENT` on the INTEGER rowid alias.
    pub autoincrement: bool,
    /// Column-level `COLLATE` declaration; `None` means `BINARY`.
    pub collate: Option<Collation>,
}

/// Value slot of an [`SqliteWriter::update`] call.
#[derive(Clone, Debug)]
pub enum UpdateValue {
    /// Keep the stored value.
    Keep,
    /// Replace the stored value.
    Set(WriteValue),
}

/// Result of inserting into a child subtree: the child keeps every key up to
/// the first boundary key, and each boundary page holds the keys above its
/// predecessor up to (and including) its own boundary key.
struct Split {
    boundaries: Vec<(i64, u32)>,
}

/// Greedily packs sorted cells into groups that each fit one page, returning
/// at least two groups when the total exceeds `capacity`.
fn pack_groups<T>(cells: Vec<T>, capacity: usize, size_of: impl Fn(&T) -> usize) -> Vec<Vec<T>> {
    let mut groups: Vec<Vec<T>> = Vec::new();
    let mut space = 0usize;
    for cell in cells {
        let size = size_of(&cell);
        if groups.is_empty() || size > space {
            groups.push(Vec::new());
            space = capacity;
        }
        space = space.saturating_sub(size);
        groups.last_mut().expect("group pushed above").push(cell);
    }
    groups
}

#[derive(Clone)]
struct MasterRow {
    rowid: i64,
    kind: String,
    name: String,
    table_name: String,
    root_page: u32,
    sql: Option<String>,
}

/// Result of [`SqliteWriter::drop_index`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropIndexOutcome {
    /// No index with that name exists.
    Missing,
    /// The name belongs to an index created by `PRIMARY KEY` / `UNIQUE`; SQLite
    /// refuses to drop it.
    Automatic,
    /// The explicit index and its b-tree were removed.
    Dropped,
}

#[derive(Clone)]
struct TableMeta {
    table_name: String,
    master_rowid: i64,
    root_page: u32,
    /// 列定义在解析一次后由所有克隆共享；批写入里每条 insert 都取表元数据，
    /// `Arc` 把列数组的重复分配降为引用计数。需要就地追加的 DDL 路径走
    /// `Arc::make_mut`。
    columns: std::sync::Arc<Vec<ColumnSpec>>,
    rowid_alias: Option<usize>,
    without_rowid: bool,
    /// Record layout of a WITHOUT ROWID table: position in the stored record
    /// to declared column position. SQLite stores the primary-key columns
    /// first and the remaining columns in declaration order, so a key declared
    /// after other columns still leads the record.
    without_rowid_layout: Vec<usize>,
    /// The rowid alias is declared `AUTOINCREMENT`.
    autoincrement: bool,
    primary_key_len: usize,
    /// 索引元数据与 `columns` 同理按 `Arc` 共享，仅 DDL 路径 `make_mut`。
    indexes: std::sync::Arc<Vec<IndexMeta>>,
}

#[derive(Clone)]
struct IndexMeta {
    master_rowid: i64,
    root_page: u32,
    columns: Vec<usize>,
    unique: bool,
    /// The key values are computed by the caller (expression and partial
    /// indexes), so this writer never derives entries from column positions.
    managed: bool,
    /// 每个键项生效的排序规则，来自索引项的显式 `COLLATE` 或索引列声明的
    /// 列级 `COLLATE`。`columns` 为空的管理型索引按索引 SQL 解析后得到。
    collations: Vec<Collation>,
}

struct Snapshot {
    /// Savepoint name, or `None` for a plain `begin` level.
    savepoint: Option<String>,
    /// 本层首次改动某页前捕获的该页完整字节,按页号索引;回滚时逐页还原。
    /// 旧实现每层克隆整镜像,批写入里事务层数乘以镜像大小;脏页日志只
    /// 捕获真正被改的页,未触碰的页零开销。
    dirty: std::collections::HashMap<u32, Vec<u8>>,
    /// 层建立时镜像的字节长度。回滚先截断回此长度,撤销本层
    /// [`SqliteWriter::allocate_page`] 的追加,再还原捕获的页。
    len: usize,
    /// debug 构建下同时保留整镜像,回滚后与脏页还原结果比对,兜底捕获
    /// 漏接 `touch_page` 的写点;release 构建零额外成本。
    #[cfg(debug_assertions)]
    check: Vec<u8>,
    index_entries: std::collections::BTreeMap<i64, std::collections::BTreeSet<Vec<u8>>>,
    without_rowid_entries: std::collections::BTreeMap<i64, std::collections::BTreeSet<Vec<u8>>>,
    without_rowid_keys: std::collections::BTreeMap<i64, std::collections::BTreeSet<Vec<u8>>>,
}

/// 表元数据缓存，以 `sqlite_master` 的 schema cookie 为纪元。
///
/// 每次解析表结构都要重新遍历 `sqlite_master` 并重解 DDL，批写入里这是
/// 主要固定开销。缓存只在 cookie 匹配时生效：任何 DDL 都会抬升 cookie，
/// 于是整表直接失效并在下一次查询时重建。插入引起的根页变化不抬高
/// cookie，由 [`SqliteWriter::set_master_root`] 就地刷新对应条目。
struct MetaCache {
    cookie: u32,
    tables: std::collections::HashMap<String, TableMeta>,
}

/// In-memory SQLite format-3 file image with mutation operations.
///
/// Index trees are maintained as ordered in-memory key sets while the image
/// is being mutated and are materialized into b-tree pages by [`SqliteWriter::save`].
/// Deleting from a b-tree in place would demand full upstream-style
/// rebalancing (merge/redistribute) to keep SQLite's equal-leaf-depth
/// invariant; rebuilding from the sorted sets reuses the proven insert path
/// instead. `pages()` therefore reflects current table content at all times,
/// while index trees are only guaranteed current after `save`.
pub struct SqliteWriter {
    pages: Vec<u8>,
    page_size: usize,
    usable: usize,
    encoding: TextEncoding,
    /// Open transaction levels, outermost first. A plain `begin` pushes an
    /// unnamed level and a `savepoint` pushes a named one, so a statement can
    /// roll itself back without discarding the scope its caller opened.
    snapshots: Vec<Snapshot>,
    /// Live index keys by `sqlite_master` rowid; rebuilt into pages on save.
    index_entries: std::collections::BTreeMap<i64, std::collections::BTreeSet<Vec<u8>>>,
    /// Live WITHOUT ROWID rows by table `sqlite_master` rowid; rebuilt on save.
    without_rowid_entries: std::collections::BTreeMap<i64, std::collections::BTreeSet<Vec<u8>>>,
    /// Primary-key prefixes of the live WITHOUT ROWID rows, for uniqueness.
    without_rowid_keys: std::collections::BTreeMap<i64, std::collections::BTreeSet<Vec<u8>>>,
    /// 按 schema cookie 纪元的表元数据缓存，见 [`MetaCache`]。
    meta_cache: std::sync::Mutex<MetaCache>,
    /// 持久化开关:`save` 写完文件后是否 `sync_all` 落盘。默认关闭,
    /// 语义对齐上游 `PRAGMA synchronous` 的 OFF / FULL 两档。
    synchronous: bool,
}

impl SqliteWriter {
    /// Creates a fresh UTF-8 database image with the given page size.
    pub fn create(page_size: usize) -> Result<Self> {
        Self::create_with_encoding(page_size, TextEncoding::Utf8)
    }

    /// Creates a fresh database image that stores text in `encoding`.
    pub fn create_with_encoding(page_size: usize, encoding: TextEncoding) -> Result<Self> {
        let mut pages = blank_with_page_size(page_size)?;
        let marker: u32 = match encoding {
            TextEncoding::Utf8 => 1,
            TextEncoding::Utf16Le => 2,
            TextEncoding::Utf16Be => 3,
        };
        pages[56..60].copy_from_slice(&marker.to_be_bytes());
        let usable = page_size - usize::from(pages[20]);
        let cookie = read_schema_cookie(&pages);
        Ok(Self {
            pages,
            page_size,
            usable,
            encoding,
            snapshots: Vec::new(),
            index_entries: std::collections::BTreeMap::new(),
            without_rowid_entries: std::collections::BTreeMap::new(),
            without_rowid_keys: std::collections::BTreeMap::new(),
            meta_cache: std::sync::Mutex::new(MetaCache { cookie, tables: std::collections::HashMap::new() }),
            synchronous: false,
        })
    }

    /// Loads an existing SQLite main file for further mutation.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_pages(fs::read(path.as_ref()).map_err(Error::Io)?)
    }

    /// Loads an existing SQLite main-file image from raw bytes, so a host that
    /// already holds the bytes can mutate them without a temporary file.
    pub fn from_pages(pages: Vec<u8>) -> Result<Self> {
        let page_size = validate_database(&pages)?;
        let usable = page_size - usize::from(pages[20]);
        let encoding = match u32::from_be_bytes(pages[56..60].try_into().map_err(|_| Error::Corrupt("sqlite header too short"))?) {
            2 => TextEncoding::Utf16Le,
            3 => TextEncoding::Utf16Be,
            _ => TextEncoding::Utf8,
        };
        let cookie = read_schema_cookie(&pages);
        let mut writer = Self {
            pages,
            page_size,
            usable,
            encoding,
            snapshots: Vec::new(),
            index_entries: std::collections::BTreeMap::new(),
            without_rowid_entries: std::collections::BTreeMap::new(),
            without_rowid_keys: std::collections::BTreeMap::new(),
            meta_cache: std::sync::Mutex::new(MetaCache { cookie, tables: std::collections::HashMap::new() }),
            synchronous: false,
        };
        for row in writer.master_rows()? {
            if row.root_page == 0 {
                continue;
            }
            if row.kind == "index" {
                let entries = writer.read_index_entries(row.root_page)?;
                writer.index_entries.insert(row.rowid, entries);
            }
            else if row.kind == "table" && row.sql.as_deref().is_some_and(is_without_rowid) {
                let sql = row.sql.as_deref().unwrap_or_default();
                let table_keys = find_primary_key_columns(sql)?.unwrap_or_default();
                let width = if table_keys.is_empty() {
                    parse_create_table(sql)?.iter().filter(|column| column.primary_key).count()
                }
                else {
                    table_keys.len()
                };
                if width == 0 {
                    return Err(Error::Corrupt("sqlite WITHOUT ROWID table has no primary key"));
                }
                let entries = writer.read_index_entries(row.root_page)?;
                let mut keys = std::collections::BTreeSet::new();
                for payload in &entries {
                    keys.insert(record_prefix(payload, width, writer.encoding)?);
                }
                writer.without_rowid_entries.insert(row.rowid, entries);
                writer.without_rowid_keys.insert(row.rowid, keys);
            }
        }
        Ok(writer)
    }

    /// Returns the raw file image.
    pub fn pages(&self) -> &[u8] {
        &self.pages
    }

    /// Returns the configured page size.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Reads one stored row by rowid, as its raw record payload.
    ///
    /// `None` means the table has no row with that rowid. `WITHOUT ROWID`
    /// tables have no rowid address space and are rejected.
    pub fn row_payload(&self, table: &str, rowid: i64) -> Result<Option<Vec<u8>>> {
        let meta = self.table_meta(table)?;
        if meta.without_rowid {
            return Err(Error::Unsupported("sqlite WITHOUT ROWID table has no rowid"));
        }
        self.read_payload(meta.root_page, rowid)
    }

    /// Materializes every index tree from its live key set, bumps the change
    /// counter and writes the image to `path`.
    pub fn save(&mut self, path: impl AsRef<Path>) -> Result<()> {
        self.materialize()?;
        self.write_image(path.as_ref())
    }

    /// Writes the image through a sibling temporary file and renames it over
    /// `path`.
    ///
    /// A concurrent reader that opens `path` while this connection publishes
    /// therefore sees either the previous or the new complete image, never a
    /// half-written one. The temporary file is removed when publication fails.
    pub fn save_atomic(&mut self, path: impl AsRef<Path>) -> Result<()> {
        self.materialize()?;
        let path = path.as_ref();
        let mut temporary = path.as_os_str().to_os_string();
        temporary.push(format!(".{}.tmp", std::process::id()));
        let temporary = PathBuf::from(temporary);
        let result = self.write_image(&temporary).and_then(|()| fs::rename(&temporary, path).map_err(Error::Io));
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    /// Rebuilds the materialized trees and bumps the change counter, so every
    /// caller that writes the image shares one publication boundary.
    fn materialize(&mut self) -> Result<()> {
        self.materialize_without_rowid()?;
        self.materialize_indexes()?;
        self.touch_page(1);
        bump_change_counter(&mut self.pages);
        Ok(())
    }

    /// Writes the current image to `path` with the configured durability.
    fn write_image(&self, path: &Path) -> Result<()> {
        if self.synchronous {
            let mut file = fs::File::create(path).map_err(Error::Io)?;
            file.write_all(&self.pages).map_err(Error::Io)?;
            file.sync_all().map_err(Error::Io)?;
        }
        else {
            fs::write(path, &self.pages).map_err(Error::Io)?;
        }
        Ok(())
    }

    /// 设置 `save` 的持久化强度:`true` 时写完文件后 `sync_all` 落盘
    /// (对齐上游 `PRAGMA synchronous=FULL`),`false` 时只交给文件系统缓存
    /// (对齐 `synchronous=OFF`)。默认关闭。
    pub fn set_synchronous(&mut self, on: bool) {
        self.synchronous = on;
    }

    /// 当前 `save` 的持久化强度,与 [`SqliteWriter::set_synchronous`] 成对。
    pub fn synchronous(&self) -> bool {
        self.synchronous
    }

    /// Starts a transaction level. A nested level lets a statement roll itself
    /// back without discarding the scope its caller opened.
    pub fn begin(&mut self) -> Result<()> {
        self.push_level(None);
        Ok(())
    }

    /// Opens a named savepoint. This also starts a transaction when none is
    /// open, matching SQLite's `SAVEPOINT` behaviour.
    pub fn savepoint(&mut self, name: &str) -> Result<()> {
        self.push_level(Some(name.to_string()));
        Ok(())
    }

    /// Commits the innermost open transaction level.
    pub fn commit(&mut self) -> Result<()> {
        let Some(level) = self.snapshots.pop()
        else {
            return Err(Error::Unsupported("sqlite writer has no open transaction"));
        };
        self.promote_dirty(level);
        Ok(())
    }

    /// 内层提交后把其脏页捕获并入外层。外层若尚无某页的捕获,内层捕获的
    /// 字节正是外层开始时的状态,外层回滚仍需还原到它们;外层已有捕获时
    /// 保留外层更旧的版本。
    fn promote_dirty(&mut self, level: Snapshot) {
        let Some(parent) = self.snapshots.last_mut()
        else {
            return;
        };
        for (page, bytes) in level.dirty {
            parent.dirty.entry(page).or_insert(bytes);
        }
    }

    /// Rolls the image back to the state before the innermost `begin`.
    pub fn rollback(&mut self) -> Result<()> {
        if self.snapshots.is_empty() {
            return Err(Error::Unsupported("sqlite writer has no open transaction"));
        }
        let index = self.snapshots.len() - 1;
        self.restore_level(index);
        self.snapshots.pop();
        Ok(())
    }

    /// Commits every open level, ending the whole transaction.
    pub fn commit_all(&mut self) -> Result<()> {
        if self.snapshots.is_empty() {
            return Err(Error::Unsupported("sqlite writer has no open transaction"));
        }
        self.snapshots.clear();
        Ok(())
    }

    /// Rolls the image back to the state before the outermost level, ending the
    /// whole transaction.
    pub fn rollback_all(&mut self) -> Result<()> {
        if self.snapshots.is_empty() {
            return Err(Error::Unsupported("sqlite writer has no open transaction"));
        }
        self.restore_level(0);
        self.snapshots.clear();
        Ok(())
    }

    /// Releases a savepoint together with every savepoint opened after it.
    /// Releasing the outermost savepoint also commits the open transaction.
    pub fn release(&mut self, name: &str) -> Result<()> {
        let index = self.find_savepoint(name)?;
        let popped: Vec<Snapshot> = self.snapshots.drain(index..).collect();
        // 由外向内并入捕获:先插入外层(更旧)的捕获,内层同名页不覆盖它。
        for level in popped {
            self.promote_dirty(level);
        }
        Ok(())
    }

    /// Rolls the image back to a savepoint and keeps it open, so the caller can
    /// continue in the same savepoint scope.
    pub fn rollback_to(&mut self, name: &str) -> Result<()> {
        let index = self.find_savepoint(name)?;
        self.restore_level(index);
        self.snapshots.truncate(index + 1);
        Ok(())
    }

    fn push_level(&mut self, savepoint: Option<String>) {
        self.snapshots.push(Snapshot {
            savepoint,
            dirty: std::collections::HashMap::new(),
            len: self.pages.len(),
            #[cfg(debug_assertions)]
            check: self.pages.clone(),
            index_entries: self.index_entries.clone(),
            without_rowid_entries: self.without_rowid_entries.clone(),
            without_rowid_keys: self.without_rowid_keys.clone(),
        });
    }

    /// 在首次改动 `page` 之前捕获其当前字节。无打开的事务、本层已捕获过
    /// 该页、或页超出当前镜像长度(本层新分配,回滚按 `len` 截断即可)时
    /// 不做任何事。所有页写入函数都必须在写前调用本方法。
    fn touch_page(&mut self, page: u32) {
        if self.snapshots.is_empty() {
            return;
        }
        let offset = (page as usize - 1) * self.page_size;
        let Some(end) = offset.checked_add(self.page_size)
        else {
            return;
        };
        if end > self.pages.len() {
            return;
        }
        let level = self.snapshots.last_mut().expect("checked non-empty");
        if level.dirty.contains_key(&page) {
            return;
        }
        level.dirty.insert(page, self.pages[offset..end].to_vec());
    }

    fn restore_level(&mut self, index: usize) {
        let snapshot = &self.snapshots[index];
        self.pages.truncate(snapshot.len);
        for (page, bytes) in &snapshot.dirty {
            let offset = (*page as usize - 1) * self.page_size;
            if offset + self.page_size <= self.pages.len() {
                self.pages[offset..offset + self.page_size].copy_from_slice(bytes);
            }
        }
        self.index_entries = snapshot.index_entries.clone();
        self.without_rowid_entries = snapshot.without_rowid_entries.clone();
        self.without_rowid_keys = snapshot.without_rowid_keys.clone();
        #[cfg(debug_assertions)]
        if self.pages != snapshot.check {
            let page = (0..std::cmp::max(self.pages.len(), snapshot.check.len()))
                .find(|byte| self.pages.get(*byte) != snapshot.check.get(*byte))
                .map_or(0, |byte| (byte / self.page_size) as u32 + 1);
            panic!("脏页日志漏捕获了页写入:首个差异在第 {page} 页");
        }
    }

    /// Finds the innermost savepoint with this name. Names are case insensitive,
    /// as in SQLite.
    fn find_savepoint(&self, name: &str) -> Result<usize> {
        self.snapshots
            .iter()
            .rposition(|level| level.savepoint.as_deref().is_some_and(|saved| saved.eq_ignore_ascii_case(name)))
            .ok_or(Error::Unsupported("sqlite savepoint does not exist"))
    }

    /// Reads the schema objects through the crate's own reader.
    pub fn schema_objects(&self, limits: TableLimits) -> Result<Vec<SchemaObject>> {
        read_schema(&self.pages, limits)
    }

    /// Reads the schema objects together with their `sqlite_master` rowids.
    pub fn schema_objects_with_rowids(&self, limits: TableLimits) -> Result<Vec<(i64, SchemaObject)>> {
        read_schema_with_rowids(&self.pages, limits)
    }

    /// Reads one table's rows through the crate's own reader.
    pub fn table_rows(&self, table: &str, limits: TableLimits) -> Result<Vec<TableRow>> {
        let meta = self.table_meta(table)?;
        read_table(&self.pages, meta.root_page, limits)
    }

    /// Reads one table's rows whose rowid falls in the closed range
    /// `[lower, upper]`, ascending, through the crate's own bounded reader.
    /// Mirrors [`SqliteWriter::table_rows`] (same name resolution and damage
    /// checks) but only materializes the in-range payloads, so a
    /// `WHERE <rowid alias> = <literal>` point read never decodes the whole
    /// table. An empty range yields an empty result.
    pub fn table_range(&self, table: &str, lower: i64, upper: i64, limits: TableLimits) -> Result<Vec<TableRow>> {
        let meta = self.table_meta(table)?;
        read_table_range(&self.pages, meta.root_page, lower, upper, limits)
    }

    /// Reads one table's first `max_rows` rows in ascending rowid order through
    /// the crate's bounded reader. Mirrors [`SqliteWriter::table_rows`] (same
    /// name resolution and damage checks) but stops the b-tree walk as soon as
    /// the bound is satisfied, so `ORDER BY <rowid order> LIMIT n` never
    /// materializes the tail of the table.
    pub fn table_head(&self, table: &str, max_rows: usize, limits: TableLimits) -> Result<Vec<TableRow>> {
        let meta = self.table_meta(table)?;
        read_table_head(&self.pages, meta.root_page, max_rows, limits)
    }

    /// Reads one index's live key set, decoded into `(key values, rowid)` pairs
    /// in the index's own column order. The key set is the writer's in-memory
    /// source of truth while the image is being mutated, so this reflects every
    /// row written since the file was opened, not just the materialized pages.
    /// Returns an error when no index with that name exists.
    pub fn index_entries_decoded(&self, index: &str) -> Result<Vec<(Vec<WriteValue>, i64)>> {
        let row = self
            .master_rows()?
            .into_iter()
            .find(|row| row.kind == "index" && eq_ignore_ascii_case(&row.name, index))
            .ok_or(Error::Unsupported("sqlite index does not exist"))?;
        let Some(entries) = self.index_entries.get(&row.rowid) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::with_capacity(entries.len());
        for payload in entries {
            let values = decode_record(payload, self.encoding, Default::default())
                .map_err(|_| Error::Corrupt("sqlite index record decode failed"))?;
            let Some(last) = values.last() else {
                continue;
            };
            let database_formats::sqlite::RecordValue::Integer(rowid) = last else {
                return Err(Error::Corrupt("sqlite index record has no rowid column"));
            };
            let key = values[..values.len() - 1]
                .iter()
                .map(|value| match value {
                    database_formats::sqlite::RecordValue::Null => WriteValue::Null,
                    database_formats::sqlite::RecordValue::Integer(number) => WriteValue::Integer(*number),
                    database_formats::sqlite::RecordValue::Real(real) => WriteValue::Real(*real),
                    database_formats::sqlite::RecordValue::Text(text) => WriteValue::Text(text.clone()),
                    database_formats::sqlite::RecordValue::Blob(blob) => WriteValue::Blob(blob.to_vec()),
                })
                .collect();
            out.push((key, *rowid));
        }
        Ok(out)
    }

    /// Reads one WITHOUT ROWID table's records in primary-key order.
    pub fn without_rowid_rows(&self, table: &str, limits: TableLimits) -> Result<Vec<Vec<u8>>> {
        let meta = self.table_meta(table)?;
        if !meta.without_rowid {
            return Err(Error::Unsupported("sqlite table is not WITHOUT ROWID"));
        }
        read_index_table(&self.pages, meta.root_page, limits)
    }

    // ------------------------------------------------------------------
    // Schema objects
    // ------------------------------------------------------------------

    /// Creates a table with the given columns.
    pub fn create_table(&mut self, name: &str, columns: &[ColumnSpec]) -> Result<()> {
        self.create_table_with_primary_key(name, columns, &[])
    }

    /// Creates a table, optionally with a table-level `PRIMARY KEY (a, b)`.
    ///
    /// SQLite keeps such a table as a rowid table: the key becomes a
    /// `sqlite_autoindex_<table>_<n>` unique index, and only a single declared
    /// `INTEGER` key column doubles as the rowid alias. `WITHOUT ROWID` tables
    /// are created separately by [`SqliteWriter::create_table_without_rowid`].
    pub fn create_table_with_primary_key(&mut self, name: &str, columns: &[ColumnSpec], primary_key: &[String]) -> Result<()> {
        self.create_table_with_primary_key_sql(name, columns, primary_key, None)
    }

    /// Same as [`SqliteWriter::create_table_with_primary_key`], but stores
    /// `sql` verbatim as the `sqlite_master.sql` text. SQLite keeps the
    /// `CREATE TABLE` statement as the user wrote it, so the SQL surface passes
    /// the original statement through; the rendered canonical text is used only
    /// when no original text exists.
    pub fn create_table_with_primary_key_sql(
        &mut self,
        name: &str,
        columns: &[ColumnSpec],
        primary_key: &[String],
        sql: Option<&str>,
    ) -> Result<()> {
        if name.is_empty() {
            return Err(Error::Unsupported("sqlite table name must not be empty"));
        }
        if columns.is_empty() {
            return Err(Error::Unsupported("sqlite table needs at least one column"));
        }
        for object in self.master_rows()? {
            if eq_ignore_ascii_case(&object.name, name) {
                return Err(Error::Unsupported("sqlite schema object already exists"));
            }
        }
        let mut seen = Vec::new();
        for column in columns {
            if column.name.is_empty() {
                return Err(Error::Unsupported("sqlite column name must not be empty"));
            }
            if seen.iter().any(|existing: &String| eq_ignore_ascii_case(existing, &column.name)) {
                return Err(Error::Unsupported("sqlite duplicate column name"));
            }
            seen.push(column.name.clone());
        }
        let inline_pk_count = columns.iter().filter(|column| column.primary_key).count();
        if inline_pk_count > 1 || (inline_pk_count == 1 && !primary_key.is_empty()) {
            return Err(Error::Unsupported("sqlite table has more than one primary key"));
        }
        let mut primary_key_positions: Vec<usize> = Vec::with_capacity(primary_key.len());
        for key in primary_key {
            let position = columns
                .iter()
                .position(|column| eq_ignore_ascii_case(&column.name, key))
                .ok_or(Error::Unsupported("sqlite primary key references unknown column"))?;
            if primary_key_positions.contains(&position) {
                return Err(Error::Unsupported("sqlite primary key repeats a column"));
            }
            primary_key_positions.push(position);
        }
        if inline_pk_count == 1 {
            primary_key_positions = columns.iter().enumerate().filter(|(_, column)| column.primary_key).map(|(index, _)| index).collect();
        }
        // Only a lone declared INTEGER key is the rowid itself; every other
        // primary key shape becomes a automatic unique index.
        let rowid_alias = match primary_key_positions.as_slice() {
            [position] if columns[*position].declared_type.eq_ignore_ascii_case("INTEGER") => Some(*position),
            _ => None,
        };
        // `AUTOINCREMENT` is only meaningful on the INTEGER rowid alias; SQLite
        // rejects every other shape with the same message.
        if columns.iter().any(|column| column.autoincrement) {
            let position = columns.iter().position(|column| column.autoincrement).expect("checked above");
            if Some(position) != rowid_alias {
                return Err(Error::Unsupported("sqlite AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY"));
            }
        }

        let root_page = self.allocate_page()?;
        self.init_leaf_page(root_page, PAGE_TABLE_LEAF);
        let rendered = render_create_table(name, columns, primary_key);
        let sql = sql.unwrap_or(&rendered);
        let master_rowid = self.insert_master_row("table", name, name, root_page, Some(sql))?;

        let mut meta = TableMeta {
            table_name: name.to_string(),
            master_rowid,
            root_page,
            columns: std::sync::Arc::new(columns.to_vec()),
            rowid_alias,
            without_rowid: false,
            without_rowid_layout: Vec::new(),
            autoincrement: columns.iter().any(|column| column.autoincrement),
            primary_key_len: 0,
            indexes: std::sync::Arc::new(Vec::new()),
        };
        let mut auto_number = 0usize;
        for (index, column) in columns.iter().enumerate() {
            // A rowid alias is the table rowid itself; SQLite keeps no
            // autoindex for it.
            if (column.primary_key || column.unique) && Some(index) != rowid_alias {
                auto_number += 1;
                let auto_name = format!("sqlite_autoindex_{name}_{auto_number}");
                self.create_index_inner(&auto_name, &mut meta, vec![index], true, true, None)?;
            }
        }
        // Table-level constraints follow the inline ones in written order. When
        // the original text is available it decides the numbering, so a
        // `UNIQUE` written before a table-level key owns the lower number.
        let constraints = parse_table_constraints(sql, columns)?;
        for (primary, positions) in constraints {
            if primary && rowid_alias.is_some() {
                continue;
            }
            auto_number += 1;
            let auto_name = format!("sqlite_autoindex_{name}_{auto_number}");
            self.create_index_inner(&auto_name, &mut meta, positions, true, true, None)?;
        }
        // SQLite creates `sqlite_sequence` as the last step of a CREATE TABLE
        // that declares AUTOINCREMENT, after the automatic constraint indexes.
        if columns.iter().any(|column| column.autoincrement) {
            self.ensure_sequence_table()?;
        }
        self.bump_cookie();
        Ok(())
    }

    /// Creates the reserved `sqlite_sequence` table if a previous
    /// AUTOINCREMENT table did not already create it.
    fn ensure_sequence_table(&mut self) -> Result<()> {
        if self.master_rows()?.iter().any(|row| row.kind == "table" && eq_ignore_ascii_case(&row.name, "sqlite_sequence")) {
            return Ok(());
        }
        let root_page = self.allocate_page()?;
        self.init_leaf_page(root_page, PAGE_TABLE_LEAF);
        self.insert_master_row("table", "sqlite_sequence", "sqlite_sequence", root_page, Some("CREATE TABLE sqlite_sequence(name,seq)"))?;
        Ok(())
    }

    /// Reads the AUTOINCREMENT high-water mark stored in `sqlite_sequence`.
    fn sequence_value(&self, table: &str) -> Result<Option<i64>> {
        let Some(sequence) =
            self.master_rows()?.into_iter().find(|row| row.kind == "table" && eq_ignore_ascii_case(&row.name, "sqlite_sequence"))
        else {
            return Ok(None);
        };
        for row in read_table(&self.pages, sequence.root_page, TableLimits::default())? {
            let values = decode_record(&row.payload, self.encoding, Default::default())
                .map_err(|_| Error::Corrupt("sqlite sqlite_sequence record decode failed"))?;
            let matches_table = matches!(&values[0], database_formats::sqlite::RecordValue::Text(name) if name == table);
            if !matches_table {
                continue;
            }
            if let database_formats::sqlite::RecordValue::Integer(seq) = &values[1] {
                return Ok(Some(*seq));
            }
        }
        Ok(None)
    }

    /// Stores the AUTOINCREMENT high-water mark for one table.
    fn set_sequence_value(&mut self, table: &str, seq: i64) -> Result<()> {
        let meta = self.table_meta("sqlite_sequence")?;
        let record = encode_record(&[WriteValue::Text(table.to_string()), WriteValue::Integer(seq)], self.encoding);
        let existing = read_table(&self.pages, meta.root_page, TableLimits::default())?
            .into_iter()
            .find(|row| {
                decode_record(&row.payload, self.encoding, Default::default())
                    .is_ok_and(|values| matches!(&values[0], database_formats::sqlite::RecordValue::Text(name) if name == table))
            })
            .map(|row| row.rowid);
        match existing {
            Some(rowid) => {
                let root = self.table_replace(meta.root_page, rowid, &record)?;
                if root != meta.root_page {
                    self.set_master_root(meta.master_rowid, root)?;
                }
            }
            None => {
                let rowid = self.max_rowid(meta.root_page)?.saturating_add(1);
                let root = self.table_insert(meta.root_page, rowid, &record)?;
                if root != meta.root_page {
                    self.set_master_root(meta.master_rowid, root)?;
                }
            }
        }
        Ok(())
    }

    /// Creates a WITHOUT ROWID table. Rows are stored in the primary-key
    /// index b-tree rooted at the schema `rootpage`, which is what external
    /// readers walk. SQLite stores the key columns first in each record, so a
    /// key declared after other columns still leads the stored record.
    pub fn create_table_without_rowid(&mut self, name: &str, columns: &[ColumnSpec], primary_key: &[String]) -> Result<()> {
        self.create_table_without_rowid_sql(name, columns, primary_key, None)
    }

    /// Same as [`SqliteWriter::create_table_without_rowid`], but stores `sql`
    /// verbatim as the `sqlite_master.sql` text, matching the statement as
    /// written.
    pub fn create_table_without_rowid_sql(
        &mut self,
        name: &str,
        columns: &[ColumnSpec],
        primary_key: &[String],
        sql: Option<&str>,
    ) -> Result<()> {
        if primary_key.is_empty() {
            return Err(Error::Unsupported("sqlite WITHOUT ROWID table needs a PRIMARY KEY"));
        }
        if columns.len() < primary_key.len() {
            return Err(Error::Unsupported("sqlite WITHOUT ROWID primary key exceeds the column list"));
        }
        for (position, key) in primary_key.iter().enumerate() {
            if !columns.iter().any(|column| column.name.eq_ignore_ascii_case(key)) {
                return Err(Error::Unsupported("sqlite WITHOUT ROWID primary key references unknown column"));
            }
            if primary_key[..position].iter().any(|existing| existing.eq_ignore_ascii_case(key)) {
                return Err(Error::Unsupported("sqlite WITHOUT ROWID primary key repeats a column"));
            }
        }
        if columns.iter().filter(|column| column.primary_key).count() > 1 {
            return Err(Error::Unsupported("sqlite table has more than one primary key"));
        }
        let table_unique = match sql {
            Some(text) => parse_table_constraints(text, columns)?.iter().any(|(primary, _)| !primary),
            None => false,
        };
        if columns.iter().any(|column| column.unique) || table_unique {
            return Err(Error::Unsupported("sqlite UNIQUE constraints on WITHOUT ROWID tables are not supported"));
        }
        if columns.iter().any(|column| column.autoincrement) {
            return Err(Error::Unsupported("sqlite AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY"));
        }
        for object in self.master_rows()? {
            if eq_ignore_ascii_case(&object.name, name) {
                return Err(Error::Unsupported("sqlite schema object already exists"));
            }
        }

        let root_page = self.allocate_page()?;
        self.init_leaf_page(root_page, PAGE_INDEX_LEAF);
        let rendered = render_create_table_without_rowid(name, columns, primary_key);
        let sql = sql.unwrap_or(&rendered);
        let master_rowid = self.insert_master_row("table", name, name, root_page, Some(sql))?;
        self.without_rowid_entries.insert(master_rowid, std::collections::BTreeSet::new());
        self.without_rowid_keys.insert(master_rowid, std::collections::BTreeSet::new());
        self.bump_cookie();
        Ok(())
    }

    /// Inserts one row into a WITHOUT ROWID table and returns the primary-key
    /// insertion order position, which carries no rowid meaning.
    fn insert_without_rowid(&mut self, meta: &TableMeta, values: &[Option<WriteValue>]) -> Result<i64> {
        let mut resolved: Vec<WriteValue> = Vec::with_capacity(values.len());
        for (index, slot) in values.iter().enumerate() {
            let column = &meta.columns[index];
            let value = match slot {
                Some(value) => apply_affinity(value.clone(), affinity_of(&column.declared_type)),
                None => column.default_value.clone().unwrap_or(WriteValue::Null),
            };
            if matches!(value, WriteValue::Null) && column.not_null {
                return Err(Error::Constraint { kind: "not-null", message: "sqlite NOT NULL constraint failed" });
            }
            resolved.push(value);
        }
        if meta.primary_key_len == 0 || meta.without_rowid_layout.len() != resolved.len() {
            return Err(Error::Corrupt("sqlite WITHOUT ROWID record layout is incomplete"));
        }
        let mut layout_values: Vec<WriteValue> = Vec::with_capacity(resolved.len());
        for position in &meta.without_rowid_layout {
            layout_values.push(resolved[*position].clone());
        }
        let payload = encode_record(&layout_values, self.encoding);
        let key = encode_record(&layout_values[..meta.primary_key_len], self.encoding);
        let unique = self.without_rowid_keys.entry(meta.master_rowid).or_default().insert(key);
        if !unique {
            return Err(Error::Constraint { kind: "unique", message: "sqlite UNIQUE constraint failed" });
        }
        self.without_rowid_entries.entry(meta.master_rowid).or_default().insert(payload);
        Ok(0)
    }

    /// Deletes one WITHOUT ROWID row identified by its declared column values.
    /// Returns whether the row was present.
    pub fn delete_without_rowid(&mut self, table: &str, values: &[WriteValue]) -> Result<bool> {
        let meta = self.table_meta(table)?;
        if !meta.without_rowid {
            return Err(Error::Unsupported("sqlite table is not WITHOUT ROWID"));
        }
        let (key, payload) = without_rowid_key_payload(&meta, values, self.encoding)?;
        let entries = self.without_rowid_entries.entry(meta.master_rowid).or_default();
        let removed = if entries.remove(&payload) {
            true
        }
        else {
            // A record written before `ALTER TABLE ADD COLUMN` is short; match
            // it by its primary-key prefix instead of its full encoding.
            let found = entries
                .iter()
                .find(|entry| record_prefix(entry, meta.primary_key_len, self.encoding).is_ok_and(|prefix| prefix == key))
                .cloned();
            found.is_some_and(|entry| entries.remove(&entry))
        };
        if removed {
            self.without_rowid_keys.entry(meta.master_rowid).or_default().remove(&key);
        }
        Ok(removed)
    }

    /// Replaces one WITHOUT ROWID row, identified by its declared column
    /// values, with another full row. Returns whether the old row existed.
    pub fn update_without_rowid(&mut self, table: &str, old_values: &[WriteValue], new_values: &[WriteValue]) -> Result<bool> {
        let meta = self.table_meta(table)?;
        if !meta.without_rowid {
            return Err(Error::Unsupported("sqlite table is not WITHOUT ROWID"));
        }
        if new_values.len() != meta.columns.len() {
            return Err(Error::Unsupported("sqlite update arity mismatch"));
        }
        let (old_key, old_payload) = without_rowid_key_payload(&meta, old_values, self.encoding)?;
        let (new_key, new_payload) = without_rowid_key_payload(&meta, new_values, self.encoding)?;
        let entries = self.without_rowid_entries.entry(meta.master_rowid).or_default();
        let found = if entries.remove(&old_payload) {
            true
        }
        else {
            let matched = entries
                .iter()
                .find(|entry| record_prefix(entry, meta.primary_key_len, self.encoding).is_ok_and(|prefix| prefix == old_key))
                .cloned();
            matched.is_some_and(|entry| entries.remove(&entry))
        };
        if !found {
            return Ok(false);
        }
        self.without_rowid_keys.entry(meta.master_rowid).or_default().remove(&old_key);
        if self.without_rowid_keys.entry(meta.master_rowid).or_default().contains(&new_key) {
            return Err(Error::Constraint { kind: "unique", message: "sqlite UNIQUE constraint failed" });
        }
        self.without_rowid_keys.entry(meta.master_rowid).or_default().insert(new_key);
        entries.insert(new_payload);
        Ok(true)
    }

    /// Creates an index over one or more columns of an existing table.
    pub fn create_index(&mut self, name: &str, table: &str, columns: &[String], unique: bool) -> Result<()> {
        self.create_index_with_sql(name, table, columns, unique, None)
    }

    /// Same as [`SqliteWriter::create_index`], but stores `sql` verbatim as the
    /// `sqlite_master.sql` text, matching the statement SQLite records.
    pub fn create_index_with_sql(&mut self, name: &str, table: &str, columns: &[String], unique: bool, sql: Option<&str>) -> Result<()> {
        if name.is_empty() || columns.is_empty() {
            return Err(Error::Unsupported("sqlite index needs a name and columns"));
        }
        if let Some(sql) = sql {
            if index_sql_is_managed(sql)? {
                return Err(Error::Unsupported("sqlite expression and partial indexes need caller-computed keys"));
            }
        }
        for object in self.master_rows()? {
            if eq_ignore_ascii_case(&object.name, name) {
                return Err(Error::Unsupported("sqlite schema object already exists"));
            }
        }
        let mut meta = self.table_meta(table)?;
        if meta.without_rowid {
            return Err(Error::Unsupported("sqlite index on a WITHOUT ROWID table is not supported"));
        }
        let mut positions = Vec::with_capacity(columns.len());
        for column in columns {
            let position = meta
                .columns
                .iter()
                .position(|existing| eq_ignore_ascii_case(&existing.name, column))
                .ok_or(Error::Unsupported("sqlite index column does not exist"))?;
            positions.push(position);
        }
        self.create_index_inner(name, &mut meta, positions, unique, false, sql)?;
        self.bump_cookie();
        Ok(())
    }

    fn create_index_inner(
        &mut self,
        name: &str,
        meta: &mut TableMeta,
        columns: Vec<usize>,
        unique: bool,
        automatic: bool,
        sql: Option<&str>,
    ) -> Result<()> {
        let root_page = self.allocate_page()?;
        self.init_leaf_page(root_page, PAGE_INDEX_LEAF);
        // Automatic constraint indexes inherit the collation of their key
        // columns; an explicit index takes it from its own `CREATE` text and
        // falls back to the column declaration for bare keys.
        let collations = if automatic {
            columns.iter().map(|position| meta.columns[*position].collate.unwrap_or_default()).collect()
        }
        else {
            let column_names: Vec<String> = columns.iter().map(|index| meta.columns[*index].name.clone()).collect();
            let rendered = render_create_index(name, &table_name_of(meta), &column_names, unique);
            index_collations(sql.unwrap_or(&rendered), &meta.columns)?
        };
        // Automatic constraint indexes carry no CREATE text in sqlite_master;
        // upstream SQLite re-derives them from the table definition and would
        // reject a duplicate if the text were present.
        let master_rowid = if automatic {
            self.insert_master_row("index", name, &table_name_of(meta), root_page, None)?
        }
        else {
            let column_names: Vec<String> = columns.iter().map(|index| meta.columns[*index].name.clone()).collect();
            let rendered = render_create_index(name, &table_name_of(meta), &column_names, unique);
            let sql = sql.unwrap_or(&rendered);
            self.insert_master_row("index", name, &table_name_of(meta), root_page, Some(sql))?
        };
        let mut index = IndexMeta { master_rowid, root_page, columns, unique, managed: false, collations };

        // Backfill existing rows into the live key set, then materialize the
        // fresh root. Splits may regrow the root; the master row must follow.
        let rows = read_table(&self.pages, meta.root_page, TableLimits::default())?;
        let mut entries = std::collections::BTreeSet::new();
        for row in rows {
            let values =
                decode_record(&row.payload, self.encoding, Default::default()).map_err(|_| Error::Corrupt("sqlite record decode failed"))?;
            let owned = expand_alias(meta, row.rowid, &values);
            entries.insert(encode_index_key(meta, &index, row.rowid, &owned, self.encoding)?);
        }
        let initial_root = root_page;
        for key in &entries {
            index.root_page = self.index_insert(index.root_page, key, &index.collations)?;
        }
        if index.root_page != initial_root {
            self.set_master_root(master_rowid, index.root_page)?;
        }
        self.index_entries.insert(master_rowid, entries);
        std::sync::Arc::make_mut(&mut meta.indexes).push(index);
        Ok(())
    }

    /// Creates an index whose key values the caller derives from row content
    /// (expression and partial indexes). `entries` holds one `(key values,
    /// rowid)` pair per indexed row and must already be filtered by the index
    /// predicate.
    pub fn create_managed_index(&mut self, name: &str, table: &str, unique: bool, sql: &str, entries: &[(Vec<WriteValue>, i64)]) -> Result<()> {
        let meta = self.table_meta(table)?;
        if meta.without_rowid {
            return Err(Error::Unsupported("sqlite indexes on WITHOUT ROWID tables are not supported"));
        }
        for object in self.master_rows()? {
            if eq_ignore_ascii_case(&object.name, name) {
                return Err(Error::Unsupported("sqlite schema object already exists"));
            }
        }
        let root_page = self.allocate_page()?;
        self.init_leaf_page(root_page, PAGE_INDEX_LEAF);
        let collations = index_collations(sql, &meta.columns)?;
        let master_rowid = self.insert_master_row("index", name, table, root_page, Some(sql))?;
        if unique {
            let mut signatures = std::collections::BTreeSet::new();
            for (values, _) in entries {
                if !signatures.insert(encode_record(&normalized_index_values(values, &collations), self.encoding)) {
                    return Err(Error::Constraint { kind: "unique", message: "sqlite UNIQUE constraint failed" });
                }
            }
        }
        let keys: std::collections::BTreeSet<Vec<u8>> =
            entries.iter().map(|(values, rowid)| managed_index_key(values, *rowid, self.encoding)).collect();
        let mut index = IndexMeta { master_rowid, root_page, columns: Vec::new(), unique, managed: true, collations };
        for key in &keys {
            index.root_page = self.index_insert(index.root_page, key, &index.collations)?;
        }
        if index.root_page != root_page {
            self.set_master_root(master_rowid, index.root_page)?;
        }
        self.index_entries.insert(master_rowid, keys);
        Ok(())
    }

    /// Replaces the key set of a managed index. The caller materializes the
    /// b-tree afterwards, like any other index mutation.
    pub fn replace_managed_index_entries(&mut self, index_name: &str, entries: &[(Vec<WriteValue>, i64)]) -> Result<()> {
        let row = self
            .master_rows()?
            .into_iter()
            .find(|row| row.kind == "index" && eq_ignore_ascii_case(&row.name, index_name))
            .ok_or(Error::Unsupported("sqlite index does not exist"))?;
        let sql = row.sql.as_deref().ok_or(Error::Unsupported("sqlite automatic indexes are not managed"))?;
        if !index_sql_is_managed(sql)? {
            return Err(Error::Unsupported("sqlite index is not managed"));
        }
        let meta = self.table_meta(&row.table_name)?;
        let collations = index_collations(sql, &meta.columns)?;
        if sql.to_ascii_uppercase().starts_with("CREATE UNIQUE INDEX") {
            let mut signatures = std::collections::BTreeSet::new();
            for (values, _) in entries {
                if !signatures.insert(encode_record(&normalized_index_values(values, &collations), self.encoding)) {
                    return Err(Error::Constraint { kind: "unique", message: "sqlite UNIQUE constraint failed" });
                }
            }
        }
        let keys: std::collections::BTreeSet<Vec<u8>> =
            entries.iter().map(|(values, rowid)| managed_index_key(values, *rowid, self.encoding)).collect();
        self.index_entries.insert(row.rowid, keys);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Row mutation
    // ------------------------------------------------------------------

    /// Inserts one row; `None` slots apply the column default or NULL.
    /// Returns the rowid of the new row.
    pub fn insert(&mut self, table: &str, values: &[Option<WriteValue>]) -> Result<i64> {
        let mut meta = self.table_meta(table)?;
        if values.len() != meta.columns.len() {
            return Err(Error::Unsupported("sqlite insert arity mismatch"));
        }
        if meta.without_rowid {
            return self.insert_without_rowid(&meta, values);
        }
        let mut resolved: Vec<WriteValue> = Vec::with_capacity(values.len());
        for (index, slot) in values.iter().enumerate() {
            let column = &meta.columns[index];
            let value = match slot {
                Some(value) => value.clone(),
                None => column.default_value.clone().unwrap_or(WriteValue::Null),
            };
            let value = apply_affinity(value, affinity_of(&column.declared_type));
            if matches!(value, WriteValue::Null) && column.not_null && Some(index) != meta.rowid_alias {
                return Err(Error::Constraint { kind: "not-null", message: "sqlite NOT NULL constraint failed" });
            }
            resolved.push(value);
        }

        let alias_index = meta.rowid_alias;
        let alias_value = alias_index.and_then(|index| match &resolved[index] {
            WriteValue::Integer(rowid) => Some(*rowid),
            _ => None,
        });
        let explicit_rowid = match alias_value {
            Some(rowid) => Some(rowid),
            None => {
                if let Some(index) = alias_index {
                    if !matches!(resolved[index], WriteValue::Null) {
                        return Err(Error::Unsupported("sqlite rowid alias must be an integer or NULL"));
                    }
                }
                None
            }
        };

        let rowid = match explicit_rowid {
            Some(rowid) => rowid,
            None => {
                let candidate = self.max_rowid(meta.root_page)?.saturating_add(1);
                if meta.autoincrement {
                    // AUTOINCREMENT continues from the high-water mark even
                    // after every row was deleted.
                    let seq = self.sequence_value(&meta.table_name)?.unwrap_or(0).saturating_add(1);
                    candidate.max(seq)
                }
                else {
                    candidate
                }
            }
        };
        if explicit_rowid.is_none() && rowid == i64::MIN {
            return Err(Error::Unsupported("sqlite rowid space exhausted"));
        }

        if let Some(index) = alias_index {
            resolved[index] = WriteValue::Null;
        }
        for index_meta in meta.indexes.iter() {
            if index_meta.managed {
                continue;
            }
            if index_meta.unique && index_key_has_no_null(&resolved, &index_meta.columns) {
                let key = encode_index_key(&meta, index_meta, rowid, &resolved, self.encoding)?;
                if self.set_prefix_match(index_meta.master_rowid, &key, index_meta.columns.len(), None, &index_meta.collations)? {
                    return Err(Error::Constraint { kind: "unique", message: "sqlite UNIQUE constraint failed" });
                }
            }
        }
        let payload = encode_record(&resolved, self.encoding);
        let root = self.table_insert(meta.root_page, rowid, &payload)?;
        if root != meta.root_page {
            self.set_master_root(meta.master_rowid, root)?;
            meta.root_page = root;
        }
        for index_meta in meta.indexes.iter() {
            if index_meta.managed {
                continue;
            }
            let key = encode_index_key(&meta, index_meta, rowid, &resolved, self.encoding)?;
            self.index_entries.entry(index_meta.master_rowid).or_default().insert(key);
        }
        // An explicit rowid only raises the high-water mark; a lower one never
        // lowers it.
        if meta.autoincrement && rowid > self.sequence_value(&meta.table_name)?.unwrap_or(0) {
            self.set_sequence_value(&meta.table_name, rowid)?;
        }
        Ok(rowid)
    }

    /// Deletes one row by rowid; returns whether a row was removed.
    pub fn delete(&mut self, table: &str, rowid: i64) -> Result<bool> {
        let meta = self.table_meta(table)?;
        if meta.without_rowid {
            return Err(Error::Unsupported("sqlite rowid delete on a WITHOUT ROWID table is not supported"));
        }
        let Some(payload) = self.read_payload(meta.root_page, rowid)?
        else {
            return Ok(false);
        };
        let values = decode_record(&payload, self.encoding, Default::default()).map_err(|_| Error::Corrupt("sqlite record decode failed"))?;
        let owned = expand_alias(&meta, rowid, &values);
        for index_meta in meta.indexes.iter() {
            if index_meta.managed {
                continue;
            }
            let key = encode_index_key(&meta, index_meta, rowid, &owned, self.encoding)?;
            if let Some(entries) = self.index_entries.get_mut(&index_meta.master_rowid) {
                entries.remove(&key);
            }
        }
        let root = self.table_delete(meta.root_page, rowid)?;
        if root != meta.root_page {
            self.set_master_root(meta.master_rowid, root)?;
        }
        Ok(true)
    }

    /// Updates one row by rowid; `UpdateValue::Keep` preserves a column.
    /// Returns whether a row was changed.
    pub fn update(&mut self, table: &str, rowid: i64, values: &[UpdateValue]) -> Result<bool> {
        if values.is_empty() {
            return Ok(false);
        }
        let meta = self.table_meta(table)?;
        if meta.without_rowid {
            return Err(Error::Unsupported("sqlite rowid update on a WITHOUT ROWID table is not supported"));
        }
        if values.len() != meta.columns.len() {
            return Err(Error::Unsupported("sqlite update arity mismatch"));
        }
        let Some(cell) = self.find_table_cell(meta.root_page, rowid)?
        else {
            return Ok(false);
        };
        let payload = self.read_cell_payload(&cell)?;
        let decoded = decode_record(&payload, self.encoding, Default::default()).map_err(|_| Error::Corrupt("sqlite record decode failed"))?;
        let mut owned = expand_alias(&meta, rowid, &decoded);
        // 旧值只用于索引键对比与回写路径的索引摘除,避免再读一次 payload。
        let before = owned.clone();
        let mut next_rowid = rowid;
        for (index, slot) in values.iter().enumerate() {
            let UpdateValue::Set(value) = slot
            else {
                continue;
            };
            let column = &meta.columns[index];
            let value = apply_affinity(value.clone(), affinity_of(&column.declared_type));
            if matches!(value, WriteValue::Null) && column.not_null && Some(index) != meta.rowid_alias {
                return Err(Error::Constraint { kind: "not-null", message: "sqlite NOT NULL constraint failed" });
            }
            if Some(index) == meta.rowid_alias {
                let WriteValue::Integer(rowid_value) = value
                else {
                    return Err(Error::Unsupported("sqlite rowid alias must be an integer"));
                };
                next_rowid = rowid_value;
            }
            owned[index] = value.clone();
        }
        if next_rowid != rowid && self.contains_rowid(meta.root_page, next_rowid)? {
            return Err(Error::Constraint { kind: "unique", message: "sqlite UNIQUE constraint failed" });
        }
        for index_meta in meta.indexes.iter() {
            if index_meta.managed {
                continue;
            }
            if index_meta.unique && index_key_has_no_null(&owned, &index_meta.columns) {
                let key = encode_index_key(&meta, index_meta, next_rowid, &owned, self.encoding)?;
                if self.set_prefix_match(index_meta.master_rowid, &key, index_meta.columns.len(), Some(rowid), &index_meta.collations)? {
                    return Err(Error::Constraint { kind: "unique", message: "sqlite UNIQUE constraint failed" });
                }
            }
        }

        // The alias slot travels as `next_rowid` through the delete/insert
        // bookkeeping, but the stored record keeps NULL like every insert.
        if let Some(index) = meta.rowid_alias {
            owned[index] = WriteValue::Null;
        }
        let payload = encode_record(&owned, self.encoding);

        // Only indexes whose key actually changed need their live entry
        // rewritten. A rowid move always takes the delete/insert path.
        let mut index_changes: Vec<(i64, Vec<u8>, Vec<u8>)> = Vec::new();
        for index_meta in meta.indexes.iter() {
            if index_meta.managed {
                continue;
            }
            let old_key = encode_index_key(&meta, index_meta, rowid, &before, self.encoding)?;
            let new_key = encode_index_key(&meta, index_meta, next_rowid, &owned, self.encoding)?;
            if old_key != new_key {
                index_changes.push((index_meta.master_rowid, old_key, new_key));
            }
        }

        if next_rowid == rowid && self.replace_cell_in_place(&cell, &payload)? {
            for (master_rowid, old_key, new_key) in index_changes {
                if let Some(entries) = self.index_entries.get_mut(&master_rowid) {
                    entries.remove(&old_key);
                }
                self.index_entries.entry(master_rowid).or_default().insert(new_key);
            }
            return Ok(true);
        }

        self.remove_index_entries(&meta, rowid, &before)?;
        let mut root = self.table_delete(meta.root_page, rowid)?;
        root = self.table_insert(root, next_rowid, &payload)?;
        if root != meta.root_page {
            self.set_master_root(meta.master_rowid, root)?;
        }
        self.add_index_entries(&meta, next_rowid, &owned)?;
        Ok(true)
    }

    /// 摘除一行在全部非托管索引里的活动键。
    fn remove_index_entries(&mut self, meta: &TableMeta, rowid: i64, values: &[WriteValue]) -> Result<()> {
        for index_meta in meta.indexes.iter() {
            if index_meta.managed {
                continue;
            }
            let key = encode_index_key(meta, index_meta, rowid, values, self.encoding)?;
            if let Some(entries) = self.index_entries.get_mut(&index_meta.master_rowid) {
                entries.remove(&key);
            }
        }
        Ok(())
    }

    /// 写入一行在全部非托管索引里的活动键。
    fn add_index_entries(&mut self, meta: &TableMeta, rowid: i64, values: &[WriteValue]) -> Result<()> {
        for index_meta in meta.indexes.iter() {
            if index_meta.managed {
                continue;
            }
            let key = encode_index_key(meta, index_meta, rowid, values, self.encoding)?;
            self.index_entries.entry(index_meta.master_rowid).or_default().insert(key);
        }
        Ok(())
    }

    /// Removes one column slot from every stored row of a table.
    ///
    /// Records are decoded and re-encoded in place, so rowids, page order and
    /// the index trees stay untouched; the caller is responsible for proving
    /// that no surviving index or constraint reads the column, and for
    /// rewriting the stored `CREATE TABLE` text afterwards.
    pub fn drop_column(&mut self, table: &str, position: usize) -> Result<()> {
        let meta = self.table_meta(table)?;
        if meta.without_rowid {
            return Err(Error::Unsupported("sqlite WITHOUT ROWID drop column"));
        }
        let rows = self.table_rows(table, TableLimits::default())?;
        let mut root = meta.root_page;
        for row in rows {
            let mut values: Vec<WriteValue> = decode_record(&row.payload, self.encoding, Default::default())
                .map_err(|_| Error::Corrupt("sqlite record decode failed"))?
                .into_iter()
                .map(|value| match value {
                    RecordValue::Null => WriteValue::Null,
                    RecordValue::Integer(number) => WriteValue::Integer(number),
                    RecordValue::Real(number) => WriteValue::Real(number),
                    RecordValue::Blob(bytes) => WriteValue::Blob(bytes.to_vec()),
                    RecordValue::Text(text) => WriteValue::Text(text),
                })
                .collect();
            if position >= values.len() {
                return Err(Error::Corrupt("sqlite record has no such column"));
            }
            values.remove(position);
            let payload = encode_record(&values, self.encoding);
            root = self.table_delete(root, row.rowid)?;
            root = self.table_insert(root, row.rowid, &payload)?;
        }
        if root != meta.root_page {
            self.set_master_root(meta.master_rowid, root)?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // sqlite_master bookkeeping
    // ------------------------------------------------------------------

    fn master_rows(&self) -> Result<Vec<MasterRow>> {
        let rows = read_table(&self.pages, 1, TableLimits::default())?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let values = decode_record(&row.payload, self.encoding, Default::default())
                .map_err(|_| Error::Corrupt("sqlite master record decode failed"))?;
            if values.len() < 5 {
                return Err(Error::Corrupt("sqlite master record has too few columns"));
            }
            let text = |index: usize| match &values[index] {
                database_formats::sqlite::RecordValue::Text(text) => Ok(text.clone()),
                _ => Err(Error::Corrupt("sqlite master text column")),
            };
            let root_page = match &values[3] {
                database_formats::sqlite::RecordValue::Integer(page) => u32::try_from(*page).unwrap_or(0),
                database_formats::sqlite::RecordValue::Null => 0,
                _ => return Err(Error::Corrupt("sqlite master rootpage column")),
            };
            let sql = match &values[4] {
                database_formats::sqlite::RecordValue::Text(text) => Some(text.clone()),
                database_formats::sqlite::RecordValue::Null => None,
                _ => return Err(Error::Corrupt("sqlite master sql column")),
            };
            out.push(MasterRow { rowid: row.rowid, kind: text(0)?, name: text(1)?, table_name: text(2)?, root_page, sql });
        }
        Ok(out)
    }

    fn insert_master_row(&mut self, kind: &str, name: &str, table_name: &str, root_page: u32, sql: Option<&str>) -> Result<i64> {
        let rowid = self.max_rowid(1)?.saturating_add(1);
        let sql_value = match sql {
            Some(text) => WriteValue::Text(text.to_string()),
            None => WriteValue::Null,
        };
        let record = encode_record(
            &[
                WriteValue::Text(kind.to_string()),
                WriteValue::Text(name.to_string()),
                WriteValue::Text(table_name.to_string()),
                WriteValue::Integer(i64::from(root_page)),
                sql_value,
            ],
            self.encoding,
        );
        let root = self.table_insert(1, rowid, &record)?;
        if root != 1 {
            return Err(Error::Corrupt("sqlite master b-tree must not gain height"));
        }
        Ok(rowid)
    }

    fn set_master_root(&mut self, master_rowid: i64, root_page: u32) -> Result<()> {
        let Some(payload) = self.read_payload(1, master_rowid)?
        else {
            return Err(Error::Corrupt("sqlite master row vanished"));
        };
        let values =
            decode_record(&payload, self.encoding, Default::default()).map_err(|_| Error::Corrupt("sqlite master record decode failed"))?;
        let mut owned: Vec<WriteValue> = values
            .iter()
            .map(|value| match value {
                database_formats::sqlite::RecordValue::Null => WriteValue::Null,
                database_formats::sqlite::RecordValue::Integer(number) => WriteValue::Integer(*number),
                database_formats::sqlite::RecordValue::Real(real) => WriteValue::Real(*real),
                database_formats::sqlite::RecordValue::Blob(bytes) => WriteValue::Blob(bytes.to_vec()),
                database_formats::sqlite::RecordValue::Text(text) => WriteValue::Text(text.clone()),
            })
            .collect();
        owned[3] = WriteValue::Integer(i64::from(root_page));
        let record = encode_record(&owned, self.encoding);
        let root = self.table_replace(1, master_rowid, &record)?;
        if root != 1 {
            return Err(Error::Corrupt("sqlite master b-tree must not gain height"));
        }
        // 根页变化不抬升 schema cookie，就地刷新缓存里的对应表与索引。
        let cookie = read_schema_cookie(&self.pages);
        let mut cache = self.meta_cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if cache.cookie == cookie {
            for meta in cache.tables.values_mut() {
                if meta.master_rowid == master_rowid {
                    meta.root_page = root_page;
                }
                for index in std::sync::Arc::make_mut(&mut meta.indexes) {
                    if index.master_rowid == master_rowid {
                        index.root_page = root_page;
                    }
                }
            }
        }
        Ok(())
    }

    /// Rewrites the name, table name and `sql` text of one `sqlite_master` row
    /// while keeping its rowid, kind and root page untouched.
    fn rewrite_master_row(&mut self, rowid: i64, name: &str, table_name: &str, sql: Option<&str>) -> Result<()> {
        let Some(payload) = self.read_payload(1, rowid)?
        else {
            return Err(Error::Corrupt("sqlite master row vanished"));
        };
        let values =
            decode_record(&payload, self.encoding, Default::default()).map_err(|_| Error::Corrupt("sqlite master record decode failed"))?;
        let mut owned: Vec<WriteValue> = values
            .iter()
            .map(|value| match value {
                database_formats::sqlite::RecordValue::Null => WriteValue::Null,
                database_formats::sqlite::RecordValue::Integer(number) => WriteValue::Integer(*number),
                database_formats::sqlite::RecordValue::Real(real) => WriteValue::Real(*real),
                database_formats::sqlite::RecordValue::Blob(bytes) => WriteValue::Blob(bytes.to_vec()),
                database_formats::sqlite::RecordValue::Text(text) => WriteValue::Text(text.clone()),
            })
            .collect();
        owned[1] = WriteValue::Text(name.to_string());
        owned[2] = WriteValue::Text(table_name.to_string());
        owned[4] = match sql {
            Some(text) => WriteValue::Text(text.to_string()),
            None => WriteValue::Null,
        };
        let record = encode_record(&owned, self.encoding);
        let root = self.table_replace(1, rowid, &record)?;
        if root != 1 {
            return Err(Error::Corrupt("sqlite master b-tree must not gain height"));
        }
        Ok(())
    }

    /// Drops one explicit index and frees its b-tree. Automatic indexes are
    /// reported instead of removed, matching SQLite's refusal.
    pub fn drop_index(&mut self, name: &str) -> Result<DropIndexOutcome> {
        let rows = self.master_rows()?;
        let Some(row) = rows.iter().find(|row| row.kind == "index" && eq_ignore_ascii_case(&row.name, name)).cloned()
        else {
            return Ok(DropIndexOutcome::Missing);
        };
        if row.sql.is_none() {
            return Ok(DropIndexOutcome::Automatic);
        }
        self.free_subtree(row.root_page)?;
        self.index_entries.remove(&row.rowid);
        self.table_delete(1, row.rowid)?;
        self.bump_cookie();
        Ok(DropIndexOutcome::Dropped)
    }

    /// Creates a view: a `sqlite_master` row of kind `view` with root page 0,
    /// exactly as SQLite stores a view definition. Reading the view is the SQL
    /// surface's job.
    pub fn create_view(&mut self, name: &str, sql: &str) -> Result<()> {
        if name.is_empty() {
            return Err(Error::Unsupported("sqlite view name must not be empty"));
        }
        for object in self.master_rows()? {
            if eq_ignore_ascii_case(&object.name, name) {
                return Err(Error::Unsupported("sqlite schema object already exists"));
            }
        }
        self.insert_master_row("view", name, name, 0, Some(sql))?;
        self.bump_cookie();
        Ok(())
    }

    /// Drops one view's schema row; returns whether a view was removed.
    pub fn drop_view(&mut self, name: &str) -> Result<bool> {
        let rows = self.master_rows()?;
        let Some(view) = rows.iter().find(|row| row.kind == "view" && eq_ignore_ascii_case(&row.name, name)).cloned()
        else {
            return Ok(false);
        };
        self.table_delete(1, view.rowid)?;
        self.bump_cookie();
        Ok(true)
    }

    /// Creates a trigger: a `sqlite_master` row of kind `trigger` attached to a
    /// table, with root page 0. Firing the body is the SQL surface's job.
    pub fn create_trigger(&mut self, name: &str, table: &str, sql: &str) -> Result<()> {
        if name.is_empty() {
            return Err(Error::Unsupported("sqlite trigger name must not be empty"));
        }
        for object in self.master_rows()? {
            if object.kind == "trigger" && eq_ignore_ascii_case(&object.name, name) {
                return Err(Error::Unsupported("sqlite trigger already exists"));
            }
        }
        self.insert_master_row("trigger", name, table, 0, Some(sql))?;
        self.bump_cookie();
        Ok(())
    }

    /// Drops one trigger's schema row; returns whether a trigger was removed.
    pub fn drop_trigger(&mut self, name: &str) -> Result<bool> {
        let rows = self.master_rows()?;
        let Some(trigger) = rows.iter().find(|row| row.kind == "trigger" && eq_ignore_ascii_case(&row.name, name)).cloned()
        else {
            return Ok(false);
        };
        self.table_delete(1, trigger.rowid)?;
        self.bump_cookie();
        Ok(true)
    }

    /// Drops a table, every index attached to it, and its `sqlite_sequence`
    /// row. The b-tree pages return to the freelist before the catalog rows
    /// disappear, so the image stays consistent for an external reader.
    pub fn drop_table(&mut self, name: &str) -> Result<bool> {
        let rows = self.master_rows()?;
        let Some(table) = rows.iter().find(|row| row.kind == "table" && eq_ignore_ascii_case(&row.table_name, name)).cloned()
        else {
            return Ok(false);
        };
        let indexes: Vec<MasterRow> =
            rows.iter().filter(|row| row.kind == "index" && eq_ignore_ascii_case(&row.table_name, &table.table_name)).cloned().collect();
        for index in &indexes {
            self.free_subtree(index.root_page)?;
            self.index_entries.remove(&index.rowid);
        }
        let without_rowid = table.sql.as_deref().is_some_and(is_without_rowid);
        if table.root_page != 0 {
            if without_rowid {
                self.free_subtree(table.root_page)?;
            }
            else {
                self.free_table_subtree(table.root_page)?;
            }
        }
        if without_rowid {
            self.without_rowid_entries.remove(&table.rowid);
            self.without_rowid_keys.remove(&table.rowid);
        }
        for index in &indexes {
            self.table_delete(1, index.rowid)?;
        }
        let triggers: Vec<MasterRow> =
            rows.iter().filter(|row| row.kind == "trigger" && eq_ignore_ascii_case(&row.table_name, &table.table_name)).cloned().collect();
        for trigger in &triggers {
            self.table_delete(1, trigger.rowid)?;
        }
        self.table_delete(1, table.rowid)?;
        if table.sql.as_deref().is_some_and(has_autoincrement) {
            self.delete_sequence_row(name)?;
        }
        self.bump_cookie();
        Ok(true)
    }

    /// Replaces the stored `CREATE TABLE` text, used by `ALTER TABLE ADD
    /// COLUMN` so every later read derives the new column list from schema.
    pub fn rewrite_table_sql(&mut self, table: &str, sql: &str) -> Result<()> {
        let rows = self.master_rows()?;
        let Some(row) = rows.iter().find(|row| row.kind == "table" && eq_ignore_ascii_case(&row.table_name, table))
        else {
            return Err(Error::Unsupported("sqlite table does not exist"));
        };
        let (rowid, name) = (row.rowid, row.name.clone());
        self.rewrite_master_row(rowid, &name, &name, Some(sql))?;
        self.bump_cookie();
        Ok(())
    }

    /// Replaces the stored `CREATE VIEW` text, used by `ALTER TABLE RENAME`
    /// so a view keeps following the table or column it references.
    pub fn rewrite_view_sql(&mut self, view: &str, sql: &str) -> Result<()> {
        let rows = self.master_rows()?;
        let Some(row) = rows.iter().find(|row| row.kind == "view" && eq_ignore_ascii_case(&row.name, view))
        else {
            return Err(Error::Unsupported("sqlite view does not exist"));
        };
        let (rowid, name) = (row.rowid, row.name.clone());
        self.rewrite_master_row(rowid, &name, &name, Some(sql))?;
        self.bump_cookie();
        Ok(())
    }

    /// Reads the `user_version` field at byte 60 of the database header.
    pub fn user_version(&self) -> i64 {
        i64::from(i32::from_be_bytes(self.pages[60..64].try_into().expect("header user version")))
    }

    /// Writes the `user_version` field at byte 60 of the database header.
    pub fn set_user_version(&mut self, value: i64) {
        self.touch_page(1);
        self.pages[60..64].copy_from_slice(&(value as i32).to_be_bytes());
    }

    /// Reads the `application_id` field at byte 68 of the database header.
    pub fn application_id(&self) -> i64 {
        i64::from(i32::from_be_bytes(self.pages[68..72].try_into().expect("header application id")))
    }

    /// Writes the `application_id` field at byte 68 of the database header.
    pub fn set_application_id(&mut self, value: i64) {
        self.touch_page(1);
        self.pages[68..72].copy_from_slice(&(value as i32).to_be_bytes());
    }

    /// Renames a table in the catalog: the table row, its `sql` text, the
    /// `tbl_name` and `sql` of every attached index, the automatic index names
    /// and the matching `sqlite_sequence` row.
    pub fn rename_table_schema(&mut self, old: &str, new: &str, sql: &str, index_sqls: &[(String, String)]) -> Result<()> {
        let rows = self.master_rows()?;
        let table_rows: Vec<MasterRow> =
            rows.iter().filter(|row| row.kind == "table" && eq_ignore_ascii_case(&row.table_name, old)).cloned().collect();
        for row in &table_rows {
            self.rewrite_master_row(row.rowid, new, new, Some(sql))?;
        }
        let indexes: Vec<MasterRow> =
            rows.iter().filter(|row| row.kind == "index" && eq_ignore_ascii_case(&row.table_name, old)).cloned().collect();
        for row in &indexes {
            match row.sql.as_deref() {
                Some(current) => {
                    let updated = index_sqls
                        .iter()
                        .find(|(name, _)| eq_ignore_ascii_case(name, &row.name))
                        .map(|(_, text)| text.as_str())
                        .unwrap_or(current);
                    self.rewrite_master_row(row.rowid, &row.name, new, Some(updated))?;
                }
                None => {
                    let renamed = renamed_autoindex(&row.name, new);
                    self.rewrite_master_row(row.rowid, &renamed, new, None)?;
                }
            }
        }
        self.rename_sequence_row(old, new)?;
        self.bump_cookie();
        Ok(())
    }

    /// Rewrites the `sql` text of a table and of its explicit indexes after a
    /// column rename. Key positions do not move, so no index tree is rebuilt.
    pub fn rename_column_schema(&mut self, table: &str, sql: &str, index_sqls: &[(String, String)]) -> Result<()> {
        self.rewrite_table_sql(table, sql)?;
        let rows = self.master_rows()?;
        for row in rows.iter().filter(|row| row.kind == "index" && eq_ignore_ascii_case(&row.table_name, table) && row.sql.is_some()) {
            if let Some((_, text)) = index_sqls.iter().find(|(name, _)| eq_ignore_ascii_case(name, &row.name)) {
                self.rewrite_master_row(row.rowid, &row.name, table, Some(text))?;
            }
        }
        Ok(())
    }

    /// Removes the `sqlite_sequence` row of a dropped AUTOINCREMENT table.
    fn delete_sequence_row(&mut self, table: &str) -> Result<()> {
        if !self.master_rows()?.iter().any(|row| row.kind == "table" && eq_ignore_ascii_case(&row.name, "sqlite_sequence")) {
            return Ok(());
        }
        let stored = self.table_rows("sqlite_sequence", TableLimits::default())?;
        for row in stored {
            let values = decode_record(&row.payload, self.encoding, Default::default())
                .map_err(|_| Error::Corrupt("sqlite sequence record decode failed"))?;
            if matches!(values.first(), Some(database_formats::sqlite::RecordValue::Text(name)) if eq_ignore_ascii_case(name, table)) {
                self.delete("sqlite_sequence", row.rowid)?;
                return Ok(());
            }
        }
        Ok(())
    }

    /// Renames the `sqlite_sequence` row of a renamed AUTOINCREMENT table.
    fn rename_sequence_row(&mut self, old: &str, new: &str) -> Result<()> {
        let rows = self.master_rows()?;
        let Some(sequence) = rows.iter().find(|row| row.kind == "table" && eq_ignore_ascii_case(&row.name, "sqlite_sequence")).cloned()
        else {
            return Ok(());
        };
        let stored = self.table_rows("sqlite_sequence", TableLimits::default())?;
        for row in stored {
            let values = decode_record(&row.payload, self.encoding, Default::default())
                .map_err(|_| Error::Corrupt("sqlite sequence record decode failed"))?;
            if matches!(values.first(), Some(database_formats::sqlite::RecordValue::Text(name)) if eq_ignore_ascii_case(name, old)) {
                let record = encode_record(&[WriteValue::Text(new.to_string()), sequence_value(values.get(1))], self.encoding);
                let root = self.table_replace(sequence.root_page, row.rowid, &record)?;
                if root != sequence.root_page {
                    self.set_master_root(sequence.rowid, root)?;
                }
                return Ok(());
            }
        }
        Ok(())
    }

    /// Frees every page of a rowid table b-tree, overflow chains included.
    fn free_table_subtree(&mut self, page: u32) -> Result<()> {
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let kind = self.pages[offset + header_relative];
        match kind {
            PAGE_TABLE_LEAF => {
                for (_, _, cell) in self.leaf_cells(page)? {
                    let (length, length_size) = decode_varint(&cell)?;
                    let length = usize::try_from(length).map_err(|_| Error::Unsupported("sqlite payload too large"))?;
                    let local = self.local_payload(length);
                    if length > local {
                        let head = u32::from_be_bytes(cell[length_size + local..length_size + local + 4].try_into().unwrap());
                        self.free_overflow_chain(head);
                    }
                }
                self.free_page(page);
                Ok(())
            }
            PAGE_TABLE_INTERIOR => {
                let count =
                    u16::from_be_bytes(self.pages[offset + header_relative + 3..offset + header_relative + 5].try_into().unwrap()) as usize;
                let mut children = Vec::with_capacity(count + 1);
                for index in 0..count {
                    let pointer = offset + header_relative + 12 + index * 2;
                    let cell_offset = u16::from_be_bytes(self.pages[pointer..pointer + 2].try_into().unwrap()) as usize;
                    let start = offset + cell_offset;
                    children.push(u32::from_be_bytes(self.pages[start..start + 4].try_into().unwrap()));
                }
                let start = offset + header_relative + 8;
                children.push(u32::from_be_bytes(self.pages[start..start + 4].try_into().unwrap()));
                for child in children {
                    self.free_table_subtree(child)?;
                }
                self.free_page(page);
                Ok(())
            }
            _ => Err(Error::Corrupt("sqlite table b-tree page type invalid")),
        }
    }

    /// Resolves the schema of one table, reusing the parse cached for the
    /// current schema cookie. Any DDL raises the cookie and invalidates the
    /// whole cache, so a hit always matches the bytes in `pages`.
    fn table_meta(&self, table: &str) -> Result<TableMeta> {
        let mut cache = self.meta_cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let cookie = read_schema_cookie(&self.pages);
        if cache.cookie != cookie {
            cache.tables.clear();
            cache.cookie = cookie;
        }
        let key = table.to_ascii_lowercase();
        if let Some(meta) = cache.tables.get(&key) {
            return Ok(meta.clone());
        }
        let meta = self.load_table_meta(table)?;
        cache.tables.insert(key, meta.clone());
        Ok(meta)
    }

    fn load_table_meta(&self, table: &str) -> Result<TableMeta> {
        let rows = self.master_rows()?;
        let table_row = rows
            .iter()
            .find(|row| row.kind == "table" && eq_ignore_ascii_case(&row.table_name, table))
            .ok_or_else(|| Error::Unsupported("sqlite table does not exist"))?;
        let sql = table_row.sql.as_deref().ok_or(Error::Unsupported("sqlite table has no CREATE text"))?;
        let columns = parse_create_table(sql)?;
        let without_rowid = is_without_rowid(sql);
        let table_key_names = find_primary_key_columns(sql)?.unwrap_or_default();
        let mut table_key_positions = Vec::with_capacity(table_key_names.len());
        for name in &table_key_names {
            let position = columns
                .iter()
                .position(|column| eq_ignore_ascii_case(&column.name, name))
                .ok_or(Error::Corrupt("sqlite primary key references unknown column"))?;
            table_key_positions.push(position);
        }
        // A table-level key on a rowid table also counts as the alias when it
        // is one declared INTEGER column; `WITHOUT ROWID` never has a rowid.
        let rowid_alias = if without_rowid {
            None
        }
        else if let [position] = table_key_positions.as_slice() {
            if columns[*position].declared_type.eq_ignore_ascii_case("INTEGER") {
                Some(*position)
            }
            else {
                columns.iter().position(is_rowid_alias)
            }
        }
        else {
            columns.iter().position(is_rowid_alias)
        };
        // An inline `column PRIMARY KEY` is the key of a WITHOUT ROWID table
        // when no table-level clause names one.
        let mut without_rowid_key_positions = table_key_positions.clone();
        if without_rowid && without_rowid_key_positions.is_empty() {
            for (position, column) in columns.iter().enumerate() {
                if column.primary_key {
                    without_rowid_key_positions.push(position);
                }
            }
            if without_rowid_key_positions.len() > 1 {
                return Err(Error::Corrupt("sqlite WITHOUT ROWID table declares more than one primary key"));
            }
        }
        let without_rowid_layout = if without_rowid {
            let mut layout = without_rowid_key_positions.clone();
            for position in 0..columns.len() {
                if !layout.contains(&position) {
                    layout.push(position);
                }
            }
            layout
        }
        else {
            Vec::new()
        };
        let mut meta = TableMeta {
            table_name: table_row.table_name.clone(),
            master_rowid: table_row.rowid,
            root_page: table_row.root_page,
            autoincrement: columns.iter().any(|column| column.autoincrement),
            columns: std::sync::Arc::new(columns),
            rowid_alias,
            without_rowid,
            without_rowid_layout,
            primary_key_len: if without_rowid { without_rowid_key_positions.len() } else { 0 },
            indexes: std::sync::Arc::new(Vec::new()),
        };
        // Automatic constraint indexes are numbered in declaration order:
        // each column-level PRIMARY KEY / UNIQUE first, then a table-level key.
        // A WITHOUT ROWID table is itself the primary-key index, so none exist.
        let mut constraint_columns: Vec<Vec<usize>> = Vec::new();
        if !without_rowid {
            for (index, column) in meta.columns.iter().enumerate() {
                if Some(index) == rowid_alias {
                    continue;
                }
                if column.primary_key || column.unique {
                    constraint_columns.push(vec![index]);
                }
            }
            for (primary, positions) in parse_table_constraints(sql, &meta.columns)? {
                if primary && rowid_alias.is_some() {
                    continue;
                }
                constraint_columns.push(positions);
            }
        }
        for row in rows.iter().filter(|row| row.kind == "index" && eq_ignore_ascii_case(&row.table_name, table)) {
            let (positions, unique, managed) = match row.sql.as_deref() {
                Some(sql) => {
                    let unique = sql.to_ascii_uppercase().starts_with("CREATE UNIQUE INDEX");
                    if index_sql_is_managed(sql)? {
                        (Vec::new(), unique, true)
                    }
                    else {
                        let (column_names, unique) = parse_create_index(sql)?;
                        let mut positions = Vec::with_capacity(column_names.len());
                        for name in column_names {
                            let position = meta
                                .columns
                                .iter()
                                .position(|column| eq_ignore_ascii_case(&column.name, &name))
                                .ok_or(Error::Corrupt("sqlite index references unknown column"))?;
                            positions.push(position);
                        }
                        (positions, unique, false)
                    }
                }
                None => {
                    // Automatic constraint indexes store no CREATE text; the
                    // `sqlite_autoindex_<table>_<n>` suffix names the n-th
                    // constraint in declaration order.
                    let number = row
                        .name
                        .rsplit_once('_')
                        .and_then(|(_, suffix)| suffix.parse::<usize>().ok())
                        .ok_or(Error::Corrupt("sqlite autoindex name unrecognized"))?;
                    let positions =
                        constraint_columns.get(number - 1).cloned().ok_or(Error::Corrupt("sqlite autoindex number out of range"))?;
                    (positions, true, false)
                }
            };
            let collations = match row.sql.as_deref() {
                Some(sql) => index_collations(sql, &meta.columns)?,
                None => positions.iter().map(|position| meta.columns[*position].collate.unwrap_or_default()).collect(),
            };
            std::sync::Arc::make_mut(&mut meta.indexes).push(IndexMeta {
                master_rowid: row.rowid,
                root_page: row.root_page,
                columns: positions,
                unique,
                managed,
                collations,
            });
        }
        Ok(meta)
    }

    fn bump_cookie(&mut self) {
        let cookie = read_schema_cookie(&self.pages).wrapping_add(1);
        self.touch_page(1);
        set_schema_cookie(&mut self.pages, cookie);
    }

    // ------------------------------------------------------------------
    // Page mechanics
    // ------------------------------------------------------------------

    fn page_offset(&self, page: u32) -> usize {
        (page as usize - 1) * self.page_size
    }

    /// Bounds-checked page window for read paths that accept arbitrary input.
    ///
    /// Write paths operate on the writer's own image and keep using
    /// [`Self::page_offset`]; anything that parses a page number out of a file
    /// must go through here so a corrupt reference becomes a structured error.
    fn checked_page_bounds(&self, page: u32) -> Result<std::ops::Range<usize>> {
        if page == 0 {
            return Err(Error::Corrupt("sqlite page number zero"));
        }
        let start = (page as usize - 1).checked_mul(self.page_size).ok_or(Error::Corrupt("sqlite page offset overflow"))?;
        let end = start.checked_add(self.page_size).ok_or(Error::Corrupt("sqlite page offset overflow"))?;
        if end > self.pages.len() {
            return Err(Error::Corrupt("sqlite page reference out of bounds"));
        }
        Ok(start..end)
    }

    fn checked_page(&self, page: u32) -> Result<&[u8]> {
        Ok(&self.pages[self.checked_page_bounds(page)?])
    }

    fn page_count(&self) -> u32 {
        (self.pages.len() / self.page_size) as u32
    }

    fn page_u16(page: &[u8], offset: usize, context: &'static str) -> Result<usize> {
        let end = offset.checked_add(2).ok_or(Error::Corrupt(context))?;
        let bytes = page.get(offset..end).ok_or(Error::Corrupt(context))?;
        let value = <[u8; 2]>::try_from(bytes).map_err(|_| Error::Corrupt(context))?;
        Ok(usize::from(u16::from_be_bytes(value)))
    }

    fn page_u32(page: &[u8], offset: usize, context: &'static str) -> Result<u32> {
        let end = offset.checked_add(4).ok_or(Error::Corrupt(context))?;
        let bytes = page.get(offset..end).ok_or(Error::Corrupt(context))?;
        let value = <[u8; 4]>::try_from(bytes).map_err(|_| Error::Corrupt(context))?;
        Ok(u32::from_be_bytes(value))
    }

    fn header_offset(page: u32) -> usize {
        if page == 1 { 100 } else { 0 }
    }

    /// Cell-plus-pointer capacity of a b-tree page. Page 1 additionally owns
    /// the 100-byte database header, so its b-tree region starts later and
    /// holds correspondingly less.
    fn page_capacity(&self, page: u32) -> usize {
        self.usable - Self::header_offset(page) - 8
    }

    fn init_leaf_page(&mut self, page: u32, kind: u8) {
        self.touch_page(page);
        let offset = self.page_offset(page);
        self.pages[offset..offset + self.page_size].fill(0);
        let header = offset + Self::header_offset(page);
        self.pages[header] = kind;
        let content_start = if self.page_size == 65_536 { 0u16 } else { self.page_size as u16 };
        self.pages[header + 5..header + 7].copy_from_slice(&content_start.to_be_bytes());
    }

    fn allocate_page(&mut self) -> Result<u32> {
        let trunk = read_freelist_trunk(&self.pages);
        if trunk != 0 {
            self.touch_page(trunk);
            self.touch_page(1);
            let offset = self.page_offset(trunk);
            let leaf_count = u32::from_be_bytes(self.pages[offset + 4..offset + 8].try_into().unwrap());
            if leaf_count > 0 {
                let slot = offset + 8 + (leaf_count as usize - 1) * 4;
                let leaf = u32::from_be_bytes(self.pages[slot..slot + 4].try_into().unwrap());
                self.pages[offset + 4..offset + 8].copy_from_slice(&(leaf_count - 1).to_be_bytes());
                self.decrement_freelist();
                return Ok(leaf);
            }
            let next = u32::from_be_bytes(self.pages[offset..offset + 4].try_into().unwrap());
            set_freelist_trunk(&mut self.pages, next);
            self.decrement_freelist();
            return Ok(trunk);
        }
        let page = (self.pages.len() / self.page_size) as u32 + 1;
        self.touch_page(1);
        self.pages.extend(std::iter::repeat_n(0u8, self.page_size));
        set_page_count(&mut self.pages, page);
        Ok(page)
    }

    fn decrement_freelist(&mut self) {
        let count = read_freelist_count(&self.pages);
        self.touch_page(1);
        set_freelist_count(&mut self.pages, count.saturating_sub(1));
    }

    fn free_page(&mut self, page: u32) {
        let trunk = read_freelist_trunk(&self.pages);
        if trunk != 0 {
            self.touch_page(trunk);
            self.touch_page(1);
            let offset = self.page_offset(trunk);
            let leaf_count = u32::from_be_bytes(self.pages[offset + 4..offset + 8].try_into().unwrap());
            let capacity = (self.usable - 8) / 4;
            if (leaf_count as usize) < capacity {
                let slot = offset + 8 + leaf_count as usize * 4;
                self.pages[slot..slot + 4].copy_from_slice(&page.to_be_bytes());
                self.pages[offset + 4..offset + 8].copy_from_slice(&(leaf_count + 1).to_be_bytes());
                let count = read_freelist_count(&self.pages);
                set_freelist_count(&mut self.pages, count + 1);
                return;
            }
        }
        self.touch_page(page);
        self.touch_page(1);
        let offset = self.page_offset(page);
        self.pages[offset..offset + self.page_size].fill(0);
        self.pages[offset..offset + 4].copy_from_slice(&trunk.to_be_bytes());
        set_freelist_trunk(&mut self.pages, page);
        let count = read_freelist_count(&self.pages);
        set_freelist_count(&mut self.pages, count + 1);
    }

    /// Cell bytes plus the span `[start, end)` it occupies on the page.
    fn leaf_cells(&self, page: u32) -> Result<Vec<(i64, usize, Vec<u8>)>> {
        self.cells_common(page, |cell| {
            let (length, length_size) = decode_varint(cell)?;
            let length = usize::try_from(length).map_err(|_| Error::Unsupported("sqlite payload size exceeds host"))?;
            let (rowid, rowid_size) = decode_varint(&cell[length_size..])?;
            Ok(CellInfo { rowid: rowid as i64, prefix: length_size + rowid_size, length })
        })
    }

    /// 只解析表叶页各 cell 的 rowid 序列，不物化任何 cell 字节。
    /// 插入下钻的重复判定与插入位置只需要有序键序列；cell 字节只在
    /// 分裂路径才需要，由调用方另行走 [`Self::leaf_cells`]。
    /// 页内 cell 指针数组按 rowid 升序排列，返回的序列同样升序。
    fn leaf_rowids(&self, page: u32) -> Result<Vec<i64>> {
        let page_bytes = self.checked_page(page)?;
        let header_relative = Self::header_offset(page);
        if *page_bytes.get(header_relative).ok_or(Error::Corrupt("sqlite page header truncated"))? != PAGE_TABLE_LEAF {
            return Err(Error::Corrupt("sqlite page type invalid"));
        }
        let count = Self::page_u16(page_bytes, header_relative + 3, "sqlite page header truncated")?;
        if header_relative + 8 + count * 2 > self.usable {
            return Err(Error::Corrupt("sqlite cell pointer array out of bounds"));
        }
        let mut out = Vec::with_capacity(count);
        for index in 0..count {
            let pointer = header_relative + 8 + index * 2;
            let cell_offset = Self::page_u16(page_bytes, pointer, "sqlite cell pointer truncated")?;
            if cell_offset >= self.usable {
                return Err(Error::Corrupt("sqlite cell pointer out of bounds"));
            }
            let tail = page_bytes.get(cell_offset..).ok_or(Error::Corrupt("sqlite cell pointer out of bounds"))?;
            let (_, length_size) = decode_varint(tail)?;
            let rowid_start = cell_offset.checked_add(length_size).ok_or(Error::Corrupt("sqlite cell offset overflow"))?;
            let rowid_tail = page_bytes.get(rowid_start..).ok_or(Error::Corrupt("sqlite rowid truncated"))?;
            let (rowid, _) = decode_varint(rowid_tail)?;
            out.push(rowid as i64);
        }
        Ok(out)
    }

    fn cells_common<F>(&self, page: u32, mut parse: F) -> Result<Vec<(i64, usize, Vec<u8>)>>
    where
        F: FnMut(&[u8]) -> Result<CellInfo>,
    {
        let page_bytes = self.checked_page(page)?;
        let header_relative = Self::header_offset(page);
        let kind = *page_bytes.get(header_relative).ok_or(Error::Corrupt("sqlite page header truncated"))?;
        let header_size = match kind {
            PAGE_TABLE_INTERIOR | PAGE_INDEX_INTERIOR => 12,
            PAGE_TABLE_LEAF | PAGE_INDEX_LEAF => 8,
            _ => return Err(Error::Corrupt("sqlite page type invalid")),
        };
        let count = Self::page_u16(page_bytes, header_relative + 3, "sqlite page header truncated")?;
        if header_relative + header_size + count * 2 > self.usable {
            return Err(Error::Corrupt("sqlite cell pointer array out of bounds"));
        }
        let mut out = Vec::with_capacity(count);
        for index in 0..count {
            let pointer = header_relative + header_size + index * 2;
            let cell_offset = Self::page_u16(page_bytes, pointer, "sqlite cell pointer truncated")?;
            if cell_offset >= self.usable {
                return Err(Error::Corrupt("sqlite cell pointer out of bounds"));
            }
            let tail = page_bytes.get(cell_offset..).ok_or(Error::Corrupt("sqlite cell pointer out of bounds"))?;
            let info = parse(tail)?;
            let local = self.local_payload(info.length);
            let span = info.prefix + local + if info.length > local { 4 } else { 0 };
            if cell_offset.checked_add(span).is_none_or(|end| end > self.usable) {
                return Err(Error::Corrupt("sqlite cell payload truncated"));
            }
            out.push((info.rowid, cell_offset, page_bytes[cell_offset..cell_offset + span].to_vec()));
        }
        Ok(out)
    }

    fn local_payload(&self, length: usize) -> usize {
        let maximum = self.usable - 35;
        if length <= maximum {
            return length;
        }
        let minimum = ((self.usable - 12) * 32 / 255) - 23;
        let candidate = minimum + (length - minimum) % (self.usable - 4);
        if candidate <= maximum { candidate } else { minimum }
    }

    fn write_overflow(&mut self, payload: &[u8], local: usize) -> Result<u32> {
        let mut remaining = &payload[local..];
        let per_page = self.usable - 4;
        let head = self.allocate_page()?;
        let mut current = head;
        loop {
            let chunk = remaining.len().min(per_page);
            self.touch_page(current);
            let offset = self.page_offset(current);
            self.pages[offset..offset + self.page_size].fill(0);
            self.pages[offset + 4..offset + 4 + chunk].copy_from_slice(&remaining[..chunk]);
            remaining = &remaining[chunk..];
            if remaining.is_empty() {
                break;
            }
            let next = self.allocate_page()?;
            self.pages[offset..offset + 4].copy_from_slice(&next.to_be_bytes());
            current = next;
        }
        Ok(head)
    }

    fn free_overflow_chain(&mut self, head: u32) {
        let mut next = head;
        while next != 0 {
            let offset = self.page_offset(next);
            let following = u32::from_be_bytes(self.pages[offset..offset + 4].try_into().unwrap());
            self.free_page(next);
            next = following;
        }
    }

    fn build_cell(&mut self, kind: u8, rowid: Option<i64>, payload: &[u8]) -> Result<Vec<u8>> {
        let local = self.local_payload(payload.len());
        let mut cell = Vec::new();
        encode_varint_into(&mut cell, payload.len() as u64);
        if kind == PAGE_TABLE_LEAF {
            encode_varint_into(&mut cell, rowid.expect("table cell carries a rowid") as u64);
        }
        cell.extend_from_slice(&payload[..local]);
        if payload.len() > local {
            let head = self.write_overflow(payload, local)?;
            cell.extend_from_slice(&head.to_be_bytes());
        }
        Ok(cell)
    }

    /// Free space available for new cell bytes plus pointer entries.
    fn free_space(&self, page: u32) -> usize {
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let page_bytes = &self.pages[offset..offset + self.page_size];
        let kind = page_bytes[header_relative];
        let header_size = if matches!(kind, PAGE_TABLE_INTERIOR | PAGE_INDEX_INTERIOR) { 12 } else { 8 };
        let count = u16::from_be_bytes(page_bytes[header_relative + 3..header_relative + 5].try_into().unwrap()) as usize;
        let pointers_end = header_relative + header_size + count * 2;
        let raw_start = u16::from_be_bytes(page_bytes[header_relative + 5..header_relative + 7].try_into().unwrap());
        let content_start = if raw_start == 0 { 65_536 } else { raw_start as usize };
        content_start.saturating_sub(pointers_end)
    }

    /// Writes sorted cells into a leaf page from scratch (no freeblocks).
    fn rebuild_leaf(&mut self, page: u32, kind: u8, cells: &[Vec<u8>]) {
        self.touch_page(page);
        let offset = self.page_offset(page);
        // Page 1 owns the 100-byte database header (magic, page size, text
        // encoding, reserved bytes), so a rebuild may only clear the b-tree
        // region that follows it.
        let btree_start = offset + Self::header_offset(page);
        self.pages[btree_start..offset + self.page_size].fill(0);
        let header_relative = Self::header_offset(page);
        self.pages[offset + header_relative] = kind;
        let count = cells.len();
        self.pages[offset + header_relative + 3..offset + header_relative + 5].copy_from_slice(&(count as u16).to_be_bytes());
        let mut content = if self.page_size == 65_536 { 65_536 } else { self.page_size };
        let pointer_base = header_relative + if matches!(kind, PAGE_TABLE_INTERIOR | PAGE_INDEX_INTERIOR) { 12 } else { 8 };
        for (index, cell) in cells.iter().enumerate() {
            content -= cell.len();
            self.pages[offset + content..offset + content + cell.len()].copy_from_slice(cell);
            let pointer = offset + pointer_base + index * 2;
            self.pages[pointer..pointer + 2].copy_from_slice(&(content as u16).to_be_bytes());
        }
        let content_field = if content == 65_536 { 0u16 } else { content as u16 };
        self.pages[offset + header_relative + 5..offset + header_relative + 7].copy_from_slice(&content_field.to_be_bytes());
    }

    /// Inserts one cell into a leaf without splitting; caller checked space.
    fn place_leaf_cell(&mut self, page: u32, position: usize, cell: &[u8]) {
        self.touch_page(page);
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let raw_start = u16::from_be_bytes(self.pages[offset + header_relative + 5..offset + header_relative + 7].try_into().unwrap());
        let content_start = if raw_start == 0 { 65_536 } else { raw_start as usize };
        let target = content_start - cell.len();
        self.pages[offset + target..offset + target + cell.len()].copy_from_slice(cell);
        self.insert_leaf_pointer(page, position, target);
        let content_field = if target == 65_536 { 0u16 } else { target as u16 };
        self.pages[offset + header_relative + 5..offset + header_relative + 7].copy_from_slice(&content_field.to_be_bytes());
    }

    /// Inserts one cell at an already allocated offset and adds its pointer.
    fn place_leaf_cell_at(&mut self, page: u32, position: usize, cell: &[u8], start: usize) {
        self.touch_page(page);
        let offset = self.page_offset(page);
        self.pages[offset + start..offset + start + cell.len()].copy_from_slice(cell);
        self.insert_leaf_pointer(page, position, start);
    }

    /// Adds `start` to the page's cell pointer array at `position`, shifting the
    /// pointers above it right by one slot.
    fn insert_leaf_pointer(&mut self, page: u32, position: usize, start: usize) {
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let kind = self.pages[offset + header_relative];
        let header_size = if matches!(kind, PAGE_TABLE_INTERIOR | PAGE_INDEX_INTERIOR) { 12 } else { 8 };
        let count = u16::from_be_bytes(self.pages[offset + header_relative + 3..offset + header_relative + 5].try_into().unwrap()) as usize;
        let pointer_base = offset + header_relative + header_size;
        let shift_start = pointer_base + position * 2;
        let shift_end = pointer_base + count * 2;
        self.pages.copy_within(shift_start..shift_end, shift_start + 2);
        self.pages[shift_start..shift_start + 2].copy_from_slice(&(start as u16).to_be_bytes());
        self.pages[offset + header_relative + 3..offset + header_relative + 5].copy_from_slice(&((count + 1) as u16).to_be_bytes());
    }

    /// Carves a `size`-byte span out of the page's freeblock chain and returns
    /// its offset.
    ///
    /// Reusing the hole a `DELETE` left behind keeps a `DELETE` + `INSERT`
    /// pair (an `UPDATE` row rewrite) from shifting the cell content area and
    /// eventually forcing a whole-leaf compaction. A remainder smaller than
    /// the four-byte freeblock header is counted as fragments, and the block
    /// is ignored once those fragments would exceed the 60-byte page limit,
    /// matching upstream `pageFindSlot`.
    fn take_freeblock(&mut self, page: u32, size: usize) -> Option<usize> {
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        // 上游 `allocateSpace` 只在指针数组还能再放一项时才查 freeblock
        // 链。少了这道检查，复用块里的新 cell 会让指针数组写进 cell
        // content 区，页头的 content offset 与实际阵列重叠。
        let kind = self.pages[offset + header_relative];
        let header_size = if matches!(kind, PAGE_TABLE_INTERIOR | PAGE_INDEX_INTERIOR) { 12 } else { 8 };
        let count = u16::from_be_bytes(self.pages[offset + header_relative + 3..offset + header_relative + 5].try_into().unwrap()) as usize;
        let raw_start = u16::from_be_bytes(self.pages[offset + header_relative + 5..offset + header_relative + 7].try_into().unwrap());
        let content_start = if raw_start == 0 { 65_536 } else { raw_start as usize };
        if header_relative + header_size + count * 2 + 2 > content_start {
            return None;
        }
        let mut link = header_relative + 1;
        let mut cursor =
            u16::from_be_bytes(self.pages[offset + header_relative + 1..offset + header_relative + 3].try_into().unwrap()) as usize;
        while cursor != 0 {
            let next = u16::from_be_bytes(self.pages[offset + cursor..offset + cursor + 2].try_into().unwrap()) as usize;
            let block_size = u16::from_be_bytes(self.pages[offset + cursor + 2..offset + cursor + 4].try_into().unwrap()) as usize;
            if block_size >= size {
                let remainder = block_size - size;
                // 一个页面的碎片总量不得超过 60 字节。上游 `pageFindSlot`
                // 在 `nFrag` 已超过 57 时放弃整条链，由调用方整理整页或
                // 从 content 区顶部另分配，剩余 1 到 3 字节才计入 `nFrag`。
                if remainder > 0 && remainder < 4 && self.pages[offset + header_relative + 7] > 57 {
                    return None;
                }
                self.touch_page(page);
                if remainder >= 4 {
                    let split = cursor + size;
                    self.pages[offset + split..offset + split + 2].copy_from_slice(&(next as u16).to_be_bytes());
                    self.pages[offset + split + 2..offset + split + 4].copy_from_slice(&(remainder as u16).to_be_bytes());
                    self.pages[offset + link..offset + link + 2].copy_from_slice(&(split as u16).to_be_bytes());
                }
                else {
                    self.pages[offset + link..offset + link + 2].copy_from_slice(&(next as u16).to_be_bytes());
                    // 剩余 1 到 3 字节放不下 freeblock 头，按上游语义记为碎片，
                    // 页头 `nFrag` 必须同步累加，否则 `integrity_check` 会报
                    // “Fragmentation of N bytes reported as 0”。
                    if remainder > 0 {
                        let fragmented = offset + header_relative + 7;
                        let current = self.pages[fragmented] as usize;
                        self.pages[fragmented] = (current + remainder) as u8;
                    }
                }
                return Some(cursor);
            }
            link = cursor;
            cursor = next;
        }
        None
    }

    // ------------------------------------------------------------------
    // Rowid b-tree
    // ------------------------------------------------------------------

    /// 表中最大 rowid；空表为 0。沿内部节点最右孩子下降到最右叶，只读
    /// 最后一个 cell 的 rowid（叶内按 rowid 升序），复杂度 O(log n)。
    /// 最右叶恰为空（删除后尚未回收的罕见形态）时回退全表扫描，
    /// 与旧实现语义完全一致。
    fn max_rowid(&self, root: u32) -> Result<i64> {
        let mut page = root;
        let mut visited = std::collections::BTreeSet::new();
        loop {
            if page == 0 || page > self.page_count() {
                return Err(Error::Corrupt("sqlite table page reference out of bounds"));
            }
            if !visited.insert(page) {
                return Err(Error::Corrupt("sqlite table b-tree page repeated"));
            }
            let page_bytes = self.checked_page(page)?;
            let header_relative = Self::header_offset(page);
            match *page_bytes.get(header_relative).ok_or(Error::Corrupt("sqlite table page header truncated"))? {
                PAGE_TABLE_LEAF => {
                    let count = Self::page_u16(page_bytes, header_relative + 3, "sqlite table page header truncated")?;
                    if count == 0 {
                        let rows = read_table(&self.pages, root, TableLimits::default())?;
                        return Ok(rows.last().map(|row| row.rowid).unwrap_or(0));
                    }
                    let pointer = header_relative + 8 + (count - 1) * 2;
                    let cell_offset = Self::page_u16(page_bytes, pointer, "sqlite table cell pointer truncated")?;
                    if cell_offset >= self.usable {
                        return Err(Error::Corrupt("sqlite table cell pointer out of bounds"));
                    }
                    let tail = page_bytes.get(cell_offset..).ok_or(Error::Corrupt("sqlite table cell pointer out of bounds"))?;
                    let (_, length_size) = decode_varint(tail)?;
                    let rowid_start = cell_offset.checked_add(length_size).ok_or(Error::Corrupt("sqlite table cell offset overflow"))?;
                    let rowid_tail = page_bytes.get(rowid_start..).ok_or(Error::Corrupt("sqlite table rowid truncated"))?;
                    let (rowid, _) = decode_varint(rowid_tail)?;
                    return Ok(rowid as i64);
                }
                PAGE_TABLE_INTERIOR => {
                    page = Self::page_u32(page_bytes, header_relative + 8, "sqlite table child pointer truncated")?;
                }
                _ => return Err(Error::Corrupt("sqlite table b-tree page type invalid")),
            }
        }
    }

    fn contains_rowid(&self, root: u32, rowid: i64) -> Result<bool> {
        Ok(self.find_table_cell(root, rowid)?.is_some())
    }

    /// 沿 rowid b-tree 下降到叶页并定位 `rowid` 对应的 cell,只解析页内
    /// cell 指针与 varint 前缀,不复制 payload。插入位置判定、原地改写与
    /// payload 读取共用这一条下钻路径。
    fn find_table_cell(&self, root: u32, rowid: i64) -> Result<Option<TableCell>> {
        let mut page = root;
        let mut visited = std::collections::BTreeSet::new();
        loop {
            if page == 0 || page > self.page_count() {
                return Err(Error::Corrupt("sqlite table page reference out of bounds"));
            }
            if !visited.insert(page) {
                return Err(Error::Corrupt("sqlite table b-tree page repeated"));
            }
            let page_bytes = self.checked_page(page)?;
            let header_relative = Self::header_offset(page);
            let kind = *page_bytes.get(header_relative).ok_or(Error::Corrupt("sqlite table page header truncated"))?;
            let count = Self::page_u16(page_bytes, header_relative + 3, "sqlite table page header truncated")?;
            match kind {
                PAGE_TABLE_LEAF => {
                    let pointers_end = header_relative + 8 + count * 2;
                    if pointers_end > self.usable {
                        return Err(Error::Corrupt("sqlite table cell pointer array out of bounds"));
                    }
                    for index in 0..count {
                        let pointer = header_relative + 8 + index * 2;
                        let cell_offset = Self::page_u16(page_bytes, pointer, "sqlite table cell pointer truncated")?;
                        if cell_offset >= self.usable {
                            return Err(Error::Corrupt("sqlite table cell pointer out of bounds"));
                        }
                        let tail = page_bytes.get(cell_offset..).ok_or(Error::Corrupt("sqlite table cell pointer out of bounds"))?;
                        let (length, length_size) = decode_varint(tail)?;
                        let rowid_start = cell_offset.checked_add(length_size).ok_or(Error::Corrupt("sqlite table cell offset overflow"))?;
                        let rowid_tail = page_bytes.get(rowid_start..).ok_or(Error::Corrupt("sqlite table rowid truncated"))?;
                        let (candidate, rowid_size) = decode_varint(rowid_tail)?;
                        if candidate as i64 != rowid {
                            continue;
                        }
                        let payload_len = usize::try_from(length).map_err(|_| Error::Unsupported("sqlite payload too large"))?;
                        if payload_len > self.pages.len() {
                            return Err(Error::Corrupt("sqlite cell payload length exceeds image size"));
                        }
                        let local = self.local_payload(payload_len);
                        let prefix = length_size + rowid_size;
                        let span = prefix + local + if payload_len > local { 4 } else { 0 };
                        if cell_offset.checked_add(span).is_none_or(|end| end > self.usable) {
                            return Err(Error::Corrupt("sqlite cell payload truncated"));
                        }
                        return Ok(Some(TableCell { page, rowid, offset: cell_offset, prefix, payload_len, local, span }));
                    }
                    return Ok(None);
                }
                PAGE_TABLE_INTERIOR => {
                    let pointers_end = header_relative + 12 + count * 2;
                    if pointers_end > self.usable {
                        return Err(Error::Corrupt("sqlite table cell pointer array out of bounds"));
                    }
                    let mut child = Self::page_u32(page_bytes, header_relative + 8, "sqlite table child pointer truncated")?;
                    for index in 0..count {
                        let pointer = header_relative + 12 + index * 2;
                        let cell_offset = Self::page_u16(page_bytes, pointer, "sqlite table cell pointer truncated")?;
                        if cell_offset >= self.usable {
                            return Err(Error::Corrupt("sqlite table cell pointer out of bounds"));
                        }
                        let candidate = Self::page_u32(page_bytes, cell_offset, "sqlite table child pointer truncated")?;
                        let key_start = cell_offset.checked_add(4).ok_or(Error::Corrupt("sqlite table cell offset overflow"))?;
                        let key_tail = page_bytes.get(key_start..).ok_or(Error::Corrupt("sqlite table key truncated"))?;
                        let (key, _) = decode_varint(key_tail)?;
                        if rowid <= key as i64 {
                            child = candidate;
                            break;
                        }
                    }
                    page = child;
                }
                _ => return Err(Error::Corrupt("sqlite table b-tree page type invalid")),
            }
        }
    }

    /// 读取 [`TableCell`] 指到的 payload;本地部分与溢出链的拼装规则与
    /// 原来的单遍读取完全一致。
    fn read_cell_payload(&self, cell: &TableCell) -> Result<Vec<u8>> {
        let body_start = cell.offset.checked_add(cell.prefix).ok_or(Error::Corrupt("sqlite table cell offset overflow"))?;
        self.cell_payload_at(cell.page, body_start, cell.payload_len).map(|(payload, _end)| payload)
    }

    fn read_payload(&self, root: u32, rowid: i64) -> Result<Option<Vec<u8>>> {
        let Some(cell) = self.find_table_cell(root, rowid)?
        else {
            return Ok(None);
        };
        self.read_cell_payload(&cell).map(Some)
    }

    /// 当新 payload 编成的 cell 仍装得进旧 cell 的页内跨度时,在原偏移
    /// 就地改写,并把余量归还给页内空闲空间。装不下时返回 `false` 且不改动
    /// 镜像,由调用方回退到 delete + insert。
    ///
    /// 原地路径不改 cell 指针数组、content offset 与原 freeblock 的位置;
    /// 只有新 cell 短于旧跨度时才向 freeblock 链追加余量(不足 4 字节时
    /// 按上游 `pageFindSlot` 语义累加页头 `nFrag`),否则重写后的页在
    /// `integrity_check` 里会报“Fragmentation ... reported as 0”。
    fn replace_cell_in_place(&mut self, cell: &TableCell, payload: &[u8]) -> Result<bool> {
        let local = self.local_payload(payload.len());
        let mut prefix = Vec::with_capacity(10);
        encode_varint_into(&mut prefix, payload.len() as u64);
        encode_varint_into(&mut prefix, cell.rowid as u64);
        let span = prefix.len() + local + if payload.len() > local { 4 } else { 0 };
        if span > cell.span {
            return Ok(false);
        }
        let remainder = cell.span - span;
        let fragment_position = self.page_offset(cell.page) + Self::header_offset(cell.page) + 7;
        let fragments = self.pages[fragment_position] as usize;
        // 1 到 3 字节的余量只能记作碎片。累计将超过 60 字节时退回整行
        // 复写,把余量并入 freeblock,避免页头 nFrag 超出上游上限。
        if remainder > 0 && remainder < 4 && fragments + remainder > 60 {
            return Ok(false);
        }

        self.touch_page(cell.page);
        if cell.payload_len > cell.local {
            let body_start = self.page_offset(cell.page) + cell.offset + cell.prefix;
            let head = u32::from_be_bytes(self.pages[body_start + cell.local..body_start + cell.local + 4].try_into().unwrap());
            self.free_overflow_chain(head);
        }
        // `free_overflow_chain` 与 `write_overflow` 都可能分配/回收页并
        // 触发镜像扩容,因此每次写之前按页号重新计算偏移。
        let mut body = self.page_offset(cell.page) + cell.offset;
        self.pages[body..body + prefix.len()].copy_from_slice(&prefix);
        body += prefix.len();
        self.pages[body..body + local].copy_from_slice(&payload[..local]);
        if payload.len() > local {
            let head = self.write_overflow(payload, local)?;
            let pointer = self.page_offset(cell.page) + cell.offset + prefix.len() + local;
            self.pages[pointer..pointer + 4].copy_from_slice(&head.to_be_bytes());
        }
        if remainder >= 4 {
            self.add_freeblock(cell.page, cell.offset + span, remainder);
        }
        else if remainder > 0 {
            let fragments = self.page_offset(cell.page) + Self::header_offset(cell.page) + 7;
            let current = self.pages[fragments] as usize;
            self.pages[fragments] = (current + remainder) as u8;
        }
        Ok(true)
    }

    fn table_insert(&mut self, page: u32, rowid: i64, payload: &[u8]) -> Result<u32> {
        match self.table_insert_inner(page, rowid, payload)? {
            Some(split) if page == 1 => {
                // Page 1 is pinned as the `sqlite_master` root by the format.
                // A regular split would move the root to a fresh page; instead
                // relocate the split-off first group into a child page and turn
                // page 1 into an interior root, exactly like upstream SQLite.
                let first = self.relocate_page_one()?;
                let mut cells = Vec::with_capacity(split.boundaries.len());
                let mut left = first;
                for (key, right) in split.boundaries.iter() {
                    cells.push(interior_cell(left, *key));
                    left = *right;
                }
                self.rebuild_leaf(1, PAGE_TABLE_INTERIOR, &cells);
                let header_relative = Self::header_offset(1);
                self.pages[header_relative + 8..header_relative + 12].copy_from_slice(&left.to_be_bytes());
                Ok(1)
            }
            Some(split) => {
                let new_root = self.allocate_page()?;
                self.init_leaf_page(new_root, PAGE_TABLE_INTERIOR);
                let mut cells = Vec::with_capacity(split.boundaries.len());
                let mut left = page;
                for (key, right) in split.boundaries.iter() {
                    cells.push(interior_cell(left, *key));
                    left = *right;
                }
                self.rebuild_leaf(new_root, PAGE_TABLE_INTERIOR, &cells);
                let offset = self.page_offset(new_root);
                let header_relative = Self::header_offset(new_root);
                self.pages[offset + header_relative + 8..offset + header_relative + 12].copy_from_slice(&left.to_be_bytes());
                Ok(new_root)
            }
            None => Ok(page),
        }
    }

    /// Moves the current b-tree content of page 1 (the pinned master root) into
    /// a freshly allocated page and returns that page number. The 100-byte
    /// database header stays in place so the caller can rebuild page 1 as an
    /// interior root.
    fn relocate_page_one(&mut self) -> Result<u32> {
        let offset = self.page_offset(1);
        let header_relative = Self::header_offset(1);
        let kind = self.pages[offset + header_relative];
        let (cells, rightmost) = match kind {
            PAGE_TABLE_LEAF => {
                let cells = self.leaf_cells(1)?.into_iter().map(|(_, _, bytes)| bytes).collect::<Vec<_>>();
                (cells, 0)
            }
            PAGE_TABLE_INTERIOR => {
                let count =
                    u16::from_be_bytes(self.pages[offset + header_relative + 3..offset + header_relative + 5].try_into().unwrap()) as usize;
                let mut cells = Vec::with_capacity(count);
                for index in 0..count {
                    let pointer = offset + header_relative + 12 + index * 2;
                    let cell_offset = u16::from_be_bytes(self.pages[pointer..pointer + 2].try_into().unwrap()) as usize;
                    let start = offset + cell_offset;
                    let (_, consumed) = decode_varint(&self.pages[start + 4..])?;
                    cells.push(self.pages[start..start + 4 + consumed].to_vec());
                }
                let rightmost = u32::from_be_bytes(self.pages[offset + header_relative + 8..offset + header_relative + 12].try_into().unwrap());
                (cells, rightmost)
            }
            _ => return Err(Error::Corrupt("sqlite master b-tree page type invalid")),
        };
        let page = self.allocate_page()?;
        self.touch_page(page);
        let dest = self.page_offset(page);
        self.pages[dest..dest + self.page_size].fill(0);
        self.rebuild_leaf(page, kind, &cells);
        if kind == PAGE_TABLE_INTERIOR {
            let header = Self::header_offset(page);
            self.pages[dest + header + 8..dest + header + 12].copy_from_slice(&rightmost.to_be_bytes());
        }
        Ok(page)
    }

    fn table_insert_inner(&mut self, page: u32, rowid: i64, payload: &[u8]) -> Result<Option<Split>> {
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let kind = self.pages[offset + header_relative];
        match kind {
            PAGE_TABLE_LEAF => {
                let rowids = self.leaf_rowids(page)?;
                let position = rowids.partition_point(|candidate| *candidate < rowid);
                if rowids.get(position).is_some_and(|candidate| *candidate == rowid) {
                    // 重复 rowid 在插入下钻到叶子时就判定，不再先做一次独立查找。
                    return Err(Error::Constraint { kind: "unique", message: "sqlite UNIQUE constraint failed" });
                }
                let cell = self.build_cell(PAGE_TABLE_LEAF, Some(rowid), payload)?;
                if let Some(start) = self.take_freeblock(page, cell.len()) {
                    self.place_leaf_cell_at(page, position, &cell, start);
                    return Ok(None);
                }
                if cell.len() + 2 <= self.free_space(page) {
                    self.place_leaf_cell(page, position, &cell);
                    return Ok(None);
                }
                // Split: pack the sorted cells into as many pages as the
                // payload sizes demand (one oversized overflow cell can make a
                // count-based half-page overflow its page). 只有分裂才物化
                // 既有 cell 字节；快路径完全不触碰它们。
                let cells = self.leaf_cells(page)?;
                let mut all: Vec<(i64, Vec<u8>)> = cells.into_iter().map(|(key, _, bytes)| (key, bytes)).collect();
                all.insert(position, (rowid, cell));
                let capacity = self.page_capacity(page);
                let groups = pack_groups(all, capacity, |(_, bytes)| bytes.len() + 2);
                if groups.len() < 2 {
                    // Freeblocks fragmented the free space; one compacted page
                    // holds everything again.
                    let only = groups.into_iter().next().expect("one group");
                    self.rebuild_leaf(page, PAGE_TABLE_LEAF, &only.iter().map(|(_, bytes)| bytes.clone()).collect::<Vec<_>>());
                    return Ok(None);
                }
                let mut iter = groups.into_iter();
                let first = iter.next().expect("at least two groups");
                let mut previous_max = first.last().map(|(key, _)| *key).expect("group keeps its cells");
                self.rebuild_leaf(page, PAGE_TABLE_LEAF, &first.iter().map(|(_, bytes)| bytes.clone()).collect::<Vec<_>>());
                let mut boundaries = Vec::new();
                for group in iter {
                    let new_page = self.allocate_page()?;
                    self.rebuild_leaf(new_page, PAGE_TABLE_LEAF, &group.iter().map(|(_, bytes)| bytes.clone()).collect::<Vec<_>>());
                    boundaries.push((previous_max, new_page));
                    previous_max = group.last().map(|(key, _)| *key).expect("group keeps its cells");
                }
                Ok(Some(Split { boundaries }))
            }
            PAGE_TABLE_INTERIOR => {
                let count =
                    u16::from_be_bytes(self.pages[offset + header_relative + 3..offset + header_relative + 5].try_into().unwrap()) as usize;
                let mut child_index = None;
                let mut child = u32::from_be_bytes(self.pages[offset + header_relative + 8..offset + header_relative + 12].try_into().unwrap());
                for index in 0..count {
                    let pointer = header_relative + 12 + index * 2;
                    let cell_offset = u16::from_be_bytes(self.pages[offset + pointer..offset + pointer + 2].try_into().unwrap()) as usize;
                    let candidate = u32::from_be_bytes(self.pages[offset + cell_offset..offset + cell_offset + 4].try_into().unwrap());
                    let (key, _) = decode_varint(&self.pages[offset + cell_offset + 4..])?;
                    if rowid <= key as i64 {
                        child = candidate;
                        child_index = Some(index);
                        break;
                    }
                }
                let Some(split) = self.table_insert_inner(child, rowid, payload)?
                else {
                    return Ok(None);
                };
                self.interior_accept(page, child_index, child, split.boundaries)
            }
            _ => Err(Error::Corrupt("sqlite table b-tree page type invalid")),
        }
    }

    /// Integrates a child's split boundaries into this interior page (may
    /// split this page in turn). The child keeps every key up to the first
    /// boundary; each boundary page covers the keys between its neighbours.
    fn interior_accept(&mut self, page: u32, child_index: Option<usize>, child: u32, boundaries: Vec<(i64, u32)>) -> Result<Option<Split>> {
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let kind = self.pages[offset + header_relative];
        let count = u16::from_be_bytes(self.pages[offset + header_relative + 3..offset + header_relative + 5].try_into().unwrap()) as usize;

        // Collect existing cells as (child pointer, key, bytes).
        let mut cells: Vec<(u32, i64, Vec<u8>)> = Vec::with_capacity(count + boundaries.len() + 1);
        for index in 0..count {
            let pointer = header_relative + 12 + index * 2;
            let cell_offset = u16::from_be_bytes(self.pages[offset + pointer..offset + pointer + 2].try_into().unwrap()) as usize;
            let candidate = u32::from_be_bytes(self.pages[offset + cell_offset..offset + cell_offset + 4].try_into().unwrap());
            let (key, consumed) = decode_varint(&self.pages[offset + cell_offset + 4..])?;
            let end = cell_offset + 4 + consumed;
            cells.push((candidate, key as i64, self.pages[offset + cell_offset..offset + end].to_vec()));
        }
        let mut rightmost = u32::from_be_bytes(self.pages[offset + header_relative + 8..offset + header_relative + 12].try_into().unwrap());

        let last_page = boundaries.last().expect("split carries at least one boundary").1;
        match child_index {
            Some(index) => {
                let old_key = cells[index].1;
                let first_key = boundaries[0].0;
                cells[index].1 = first_key;
                cells[index].2 = interior_cell(child, first_key);
                let mut insert_at = index + 1;
                for window in boundaries.windows(2) {
                    let (upper_page, upper_key) = (window[0].1, window[1].0);
                    cells.insert(insert_at, (upper_page, upper_key, interior_cell(upper_page, upper_key)));
                    insert_at += 1;
                }
                cells.insert(insert_at, (last_page, old_key, interior_cell(last_page, old_key)));
            }
            None => {
                let first_key = boundaries[0].0;
                cells.push((child, first_key, interior_cell(child, first_key)));
                for window in boundaries.windows(2) {
                    let (upper_page, upper_key) = (window[0].1, window[1].0);
                    cells.push((upper_page, upper_key, interior_cell(upper_page, upper_key)));
                }
                rightmost = last_page;
            }
        }

        let total: usize = cells.iter().map(|(_, _, bytes)| bytes.len() + 2).sum();
        let header_size = 12usize;
        if total + header_size + Self::header_offset(page) <= self.usable {
            // Fits: rewrite the page in place.
            let rebuilt: Vec<Vec<u8>> = cells.iter().map(|(_, _, bytes)| bytes.clone()).collect();
            self.rebuild_leaf(page, kind, &rebuilt);
            self.pages[offset + header_relative + 8..offset + header_relative + 12].copy_from_slice(&rightmost.to_be_bytes());
            return Ok(None);
        }

        // Split this interior page: the middle cell moves up.
        let mid = cells.len() / 2;
        let moved = cells[mid].clone();
        let right_cells: Vec<Vec<u8>> = cells[mid + 1..].iter().map(|(_, _, bytes)| bytes.clone()).collect();
        let right_page = self.allocate_page()?;
        self.rebuild_leaf(right_page, kind, &right_cells);
        let right_offset = self.page_offset(right_page);
        let right_header = Self::header_offset(right_page);
        self.pages[right_offset + right_header + 8..right_offset + right_header + 12].copy_from_slice(&rightmost.to_be_bytes());
        let left_cells: Vec<Vec<u8>> = cells[..mid].iter().map(|(_, _, bytes)| bytes.clone()).collect();
        self.rebuild_leaf(page, kind, &left_cells);
        self.pages[offset + header_relative + 8..offset + header_relative + 12].copy_from_slice(&moved.0.to_be_bytes());
        Ok(Some(Split { boundaries: vec![(moved.1, right_page)] }))
    }

    fn table_delete(&mut self, page: u32, rowid: i64) -> Result<u32> {
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let kind = self.pages[offset + header_relative];
        match kind {
            PAGE_TABLE_LEAF => {
                let rowids = self.leaf_rowids(page)?;
                let position = rowids.partition_point(|candidate| *candidate < rowid);
                if !rowids.get(position).is_some_and(|candidate| *candidate == rowid) {
                    return Err(Error::Unsupported("sqlite rowid not found"));
                };
                self.remove_cell(page, position)?;
                Ok(page)
            }
            PAGE_TABLE_INTERIOR => {
                let count =
                    u16::from_be_bytes(self.pages[offset + header_relative + 3..offset + header_relative + 5].try_into().unwrap()) as usize;
                let mut child_index = None;
                let mut child = u32::from_be_bytes(self.pages[offset + header_relative + 8..offset + header_relative + 12].try_into().unwrap());
                for index in 0..count {
                    let pointer = header_relative + 12 + index * 2;
                    let cell_offset = u16::from_be_bytes(self.pages[offset + pointer..offset + pointer + 2].try_into().unwrap()) as usize;
                    let candidate = u32::from_be_bytes(self.pages[offset + cell_offset..offset + cell_offset + 4].try_into().unwrap());
                    let (key, _) = decode_varint(&self.pages[offset + cell_offset + 4..])?;
                    if rowid <= key as i64 {
                        child = candidate;
                        child_index = Some(index);
                        break;
                    }
                }
                let root = self.table_delete(child, rowid)?;
                if root != child {
                    // Child shrank to nothing and was freed; repoint.
                    self.repoint_child(page, child_index, child, root)?;
                }
                else {
                    self.reclaim_empty_leaf(page, child_index, child)?;
                }
                // Collapse an interior root that lost all cells.
                let offset = self.page_offset(page);
                let header_relative = Self::header_offset(page);
                let count = u16::from_be_bytes(self.pages[offset + header_relative + 3..offset + header_relative + 5].try_into().unwrap());
                if count == 0 && page != 1 {
                    let rightmost =
                        u32::from_be_bytes(self.pages[offset + header_relative + 8..offset + header_relative + 12].try_into().unwrap());
                    self.free_page(page);
                    return Ok(rightmost);
                }
                Ok(page)
            }
            _ => Err(Error::Corrupt("sqlite table b-tree page type invalid")),
        }
    }

    /// Frees a child leaf that lost its last cell and drops the routing entry
    /// in this interior page. A leaf that is the page's only routing stays
    /// attached so the subtree never dangles.
    fn reclaim_empty_leaf(&mut self, page: u32, child_index: Option<usize>, child: u32) -> Result<()> {
        if !self.is_empty_leaf(child)? {
            return Ok(());
        }
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let count = u16::from_be_bytes(self.pages[offset + header_relative + 3..offset + header_relative + 5].try_into().unwrap()) as usize;
        if count == 0 {
            return Ok(());
        }
        self.free_page(child);

        // Rebuild the interior page without the routing entry for `child`.
        let mut cells: Vec<(u32, i64)> = Vec::with_capacity(count);
        for index in 0..count {
            let pointer = header_relative + 12 + index * 2;
            let cell_offset = u16::from_be_bytes(self.pages[offset + pointer..offset + pointer + 2].try_into().unwrap()) as usize;
            let candidate = u32::from_be_bytes(self.pages[offset + cell_offset..offset + cell_offset + 4].try_into().unwrap());
            let (key, _) = decode_varint(&self.pages[offset + cell_offset + 4..])?;
            cells.push((candidate, key as i64));
        }
        let mut rightmost = u32::from_be_bytes(self.pages[offset + header_relative + 8..offset + header_relative + 12].try_into().unwrap());
        match child_index {
            Some(index) => {
                cells.remove(index);
            }
            None => {
                // The freed leaf was the rightmost pointer; the last cell's
                // child takes over the rightmost slot.
                let (last_child, _) = cells.pop().expect("interior page has at least one cell");
                rightmost = last_child;
            }
        }
        let rebuilt: Vec<Vec<u8>> = cells.iter().map(|(child_page, key)| interior_cell(*child_page, *key)).collect();
        self.rebuild_leaf(page, PAGE_TABLE_INTERIOR, &rebuilt);
        self.pages[offset + header_relative + 8..offset + header_relative + 12].copy_from_slice(&rightmost.to_be_bytes());
        Ok(())
    }

    fn is_empty_leaf(&self, page: u32) -> Result<bool> {
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let kind = self.pages[offset + header_relative];
        if kind != PAGE_TABLE_LEAF {
            return Ok(false);
        }
        let count = u16::from_be_bytes(self.pages[offset + header_relative + 3..offset + header_relative + 5].try_into().unwrap());
        Ok(count == 0)
    }

    fn repoint_child(&mut self, page: u32, child_index: Option<usize>, old_child: u32, new_child: u32) -> Result<()> {
        self.touch_page(page);
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        match child_index {
            Some(index) => {
                let pointer = header_relative + 12 + index * 2;
                let cell_offset = u16::from_be_bytes(self.pages[offset + pointer..offset + pointer + 2].try_into().unwrap()) as usize;
                let stored = u32::from_be_bytes(self.pages[offset + cell_offset..offset + cell_offset + 4].try_into().unwrap());
                if stored != old_child {
                    return Err(Error::Corrupt("sqlite interior child pointer mismatch"));
                }
                self.pages[offset + cell_offset..offset + cell_offset + 4].copy_from_slice(&new_child.to_be_bytes());
            }
            None => {
                self.pages[offset + header_relative + 8..offset + header_relative + 12].copy_from_slice(&new_child.to_be_bytes());
            }
        }
        Ok(())
    }

    /// Removes cell `position` from a leaf page, maintaining the freeblock chain.
    fn remove_cell(&mut self, page: u32, position: usize) -> Result<()> {
        self.touch_page(page);
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let kind = self.pages[offset + header_relative];
        let header_size = if matches!(kind, PAGE_TABLE_INTERIOR | PAGE_INDEX_INTERIOR) { 12 } else { 8 };
        let count = u16::from_be_bytes(self.pages[offset + header_relative + 3..offset + header_relative + 5].try_into().unwrap()) as usize;
        if position >= count {
            return Err(Error::Corrupt("sqlite cell position out of range"));
        }

        // Locate the cell span including its overflow pages. Index interior
        // cells carry a 4-byte child pointer before the payload-length varint.
        let pointer = header_relative + header_size + position * 2;
        let cell_offset = u16::from_be_bytes(self.pages[offset + pointer..offset + pointer + 2].try_into().unwrap()) as usize;
        let child_bytes = if kind == PAGE_INDEX_INTERIOR { 4 } else { 0 };
        let (length, length_size) = decode_varint(&self.pages[offset + cell_offset + child_bytes..])?;
        let length = usize::try_from(length).map_err(|_| Error::Unsupported("sqlite payload too large"))?;
        let prefix = child_bytes
            + match kind {
                PAGE_TABLE_LEAF => {
                    let (_, rowid_size) = decode_varint(&self.pages[offset + cell_offset + length_size..])?;
                    length_size + rowid_size
                }
                _ => length_size,
            };
        let local = self.local_payload(length);
        let span = prefix + local + if length > local { 4 } else { 0 };
        if length > local {
            let head = u32::from_be_bytes(
                self.pages[offset + cell_offset + prefix + local..offset + cell_offset + prefix + local + 4].try_into().unwrap(),
            );
            self.free_overflow_chain(head);
        }

        // Drop the pointer entry.
        let pointer_base = offset + header_relative + header_size;
        let mut pointers = Vec::with_capacity(count - 1);
        for index in 0..count {
            if index == position {
                continue;
            }
            let slot = pointer_base + index * 2;
            pointers.push(u16::from_be_bytes(self.pages[slot..slot + 2].try_into().unwrap()));
        }
        for (index, value) in pointers.iter().enumerate() {
            let slot = pointer_base + index * 2;
            self.pages[slot..slot + 2].copy_from_slice(&value.to_be_bytes());
        }
        self.pages[offset + header_relative + 3..offset + header_relative + 5].copy_from_slice(&((count - 1) as u16).to_be_bytes());

        self.add_freeblock(page, cell_offset, span);
        Ok(())
    }

    /// Inserts `[start, start+size)` into the page's ascending freeblock chain,
    /// coalescing with touching neighbours.
    fn add_freeblock(&mut self, page: u32, start: usize, size: usize) {
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let mut first = u16::from_be_bytes(self.pages[offset + header_relative + 1..offset + header_relative + 3].try_into().unwrap()) as usize;

        let mut blocks: Vec<(usize, usize)> = Vec::new();
        let mut cursor = first;
        while cursor != 0 {
            let next = u16::from_be_bytes(self.pages[offset + cursor..offset + cursor + 2].try_into().unwrap()) as usize;
            let block_size = u16::from_be_bytes(self.pages[offset + cursor + 2..offset + cursor + 4].try_into().unwrap()) as usize;
            blocks.push((cursor, block_size));
            cursor = next;
        }
        blocks.push((start, size));
        blocks.sort_unstable_by_key(|(block_start, _)| *block_start);

        let mut coalesced: Vec<(usize, usize)> = Vec::new();
        for block in blocks {
            if let Some(last) = coalesced.last_mut() {
                if last.0 + last.1 >= block.0 {
                    last.1 = (block.0 + block.1).saturating_sub(last.0).max(last.1);
                    continue;
                }
            }
            coalesced.push(block);
        }

        // Rewrite the chain from scratch (blocks are small and few).
        let mut previous = 0usize;
        for (index, (block_start, block_size)) in coalesced.iter().enumerate() {
            let next = if index + 1 < coalesced.len() { coalesced[index + 1].0 } else { 0 };
            self.pages[offset + block_start..offset + block_start + 2].copy_from_slice(&(next as u16).to_be_bytes());
            self.pages[offset + block_start + 2..offset + block_start + 4].copy_from_slice(&(*block_size as u16).to_be_bytes());
            if previous == 0 {
                first = *block_start;
            }
            previous = *block_start;
        }
        if coalesced.is_empty() {
            first = 0;
        }
        self.pages[offset + header_relative + 1..offset + header_relative + 3].copy_from_slice(&(first as u16).to_be_bytes());
    }

    /// Replaces the payload of an existing rowid cell in place when it fits,
    /// otherwise deletes and reinserts.
    fn table_replace(&mut self, root: u32, rowid: i64, payload: &[u8]) -> Result<u32> {
        self.table_delete(root, rowid)?;
        self.table_insert(root, rowid, payload)
    }

    // ------------------------------------------------------------------
    // Index b-tree
    // ------------------------------------------------------------------

    /// Index pages route strictly: keys below a cell key descend into that
    /// cell's child, keys at or above continue right. The divider of a split
    /// is a real entry that MOVES up to the parent and leaves both halves,
    /// exactly like upstream SQLite.
    fn index_insert(&mut self, root: u32, key: &[u8], collations: &[Collation]) -> Result<u32> {
        match self.index_insert_inner(root, key, collations)? {
            Some(split) => {
                let new_root = self.allocate_page()?;
                self.init_leaf_page(new_root, PAGE_INDEX_INTERIOR);
                let mut cells = Vec::with_capacity(split.dividers.len());
                let mut left = root;
                for (divider, page) in split.dividers.iter().zip(split.pages.iter()) {
                    cells.push(index_interior_cell(left, divider));
                    left = *page;
                }
                self.rebuild_leaf(new_root, PAGE_INDEX_INTERIOR, &cells);
                let offset = self.page_offset(new_root);
                let header_relative = Self::header_offset(new_root);
                self.pages[offset + header_relative + 8..offset + header_relative + 12].copy_from_slice(&left.to_be_bytes());
                Ok(new_root)
            }
            None => Ok(root),
        }
    }

    fn index_insert_inner(&mut self, page: u32, key: &[u8], collations: &[Collation]) -> Result<Option<IndexSplit>> {
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let kind = self.pages[offset + header_relative];
        match kind {
            PAGE_INDEX_LEAF => {
                let entries = self.index_leaf_entries(page)?;
                let position = entries.partition_point(|(payload, _)| compare_index_payloads(payload, key, self.encoding, collations).is_lt());
                if entries.get(position).is_some_and(|(payload, _)| payload.as_slice() == key) {
                    return Err(Error::Unsupported("sqlite duplicate index key"));
                }
                let cell = self.build_cell(PAGE_INDEX_LEAF, None, key)?;
                if cell.len() + 2 <= self.free_space(page) {
                    self.place_leaf_cell(page, position, &cell);
                    return Ok(None);
                }
                // Pack into as many pages as the payload sizes demand, then
                // move one divider entry per boundary up to the parent.
                let mut all: Vec<Vec<u8>> = entries.into_iter().map(|(_, bytes)| bytes).collect();
                all.insert(position, cell);
                let capacity = self.page_capacity(page);
                let mut groups = pack_groups(all, capacity, |bytes| bytes.len() + 2);
                if groups.len() < 2 {
                    // Freeblocks fragmented the free space; one compacted page
                    // holds everything again.
                    let only = groups.into_iter().next().expect("one group");
                    self.rebuild_leaf(page, PAGE_INDEX_LEAF, &only);
                    return Ok(None);
                }
                let mut dividers = Vec::with_capacity(groups.len() - 1);
                for boundary in 1..groups.len() {
                    let divider_cell = if groups[boundary].len() >= 2 {
                        groups[boundary].remove(0)
                    }
                    else if groups[boundary - 1].len() >= 2 {
                        groups[boundary - 1].pop().expect("checked length")
                    }
                    else {
                        return Err(Error::Unsupported("sqlite index cell too large to split"));
                    };
                    dividers.push(decode_cell_payload(&divider_cell)?);
                }
                let mut pages = Vec::with_capacity(groups.len() - 1);
                let mut iter = groups.into_iter();
                let first = iter.next().expect("at least two groups");
                for group in iter {
                    let new_page = self.allocate_page()?;
                    self.rebuild_leaf(new_page, PAGE_INDEX_LEAF, &group);
                    pages.push(new_page);
                }
                self.rebuild_leaf(page, PAGE_INDEX_LEAF, &first);
                Ok(Some(IndexSplit { dividers, pages }))
            }
            PAGE_INDEX_INTERIOR => {
                let entries = self.index_interior_entries(page)?;
                let mut child_index = None;
                let mut child = entries.rightmost;
                for (index, (payload, child_page)) in entries.cells.iter().enumerate() {
                    match compare_index_payloads(key, payload, self.encoding, collations) {
                        std::cmp::Ordering::Less => {
                            child = *child_page;
                            child_index = Some(index);
                            break;
                        }
                        std::cmp::Ordering::Equal => {
                            return Err(Error::Unsupported("sqlite duplicate index key"));
                        }
                        std::cmp::Ordering::Greater => continue,
                    }
                }
                let Some(split) = self.index_insert_inner(child, key, collations)?
                else {
                    return Ok(None);
                };
                self.index_interior_accept(page, child_index, child, split)
            }
            _ => Err(Error::Corrupt("sqlite index b-tree page type invalid")),
        }
    }

    /// Integrates a child split into this interior index page. Divider entries
    /// move up into this page; each group page covers the keys between its
    /// neighbouring dividers.
    fn index_interior_accept(&mut self, page: u32, child_index: Option<usize>, child: u32, split: IndexSplit) -> Result<Option<IndexSplit>> {
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let entries = self.index_interior_entries(page)?;
        let mut pairs: Vec<(Vec<u8>, u32)> = entries.cells;
        let mut rightmost = entries.rightmost;
        let last_page = *split.pages.last().expect("split carries at least one page");
        match child_index {
            Some(index) => {
                let old_payload = pairs[index].0.clone();
                pairs[index] = (split.dividers[0].clone(), child);
                let mut insert_at = index + 1;
                for (window_index, window) in split.dividers.windows(2).enumerate() {
                    pairs.insert(insert_at, (window[1].clone(), split.pages[window_index + 1]));
                    insert_at += 1;
                }
                pairs.insert(insert_at, (old_payload, last_page));
            }
            None => {
                pairs.push((split.dividers[0].clone(), child));
                for (window_index, window) in split.dividers.windows(2).enumerate() {
                    pairs.push((window[1].clone(), split.pages[window_index + 1]));
                }
                rightmost = last_page;
            }
        }
        let cells: Vec<Vec<u8>> = pairs.iter().map(|(payload, child_page)| index_interior_cell(*child_page, payload)).collect();
        let total: usize = cells.iter().map(|bytes| bytes.len() + 2).sum();
        if total + 12 <= self.usable {
            self.rebuild_leaf(page, PAGE_INDEX_INTERIOR, &cells);
            self.pages[offset + header_relative + 8..offset + header_relative + 12].copy_from_slice(&rightmost.to_be_bytes());
            return Ok(None);
        }
        // Split this interior page: the middle cell (an entry itself) moves up.
        let mid = cells.len() / 2;
        let moved_payload = pairs[mid].0.clone();
        let moved_child = pairs[mid].1;
        let right_pairs: Vec<(Vec<u8>, u32)> = pairs[mid + 1..].to_vec();
        let right_cells: Vec<Vec<u8>> = right_pairs.iter().map(|(payload, child_page)| index_interior_cell(*child_page, payload)).collect();
        let right_page = self.allocate_page()?;
        self.rebuild_leaf(right_page, PAGE_INDEX_INTERIOR, &right_cells);
        let right_offset = self.page_offset(right_page);
        let right_header = Self::header_offset(right_page);
        self.pages[right_offset + right_header + 8..right_offset + right_header + 12].copy_from_slice(&rightmost.to_be_bytes());
        let left_cells: Vec<Vec<u8>> = cells[..mid].to_vec();
        self.rebuild_leaf(page, PAGE_INDEX_INTERIOR, &left_cells);
        self.pages[offset + header_relative + 8..offset + header_relative + 12].copy_from_slice(&moved_child.to_be_bytes());
        Ok(Some(IndexSplit { dividers: vec![moved_payload], pages: vec![right_page] }))
    }

    /// Reads the payload of one cell whose local part starts at `body_start`.
    ///
    /// Every offset and overflow page number comes from the file, so all of
    /// them are bounds-checked and the overflow chain is cycle-checked. The
    /// returned offset is the first byte after the cell (including the overflow
    /// pointer when the payload spills).
    fn cell_payload_at(&self, page: u32, body_start: usize, length: usize) -> Result<(Vec<u8>, usize)> {
        if length > self.pages.len() {
            return Err(Error::Corrupt("sqlite cell payload length exceeds image size"));
        }
        let page_bytes = self.checked_page(page)?;
        let local = self.local_payload(length);
        let end = body_start.checked_add(local).filter(|end| *end <= self.usable).ok_or(Error::Corrupt("sqlite cell payload truncated"))?;
        let mut payload = page_bytes[body_start..end].to_vec();
        if length == local {
            return Ok((payload, end));
        }
        let pointer_end = end.checked_add(4).filter(|end| *end <= self.usable).ok_or(Error::Corrupt("sqlite overflow pointer truncated"))?;
        let mut next = Self::page_u32(page_bytes, end, "sqlite overflow pointer truncated")?;
        let mut visited = std::collections::BTreeSet::new();
        while payload.len() < length {
            if next == 0 || next > self.page_count() {
                return Err(Error::Corrupt("sqlite overflow page reference out of bounds"));
            }
            if !visited.insert(next) {
                return Err(Error::Corrupt("sqlite overflow chain repeats a page"));
            }
            let overflow = self.checked_page(next)?;
            let chunk = (length - payload.len()).min(self.usable - 4);
            let bytes = overflow.get(4..4 + chunk).ok_or(Error::Corrupt("sqlite overflow page truncated"))?;
            payload.extend_from_slice(bytes);
            next = Self::page_u32(overflow, 0, "sqlite overflow pointer truncated")?;
        }
        if next != 0 {
            return Err(Error::Corrupt("sqlite overflow chain exceeds payload"));
        }
        Ok((payload, pointer_end))
    }

    fn index_leaf_entries(&self, page: u32) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let page_bytes = self.checked_page(page)?;
        let header_relative = Self::header_offset(page);
        let count = Self::page_u16(page_bytes, header_relative + 3, "sqlite index page header truncated")?;
        let pointers_end = header_relative + 8 + count * 2;
        if pointers_end > self.usable {
            return Err(Error::Corrupt("sqlite index cell pointer array out of bounds"));
        }
        let raw_start = Self::page_u16(page_bytes, header_relative + 5, "sqlite index page header truncated")?;
        let content_start = if raw_start == 0 { 65_536 } else { raw_start };
        if content_start < pointers_end || content_start > self.usable {
            return Err(Error::Corrupt("sqlite index content boundary invalid"));
        }
        let mut out = Vec::with_capacity(count);
        for index in 0..count {
            let pointer = header_relative + 8 + index * 2;
            let cell_offset = Self::page_u16(page_bytes, pointer, "sqlite index cell pointer truncated")?;
            if cell_offset < content_start || cell_offset >= self.usable {
                return Err(Error::Corrupt("sqlite index cell pointer out of bounds"));
            }
            let tail = page_bytes.get(cell_offset..).ok_or(Error::Corrupt("sqlite index cell pointer out of bounds"))?;
            let (length, length_size) = decode_varint(tail)?;
            let length = usize::try_from(length).map_err(|_| Error::Unsupported("sqlite payload too large"))?;
            let body_start = cell_offset.checked_add(length_size).ok_or(Error::Corrupt("sqlite index cell offset overflow"))?;
            let (payload, end) = self.cell_payload_at(page, body_start, length)?;
            out.push((payload, page_bytes[cell_offset..end].to_vec()));
        }
        Ok(out)
    }

    fn index_interior_entries(&self, page: u32) -> Result<InteriorEntries> {
        let page_bytes = self.checked_page(page)?;
        let header_relative = Self::header_offset(page);
        let count = Self::page_u16(page_bytes, header_relative + 3, "sqlite index page header truncated")?;
        let pointers_end = header_relative + 12 + count * 2;
        if pointers_end > self.usable {
            return Err(Error::Corrupt("sqlite index cell pointer array out of bounds"));
        }
        let raw_start = Self::page_u16(page_bytes, header_relative + 5, "sqlite index page header truncated")?;
        let content_start = if raw_start == 0 { 65_536 } else { raw_start };
        if content_start < pointers_end || content_start > self.usable {
            return Err(Error::Corrupt("sqlite index content boundary invalid"));
        }
        let mut cells = Vec::with_capacity(count);
        for index in 0..count {
            let pointer = header_relative + 12 + index * 2;
            let cell_offset = Self::page_u16(page_bytes, pointer, "sqlite index cell pointer truncated")?;
            if cell_offset < content_start || cell_offset >= self.usable {
                return Err(Error::Corrupt("sqlite index cell pointer out of bounds"));
            }
            let child_page = Self::page_u32(page_bytes, cell_offset, "sqlite index child pointer truncated")?;
            let tail_start = cell_offset.checked_add(4).ok_or(Error::Corrupt("sqlite index cell offset overflow"))?;
            let tail = page_bytes.get(tail_start..).ok_or(Error::Corrupt("sqlite index cell pointer out of bounds"))?;
            let (length, length_size) = decode_varint(tail)?;
            let length = usize::try_from(length).map_err(|_| Error::Unsupported("sqlite payload too large"))?;
            let body_start = tail_start.checked_add(length_size).ok_or(Error::Corrupt("sqlite index cell offset overflow"))?;
            let (payload, _end) = self.cell_payload_at(page, body_start, length)?;
            cells.push((payload, child_page));
        }
        let rightmost = Self::page_u32(page_bytes, header_relative + 8, "sqlite index rightmost pointer truncated")?;
        Ok(InteriorEntries { cells, rightmost })
    }

    /// UNIQUE constraint probe against the live key set of one index.
    fn set_prefix_match(
        &self,
        master_rowid: i64,
        key: &[u8],
        prefix_columns: usize,
        excluded_rowid: Option<i64>,
        collations: &[Collation],
    ) -> Result<bool> {
        let wanted = decode_record(key, self.encoding, Default::default()).map_err(|_| Error::Corrupt("sqlite index record decode failed"))?;
        let Some(entries) = self.index_entries.get(&master_rowid)
        else {
            return Ok(false);
        };
        for payload in entries {
            if self.prefix_matches(payload, &wanted, prefix_columns, excluded_rowid, collations)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Collects every entry of an index b-tree into an ordered set. Used when
    /// opening an existing file: the tree becomes the live key set and is only
    /// materialized back into pages on save.
    fn read_index_entries(&self, root: u32) -> Result<std::collections::BTreeSet<Vec<u8>>> {
        let mut out = std::collections::BTreeSet::new();
        let mut pending = vec![root];
        let mut visited = std::collections::BTreeSet::new();
        while let Some(page) = pending.pop() {
            if page == 0 || page > self.page_count() {
                return Err(Error::Corrupt("sqlite index page reference out of bounds"));
            }
            if !visited.insert(page) {
                return Err(Error::Corrupt("sqlite index b-tree page repeated"));
            }
            let page_bytes = self.checked_page(page)?;
            let header_relative = Self::header_offset(page);
            let kind = *page_bytes.get(header_relative).ok_or(Error::Corrupt("sqlite index page header truncated"))?;
            match kind {
                PAGE_INDEX_LEAF => {
                    for (payload, _) in self.index_leaf_entries(page)? {
                        out.insert(payload);
                    }
                }
                PAGE_INDEX_INTERIOR => {
                    let entries = self.index_interior_entries(page)?;
                    for (payload, child_page) in entries.cells {
                        out.insert(payload);
                        pending.push(child_page);
                    }
                    pending.push(entries.rightmost);
                }
                _ => return Err(Error::Corrupt("sqlite index b-tree page type invalid")),
            }
        }
        Ok(out)
    }

    /// Frees every page of an index subtree, including overflow chains.
    fn free_subtree(&mut self, page: u32) -> Result<()> {
        let offset = self.page_offset(page);
        let header_relative = Self::header_offset(page);
        let kind = self.pages[offset + header_relative];
        match kind {
            PAGE_INDEX_LEAF => {
                for (_, cell) in self.index_leaf_entries(page)? {
                    let (length, length_size) = decode_varint(&cell)?;
                    let length = usize::try_from(length).map_err(|_| Error::Unsupported("sqlite payload too large"))?;
                    let local = self.local_payload(length);
                    if length > local {
                        let head = u32::from_be_bytes(cell[length_size + local..length_size + local + 4].try_into().unwrap());
                        self.free_overflow_chain(head);
                    }
                }
                self.free_page(page);
                Ok(())
            }
            PAGE_INDEX_INTERIOR => {
                let entries = self.index_interior_entries(page)?;
                for (_, child_page) in &entries.cells {
                    self.free_subtree(*child_page)?;
                }
                self.free_subtree(entries.rightmost)?;
                self.free_page(page);
                Ok(())
            }
            _ => Err(Error::Corrupt("sqlite index b-tree page type invalid")),
        }
    }

    /// Rebuilds every WITHOUT ROWID primary-key b-tree from its live record set.
    /// Callers that expose [`SqliteWriter::pages`] without saving use this after
    /// row mutation, the same way [`SqliteWriter::materialize_indexes`] keeps
    /// secondary index pages current.
    pub fn materialize_without_rowid(&mut self) -> Result<()> {
        let rows = self.master_rows()?;
        for row in rows.iter().filter(|row| row.kind == "table" && row.sql.as_deref().is_some_and(is_without_rowid)) {
            let entries = self.without_rowid_entries.get(&row.rowid).cloned().unwrap_or_default();
            let meta = self.load_table_meta(&row.table_name)?;
            let collations: Vec<Collation> = meta.without_rowid_layout[..meta.primary_key_len]
                .iter()
                .map(|position| meta.columns[*position].collate.unwrap_or_default())
                .collect();
            if row.root_page != 0 {
                self.free_subtree(row.root_page)?;
            }
            let mut root = self.allocate_page()?;
            self.init_leaf_page(root, PAGE_INDEX_LEAF);
            for key in &entries {
                root = self.index_insert(root, key, &collations)?;
            }
            if root != row.root_page {
                self.set_master_root(row.rowid, root)?;
            }
        }
        Ok(())
    }

    /// Rebuilds every index b-tree from the live key set.
    ///
    /// In-memory callers that expose [`SqliteWriter::pages`] without saving to a
    /// file use this after row mutation so index pages always match the table
    /// pages they index.
    pub fn materialize_indexes(&mut self) -> Result<()> {
        let rows = self.master_rows()?;
        for row in rows.iter().filter(|row| row.kind == "index") {
            let entries = self.index_entries.get(&row.rowid).cloned().unwrap_or_default();
            let meta = self.load_table_meta(&row.table_name)?;
            let collations =
                meta.indexes.iter().find(|index| index.master_rowid == row.rowid).map(|index| index.collations.clone()).unwrap_or_default();
            if row.root_page != 0 {
                self.free_subtree(row.root_page)?;
            }
            let mut root = self.allocate_page()?;
            self.init_leaf_page(root, PAGE_INDEX_LEAF);
            for key in &entries {
                root = self.index_insert(root, key, &collations)?;
            }
            if root != row.root_page {
                self.set_master_root(row.rowid, root)?;
            }
        }
        Ok(())
    }

    fn prefix_matches(
        &self,
        payload: &[u8],
        wanted: &[database_formats::sqlite::RecordValue<'_>],
        prefix_columns: usize,
        excluded_rowid: Option<i64>,
        collations: &[Collation],
    ) -> Result<bool> {
        let candidate =
            decode_record(payload, self.encoding, Default::default()).map_err(|_| Error::Corrupt("sqlite index record decode failed"))?;
        if !prefix_equal(&candidate, wanted, prefix_columns, collations) {
            return Ok(false);
        }
        if let Some(excluded) = excluded_rowid {
            if candidate
                .last()
                .is_some_and(|value| matches!(value, database_formats::sqlite::RecordValue::Integer(rowid) if *rowid == excluded))
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

struct CellInfo {
    rowid: i64,
    prefix: usize,
    length: usize,
}

/// 表叶页上一个 cell 的定位信息:所在页、页内起始偏移、payload 之前的
/// 前缀长度(长度 varint + rowid varint)、payload 长度、本地字节数,以及
/// cell 在页内占用的总跨度(含溢出页指针)。
struct TableCell {
    page: u32,
    rowid: i64,
    offset: usize,
    prefix: usize,
    payload_len: usize,
    local: usize,
    span: usize,
}

struct IndexSplit {
    /// Divider entries moved up to the parent, between consecutive groups.
    dividers: Vec<Vec<u8>>,
    /// Pages holding the groups after the first, aligned with `dividers`.
    pages: Vec<u32>,
}

struct InteriorEntries {
    cells: Vec<(Vec<u8>, u32)>,
    rightmost: u32,
}

fn table_name_of(meta: &TableMeta) -> String {
    meta.table_name.clone()
}

fn interior_cell(child: u32, key: i64) -> Vec<u8> {
    let mut cell = Vec::with_capacity(16);
    cell.extend_from_slice(&child.to_be_bytes());
    encode_varint_into(&mut cell, key as u64);
    cell
}

fn index_interior_cell(child: u32, payload: &[u8]) -> Vec<u8> {
    let mut cell = Vec::with_capacity(payload.len() + 8);
    cell.extend_from_slice(&child.to_be_bytes());
    encode_varint_into(&mut cell, payload.len() as u64);
    cell.extend_from_slice(payload);
    cell
}

fn decode_cell_payload(cell: &[u8]) -> Result<Vec<u8>> {
    let (length, length_size) = decode_varint(cell)?;
    let length = usize::try_from(length).map_err(|_| Error::Unsupported("sqlite payload too large"))?;
    Ok(cell[length_size..length_size + length].to_vec())
}

fn compare_index_payloads(left: &[u8], right: &[u8], encoding: TextEncoding, collations: &[Collation]) -> std::cmp::Ordering {
    crate::sqlite::table::compare_records_with(left, right, encoding, collations).unwrap_or(std::cmp::Ordering::Equal)
}

fn compare_record_values(
    a: &database_formats::sqlite::RecordValue<'_>,
    b: &database_formats::sqlite::RecordValue<'_>,
    collation: Collation,
) -> std::cmp::Ordering {
    use database_formats::sqlite::RecordValue as V;
    let class = |value: &V| match value {
        V::Null => 0,
        V::Integer(_) | V::Real(_) => 1,
        V::Text(_) => 2,
        V::Blob(_) => 3,
    };
    match (class(a), class(b)) {
        (0, 0) => std::cmp::Ordering::Equal,
        (1, 1) => compare_numeric(a, b),
        (2, 2) => {
            let (V::Text(left), V::Text(right)) = (a, b)
            else {
                unreachable!()
            };
            collation.compare_text(left, right)
        }
        (3, 3) => {
            let (V::Blob(left), V::Blob(right)) = (a, b)
            else {
                unreachable!()
            };
            left.cmp(right)
        }
        (left, right) => left.cmp(&right),
    }
}

fn compare_numeric(a: &database_formats::sqlite::RecordValue<'_>, b: &database_formats::sqlite::RecordValue<'_>) -> std::cmp::Ordering {
    use database_formats::sqlite::RecordValue as V;
    let to_f64 = |value: &V| match value {
        V::Integer(number) => *number as f64,
        V::Real(real) => *real,
        _ => unreachable!(),
    };
    let (left, right) = (to_f64(a), to_f64(b));
    if left.is_nan() || right.is_nan() {
        return left.total_cmp(&right);
    }
    match (a, b) {
        (V::Integer(left_int), V::Real(right_real)) => exact_int_real(*left_int, *right_real),
        (V::Real(left_real), V::Integer(right_int)) => exact_int_real(*right_int, *left_real).reverse(),
        _ => left.total_cmp(&right),
    }
}

fn exact_int_real(integer: i64, real: f64) -> std::cmp::Ordering {
    if real.is_infinite() {
        return if real.is_sign_positive() { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater };
    }
    let truncated = real.trunc();
    if truncated == real && truncated.abs() < 9.223372036854776e18 {
        return integer.cmp(&(truncated as i64));
    }
    (integer as f64).total_cmp(&real)
}

fn prefix_equal(
    candidate: &[database_formats::sqlite::RecordValue<'_>],
    wanted: &[database_formats::sqlite::RecordValue<'_>],
    prefix_columns: usize,
    collations: &[Collation],
) -> bool {
    if candidate.len() < prefix_columns || wanted.len() < prefix_columns {
        return false;
    }
    candidate[..prefix_columns]
        .iter()
        .zip(wanted[..prefix_columns].iter())
        .enumerate()
        .all(|(position, (left, right))| compare_record_values(left, right, collations.get(position).copied().unwrap_or_default()).is_eq())
}

fn is_rowid_alias(column: &ColumnSpec) -> bool {
    column.primary_key && column.declared_type.eq_ignore_ascii_case("INTEGER")
}

/// SQLite column affinity derived from the declared type (fileformat2 §3.1).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Affinity {
    /// `INT`-shaped declared types.
    Integer,
    /// `CHAR` / `CLOB` / `TEXT` declared types.
    Text,
    /// `BLOB` or an empty declared type.
    Blob,
    /// `REAL` / `FLOA` / `DOUB` declared types.
    Real,
    /// Everything else.
    Numeric,
}

/// Affinity a column with this declared type text takes for stored values.
pub fn affinity_of(declared: &str) -> Affinity {
    let upper = declared.to_ascii_uppercase();
    if upper.contains("INT") {
        Affinity::Integer
    }
    else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        Affinity::Text
    }
    else if upper.contains("BLOB") || upper.trim().is_empty() {
        Affinity::Blob
    }
    else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        Affinity::Real
    }
    else {
        Affinity::Numeric
    }
}

/// Applies the affinity SQLite would apply to a column value before the
/// constraints of that row are evaluated.
///
/// The engine evaluates `CHECK` constraints on the converted value, which is the
/// same order upstream uses, so callers outside this crate need the conversion.
pub fn apply_write_affinity(declared_type: &str, value: WriteValue) -> WriteValue {
    apply_affinity(value, affinity_of(declared_type))
}

/// Applies SQLite affinity conversion to a value before storage.
fn apply_affinity(value: WriteValue, affinity: Affinity) -> WriteValue {
    match affinity {
        Affinity::Blob => value,
        Affinity::Real => match value {
            WriteValue::Integer(number) => WriteValue::Real(number as f64),
            // REAL 亲和性先按 NUMERIC 规则解释文本字面量，再把整数结果提升为浮点。
            WriteValue::Text(text) => match convert_text_numeric(&text, Affinity::Numeric) {
                WriteValue::Integer(number) => WriteValue::Real(number as f64),
                other => other,
            },
            other => other,
        },
        Affinity::Text => match value {
            WriteValue::Integer(number) => WriteValue::Text(number.to_string()),
            WriteValue::Real(real) => WriteValue::Text(render_real_as_text(real)),
            other => other,
        },
        Affinity::Numeric | Affinity::Integer => match value {
            WriteValue::Real(real) => match lossless_integer(real) {
                Some(integer) => WriteValue::Integer(integer),
                None => WriteValue::Real(real),
            },
            WriteValue::Text(text) => convert_text_numeric(&text, affinity),
            other => other,
        },
    }
}

fn lossless_integer(real: f64) -> Option<i64> {
    if !real.is_finite() || real.trunc() != real {
        return None;
    }
    if real.abs() >= 9.223372036854776e18 {
        return None;
    }
    let integer = real as i64;
    if (integer as f64) == real { Some(integer) } else { None }
}

fn convert_text_numeric(text: &str, affinity: Affinity) -> WriteValue {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return WriteValue::Text(text.to_string());
    }
    if let Ok(integer) = trimmed.parse::<i64>() {
        return WriteValue::Integer(integer);
    }
    if affinity == Affinity::Integer {
        return WriteValue::Text(text.to_string());
    }
    match trimmed.parse::<f64>() {
        Ok(real) if trimmed.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E')) => match lossless_integer(real) {
            Some(integer) => WriteValue::Integer(integer),
            None => WriteValue::Real(real),
        },
        _ => WriteValue::Text(text.to_string()),
    }
}

fn render_real_as_text(real: f64) -> String {
    if real.is_infinite() || real.is_nan() {
        return format!("{real}");
    }
    let mut text = format!("{real:.15e}");
    // %.15g trims trailing zeros of the mantissa; mirror that.
    if let Some(exponent_position) = text.find('e') {
        let (mantissa, exponent) = text.split_at(exponent_position);
        let mut mantissa = mantissa.trim_end_matches('0').trim_end_matches('.').to_string();
        if mantissa.is_empty() || mantissa == "-" {
            mantissa.push('0');
        }
        let exponent_value: i32 = exponent[1..].parse().unwrap_or(0);
        text = format!("{mantissa}e{exponent_value:+}");
    }
    text
}

fn eq_ignore_ascii_case(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

/// Keeps the `seq` column value when renaming a `sqlite_sequence` row.
fn sequence_value(value: Option<&database_formats::sqlite::RecordValue<'_>>) -> WriteValue {
    match value {
        Some(database_formats::sqlite::RecordValue::Integer(number)) => WriteValue::Integer(*number),
        Some(database_formats::sqlite::RecordValue::Real(real)) => WriteValue::Real(*real),
        Some(database_formats::sqlite::RecordValue::Text(text)) => WriteValue::Text(text.clone()),
        Some(database_formats::sqlite::RecordValue::Blob(bytes)) => WriteValue::Blob(bytes.to_vec()),
        _ => WriteValue::Null,
    }
}

fn has_autoincrement(sql: &str) -> bool {
    parse_create_table(sql).map(|columns| columns.iter().any(|column| column.autoincrement)).unwrap_or(false)
}

/// `sqlite_autoindex_<table>_<n>` follows the table name when it is renamed.
fn renamed_autoindex(name: &str, table: &str) -> String {
    let suffix = name.rsplit('_').next().unwrap_or("1");
    format!("sqlite_autoindex_{table}_{suffix}")
}

fn expand_alias(meta: &TableMeta, rowid: i64, values: &[database_formats::sqlite::RecordValue<'_>]) -> Vec<WriteValue> {
    let mut owned: Vec<WriteValue> = values
        .iter()
        .map(|value| match value {
            database_formats::sqlite::RecordValue::Null => WriteValue::Null,
            database_formats::sqlite::RecordValue::Integer(number) => WriteValue::Integer(*number),
            database_formats::sqlite::RecordValue::Real(real) => WriteValue::Real(*real),
            database_formats::sqlite::RecordValue::Blob(bytes) => WriteValue::Blob(bytes.to_vec()),
            database_formats::sqlite::RecordValue::Text(text) => WriteValue::Text(text.clone()),
        })
        .collect();
    // A record written before `ALTER TABLE ADD COLUMN` is short; SQLite reads
    // the missing trailing columns as their declared default.
    while owned.len() < meta.columns.len() {
        let column = &meta.columns[owned.len()];
        owned.push(match &column.default_value {
            Some(value) => apply_write_affinity(&column.declared_type, value.clone()),
            None => WriteValue::Null,
        });
    }
    if let Some(index) = meta.rowid_alias {
        if index < owned.len() {
            owned[index] = WriteValue::Integer(rowid);
        }
    }
    owned
}

/// SQLite lets every row whose unique key contains a `NULL` coexist, because
/// `NULL` never compares equal to anything.
fn index_key_has_no_null(values: &[WriteValue], columns: &[usize]) -> bool {
    columns.iter().all(|column| !matches!(values.get(*column), Some(WriteValue::Null)))
}

fn encode_index_key(meta: &TableMeta, index: &IndexMeta, rowid: i64, values: &[WriteValue], encoding: TextEncoding) -> Result<Vec<u8>> {
    let mut key: Vec<WriteValue> = index.columns.iter().map(|column| values[*column].clone()).collect();
    let _ = meta;
    key.push(WriteValue::Integer(rowid));
    Ok(encode_record(&key, encoding))
}

fn render_create_table(name: &str, columns: &[ColumnSpec], primary_key: &[String]) -> String {
    let mut parts = Vec::with_capacity(columns.len() + 1);
    for column in columns {
        let mut fragment = render_column_head(&column.name, &column.declared_type);
        if let Some(collate) = column.collate {
            fragment.push_str(&format!(" COLLATE {}", collate.name()));
        }
        if column.primary_key {
            fragment.push_str(" PRIMARY KEY");
            if column.autoincrement {
                fragment.push_str(" AUTOINCREMENT");
            }
        }
        if column.not_null {
            fragment.push_str(" NOT NULL");
        }
        if let Some(default) = &column.default_value {
            fragment.push_str(&format!(" DEFAULT {}", render_literal(default)));
        }
        if column.unique && !column.primary_key {
            fragment.push_str(" UNIQUE");
        }
        parts.push(fragment);
    }
    if !primary_key.is_empty() {
        let key_list = primary_key.iter().map(|column| quote_identifier(column)).collect::<Vec<_>>().join(", ");
        parts.push(format!("PRIMARY KEY ({key_list})"));
    }
    format!("CREATE TABLE {} ({})", quote_identifier(name), parts.join(", "))
}

fn render_create_table_without_rowid(name: &str, columns: &[ColumnSpec], primary_key: &[String]) -> String {
    let mut parts = Vec::with_capacity(columns.len() + 1);
    for column in columns {
        let mut fragment = render_column_head(&column.name, &column.declared_type);
        if let Some(collate) = column.collate {
            fragment.push_str(&format!(" COLLATE {}", collate.name()));
        }
        if column.not_null {
            fragment.push_str(" NOT NULL");
        }
        if let Some(default) = &column.default_value {
            fragment.push_str(&format!(" DEFAULT {}", render_literal(default)));
        }
        parts.push(fragment);
    }
    let key_list = primary_key.iter().map(|column| quote_identifier(column)).collect::<Vec<_>>().join(", ");
    parts.push(format!("PRIMARY KEY ({key_list})"));
    format!("CREATE TABLE {} ({}) WITHOUT ROWID", quote_identifier(name), parts.join(", "))
}

/// Returns the table-level `PRIMARY KEY (...)` columns, if the schema has one.
fn find_primary_key_columns(sql: &str) -> Result<Option<Vec<String>>> {
    let open = sql.find('(').ok_or(Error::Corrupt("sqlite CREATE TABLE has no column list"))?;
    for fragment in split_top_level(&sql[open + 1..]) {
        let fragment = fragment.trim();
        if !fragment.to_ascii_uppercase().starts_with("PRIMARY") {
            continue;
        }
        let inner_open = fragment.find('(').ok_or(Error::Corrupt("sqlite PRIMARY KEY clause malformed"))?;
        let mut depth = 0usize;
        let mut close = None;
        for (offset, character) in fragment[inner_open..].char_indices() {
            match character {
                '(' => depth += 1,
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        close = Some(inner_open + offset);
                        break;
                    }
                }
                _ => {}
            }
        }
        let close = close.ok_or(Error::Corrupt("sqlite PRIMARY KEY clause malformed"))?;
        let mut columns = Vec::new();
        for entry in split_top_level(&fragment[inner_open + 1..close]) {
            columns.push(unquote_identifier(entry.trim())?);
        }
        if columns.is_empty() {
            return Err(Error::Corrupt("sqlite PRIMARY KEY clause declares no columns"));
        }
        return Ok(Some(columns));
    }
    Ok(None)
}

fn without_rowid_key_payload(meta: &TableMeta, values: &[WriteValue], encoding: TextEncoding) -> Result<(Vec<u8>, Vec<u8>)> {
    if meta.primary_key_len == 0 || meta.without_rowid_layout.len() != values.len() {
        return Err(Error::Corrupt("sqlite WITHOUT ROWID record layout is incomplete"));
    }
    let mut layout_values: Vec<WriteValue> = Vec::with_capacity(values.len());
    for position in &meta.without_rowid_layout {
        layout_values.push(values[*position].clone());
    }
    let key = encode_record(&layout_values[..meta.primary_key_len], encoding);
    let payload = encode_record(&layout_values, encoding);
    Ok((key, payload))
}

fn record_prefix(payload: &[u8], width: usize, encoding: TextEncoding) -> Result<Vec<u8>> {
    let values =
        decode_record(payload, encoding, Default::default()).map_err(|_| Error::Corrupt("sqlite WITHOUT ROWID record decode failed"))?;
    if values.len() < width {
        return Err(Error::Corrupt("sqlite WITHOUT ROWID record is shorter than its primary key"));
    }
    let width_values: Vec<WriteValue> = values[..width]
        .iter()
        .map(|value| match value {
            database_formats::sqlite::RecordValue::Null => WriteValue::Null,
            database_formats::sqlite::RecordValue::Integer(number) => WriteValue::Integer(*number),
            database_formats::sqlite::RecordValue::Real(real) => WriteValue::Real(*real),
            database_formats::sqlite::RecordValue::Blob(bytes) => WriteValue::Blob(bytes.to_vec()),
            database_formats::sqlite::RecordValue::Text(text) => WriteValue::Text(text.clone()),
        })
        .collect();
    Ok(encode_record(&width_values, encoding))
}

fn is_without_rowid(sql: &str) -> bool {
    sql.trim().trim_end_matches(';').trim().to_ascii_uppercase().ends_with("WITHOUT ROWID")
}

fn render_create_index(name: &str, table: &str, columns: &[String], unique: bool) -> String {
    let keyword = if unique { "CREATE UNIQUE INDEX" } else { "CREATE INDEX" };
    let column_list = columns.iter().map(|column| quote_identifier(column)).collect::<Vec<_>>().join(", ");
    format!("{keyword} {} ON {} ({column_list})", quote_identifier(name), quote_identifier(table))
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn render_declared_type(declared: &str) -> String {
    declared.trim().to_string()
}

fn render_column_head(name: &str, declared: &str) -> String {
    let declared = render_declared_type(declared);
    if declared.is_empty() { quote_identifier(name) } else { format!("{} {}", quote_identifier(name), declared) }
}

fn render_literal(value: &WriteValue) -> String {
    match value {
        WriteValue::Null => "NULL".to_string(),
        WriteValue::Integer(number) => number.to_string(),
        WriteValue::Real(real) => format!("{real:?}"),
        WriteValue::Blob(bytes) => {
            let hex: String = bytes.iter().map(|byte| format!("{byte:02X}")).collect();
            format!("X'{hex}'")
        }
        WriteValue::Text(text) => format!("'{}'", text.replace('\'', "''")),
    }
}

/// Returns the parenthesised body of a `CREATE TABLE` statement.
fn create_table_body(sql: &str) -> Result<&str> {
    let trimmed = sql.trim();
    let upper = trimmed.to_ascii_uppercase();
    let Some(start) = upper.find("CREATE TABLE ")
    else {
        return Err(Error::Unsupported("sqlite CREATE TABLE text unrecognized"));
    };
    let rest = &trimmed[start + "CREATE TABLE ".len()..];
    let Some(open) = rest.find('(')
    else {
        return Err(Error::Unsupported("sqlite CREATE TABLE text lacks columns"));
    };
    let Some(close) = rest.rfind(')')
    else {
        return Err(Error::Unsupported("sqlite CREATE TABLE text lacks closing paren"));
    };
    if close <= open {
        return Err(Error::Unsupported("sqlite CREATE TABLE text malformed"));
    }
    Ok(&rest[open + 1..close])
}

/// True when `text` begins with `keyword` as a whole word.
fn starts_with_keyword(text: &str, keyword: &str) -> bool {
    let Some(prefix) = text.get(..keyword.len())
    else {
        return false;
    };
    if !prefix.eq_ignore_ascii_case(keyword) {
        return false;
    }
    match text.as_bytes().get(keyword.len()) {
        None => true,
        Some(next) => next.is_ascii_whitespace() || *next == b'(',
    }
}

/// Table-level constraints in written order: `(is_primary_key, key positions)`.
/// The order decides the `sqlite_autoindex_<table>_<n>` suffix SQLite assigns,
/// so a `UNIQUE` written before a `PRIMARY KEY` owns the lower number.
fn parse_table_constraints(sql: &str, columns: &[ColumnSpec]) -> Result<Vec<(bool, Vec<usize>)>> {
    let body = create_table_body(sql)?;
    let mut constraints = Vec::new();
    for fragment in split_top_level(body) {
        let mut fragment = fragment.trim();
        // Skip an optional `CONSTRAINT <name>` prefix.
        if starts_with_keyword(fragment, "CONSTRAINT") {
            fragment = fragment["CONSTRAINT".len()..].trim_start();
            if fragment.starts_with('"') {
                let close = fragment[1..].find('"').map(|index| index + 2).ok_or(Error::Unsupported("sqlite constraint name unterminated"))?;
                fragment = fragment[close..].trim_start();
            }
            else {
                let end = fragment.find(|character: char| character.is_ascii_whitespace() || character == '(').unwrap_or(fragment.len());
                fragment = fragment[end..].trim_start();
            }
        }
        let primary = if starts_with_keyword(fragment, "PRIMARY") {
            true
        }
        else if starts_with_keyword(fragment, "UNIQUE") && fragment["UNIQUE".len()..].trim_start().starts_with('(') {
            false
        }
        else {
            continue;
        };
        let open = fragment.find('(').ok_or(Error::Unsupported("sqlite table constraint lacks parentheses"))?;
        let close = fragment.rfind(')').ok_or(Error::Unsupported("sqlite table constraint lacks closing paren"))?;
        let inner = &fragment[open + 1..close];
        let mut positions = Vec::new();
        for name in split_top_level(inner) {
            let name = unquote_identifier(&name.trim().to_string())?;
            let position = columns
                .iter()
                .position(|column| eq_ignore_ascii_case(&column.name, &name))
                .ok_or(Error::Unsupported("sqlite table constraint references unknown column"))?;
            positions.push(position);
        }
        constraints.push((primary, positions));
    }
    Ok(constraints)
}

fn parse_create_table(sql: &str) -> Result<Vec<ColumnSpec>> {
    let body = create_table_body(sql)?;
    let mut columns = Vec::new();
    for fragment in split_top_level(body) {
        let fragment = fragment.trim();
        let upper = fragment.to_ascii_uppercase();
        if ["CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"].iter().any(|keyword| upper.starts_with(keyword)) {
            continue;
        }
        columns.push(parse_column(fragment)?);
    }
    Ok(columns)
}

fn parse_column(fragment: &str) -> Result<ColumnSpec> {
    let tokens = tokenize_fragment(fragment)?;
    if tokens.is_empty() {
        return Err(Error::Unsupported("sqlite column definition empty"));
    }
    let name = unquote_identifier(&tokens[0])?;
    let mut declared_type = String::new();
    let mut index = 1usize;
    while index < tokens.len() {
        let upper = tokens[index].to_ascii_uppercase();
        if matches!(upper.as_str(), "NOT" | "DEFAULT" | "PRIMARY" | "UNIQUE" | "NULL" | "REFERENCES" | "COLLATE") {
            break;
        }
        if !declared_type.is_empty() {
            declared_type.push(' ');
        }
        declared_type.push_str(&tokens[index]);
        index += 1;
    }
    let mut not_null = false;
    let mut primary_key = false;
    let mut unique = false;
    let mut autoincrement = false;
    let mut collate = None;
    let mut default_value = None;
    let mut default_text = None;
    while index < tokens.len() {
        let upper = tokens[index].to_ascii_uppercase();
        match upper.as_str() {
            "NOT" => {
                require_keyword(&tokens, index + 1, "NULL")?;
                not_null = true;
                index += 2;
            }
            "PRIMARY" => {
                require_keyword(&tokens, index + 1, "KEY")?;
                primary_key = true;
                index += 2;
            }
            "UNIQUE" => {
                unique = true;
                index += 1;
            }
            "AUTOINCREMENT" => {
                if !primary_key {
                    return Err(Error::Unsupported("sqlite AUTOINCREMENT needs a preceding PRIMARY KEY"));
                }
                autoincrement = true;
                index += 1;
            }
            "DEFAULT" => {
                let (value, consumed) = parse_literal(&tokens[index + 1..])?;
                default_value = Some(value);
                default_text = Some(tokens[index + 1..index + 1 + consumed].join(" "));
                index += 1 + consumed;
            }
            "NULL" => {
                index += 1;
            }
            "COLLATE" => {
                let name = tokens.get(index + 1).ok_or(Error::Unsupported("sqlite COLLATE name missing"))?;
                let name = unquote_identifier(name)?;
                collate = Some(Collation::parse(&name).ok_or(Error::Unsupported("sqlite no such collation sequence"))?);
                index += 2;
            }
            "CONSTRAINT" => {
                // Skip the name; the clause that follows is parsed next.
                if index + 1 >= tokens.len() {
                    return Err(Error::Unsupported("sqlite column constraint name missing"));
                }
                index += 2;
            }
            _ if upper.starts_with("CHECK") => {
                // The storage layer does not evaluate checks; the engine does.
                // Consume the whole balanced `CHECK (...)` clause so the rest of
                // the column definition still parses.
                let mut depth = 0i64;
                let mut opened = false;
                while index < tokens.len() {
                    for character in tokens[index].chars() {
                        match character {
                            '(' => {
                                depth += 1;
                                opened = true;
                            }
                            ')' => depth -= 1,
                            _ => {}
                        }
                    }
                    index += 1;
                    if opened && depth <= 0 {
                        break;
                    }
                }
                if !opened {
                    return Err(Error::Unsupported("sqlite CHECK constraint lacks parentheses"));
                }
            }
            "REFERENCES" => {
                // The storage layer does not enforce foreign keys; the engine
                // does. Skip the parent reference, its optional column list,
                // the `ON DELETE` / `ON UPDATE` action and the deferral clause
                // so the column definition still parses on reopen.
                index += 1;
                if index < tokens.len() {
                    index += 1;
                }
                if tokens.get(index).map(|token| token == "(").unwrap_or(false) {
                    let mut depth = 0i64;
                    let mut opened = false;
                    while index < tokens.len() {
                        for character in tokens[index].chars() {
                            match character {
                                '(' => {
                                    depth += 1;
                                    opened = true;
                                }
                                ')' => depth -= 1,
                                _ => {}
                            }
                        }
                        index += 1;
                        if opened && depth <= 0 {
                            break;
                        }
                    }
                }
                while index < tokens.len() {
                    if tokens[index].eq_ignore_ascii_case("NOT")
                        && tokens.get(index + 1).map(|token| token.eq_ignore_ascii_case("DEFERRABLE")).unwrap_or(false)
                    {
                        index += 2;
                    }
                    else {
                        index += 1;
                    }
                }
            }
            _ => return Err(Error::Unsupported("sqlite column constraint unrecognized")),
        }
    }
    Ok(ColumnSpec { name, declared_type, not_null, default_value, default_text, primary_key, unique, autoincrement, collate })
}

fn require_keyword(tokens: &[String], index: usize, keyword: &str) -> Result<()> {
    match tokens.get(index) {
        Some(token) if token.eq_ignore_ascii_case(keyword) => Ok(()),
        _ => Err(Error::Unsupported("sqlite column constraint unrecognized")),
    }
}

fn parse_literal(tokens: &[String]) -> Result<(WriteValue, usize)> {
    let Some(first) = tokens.first()
    else {
        return Err(Error::Unsupported("sqlite DEFAULT literal missing"));
    };
    if first.eq_ignore_ascii_case("NULL") {
        return Ok((WriteValue::Null, 1));
    }
    if first.starts_with('\'') {
        let mut text = String::new();
        let mut consumed = 0usize;
        let mut closed = false;
        for token in tokens {
            consumed += 1;
            let trimmed = if consumed == 1 { &token[1..] } else { token.as_str() };
            text.push_str(trimmed);
            if text.ends_with('\'') && !text.ends_with("''") {
                text.pop();
                closed = true;
                break;
            }
            text.push(' ');
        }
        if !closed {
            return Err(Error::Unsupported("sqlite DEFAULT string literal unterminated"));
        }
        return Ok((WriteValue::Text(text.replace("''", "'")), consumed));
    }
    if first.starts_with("X'") || first.starts_with("x'") {
        let inner = &first[2..first.len() - 1];
        let mut bytes = Vec::with_capacity(inner.len() / 2);
        let mut chars = inner.chars();
        while let (Some(high), Some(low)) = (chars.next(), chars.next()) {
            let byte =
                u8::from_str_radix(&format!("{high}{low}"), 16).map_err(|_| Error::Unsupported("sqlite DEFAULT blob literal invalid"))?;
            bytes.push(byte);
        }
        return Ok((WriteValue::Blob(bytes), 1));
    }
    if let Ok(integer) = first.parse::<i64>() {
        return Ok((WriteValue::Integer(integer), 1));
    }
    if let Ok(real) = first.parse::<f64>() {
        return Ok((WriteValue::Real(real), 1));
    }
    Err(Error::Unsupported("sqlite DEFAULT literal unrecognized"))
}

fn tokenize_fragment(fragment: &str) -> Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut chars = fragment.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            ' ' | '\t' | '\n' | '\r' => continue,
            '"' => {
                let mut token = String::from("\"");
                loop {
                    let Some(next) = chars.next()
                    else {
                        return Err(Error::Unsupported("sqlite identifier unterminated"));
                    };
                    token.push(next);
                    if next == '"' {
                        if chars.peek() == Some(&'"') {
                            token.push(chars.next().unwrap());
                            continue;
                        }
                        break;
                    }
                }
                tokens.push(token);
            }
            '\'' => {
                let mut token = String::from("'");
                loop {
                    let Some(next) = chars.next()
                    else {
                        return Err(Error::Unsupported("sqlite literal unterminated"));
                    };
                    token.push(next);
                    if next == '\'' {
                        if chars.peek() == Some(&'\'') {
                            token.push(chars.next().unwrap());
                            continue;
                        }
                        break;
                    }
                }
                tokens.push(token);
            }
            _ => {
                let mut token = String::new();
                token.push(character);
                while let Some(next) = chars.peek() {
                    if matches!(next, ' ' | '\t' | '\n' | '\r' | '"' | '\'') {
                        break;
                    }
                    token.push(*next);
                    chars.next();
                }
                tokens.push(token);
            }
        }
    }
    Ok(tokens)
}

fn unquote_identifier(token: &str) -> Result<String> {
    if token.starts_with('"') && token.ends_with('"') && token.len() >= 2 {
        return Ok(token[1..token.len() - 1].replace("\"\"", "\""));
    }
    Ok(token.to_string())
}

fn split_top_level(body: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut current = String::new();
    let mut in_quote: Option<char> = None;
    for character in body.chars() {
        if let Some(quote) = in_quote {
            current.push(character);
            if character == quote {
                in_quote = None;
            }
            continue;
        }
        match character {
            '"' | '\'' => {
                in_quote = Some(character);
                current.push(character);
            }
            '(' => {
                depth += 1;
                current.push(character);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(character);
            }
            ',' if depth == 0 => {
                parts.push(current.clone());
                current.clear();
            }
            _ => current.push(character),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current);
    }
    parts
}

/// Encodes one managed index key: the caller's key values followed by the
/// rowid, which is what makes each entry unique in the b-tree.
fn managed_index_key(values: &[WriteValue], rowid: i64, encoding: TextEncoding) -> Vec<u8> {
    let mut key = values.to_vec();
    key.push(WriteValue::Integer(rowid));
    encode_record(&key, encoding)
}

/// Splits an index key list on top-level commas, keeping commas inside
/// parentheses or quotes part of their term.
fn split_index_fragments(text: &str) -> Vec<&str> {
    let mut fragments = Vec::new();
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut start = 0usize;
    for (index, character) in text.char_indices() {
        if let Some(active) = quote {
            if character == active {
                quote = None;
            }
            continue;
        }
        match character {
            '"' | '\'' | '`' => quote = Some(character),
            '(' | '[' => depth += 1,
            ')' | ']' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                fragments.push(&text[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    fragments.push(&text[start..]);
    fragments
}

/// Returns whether an index key is a bare identifier, optionally quoted.
fn is_plain_index_identifier(text: &str) -> bool {
    let text = text.trim();
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    if let Some(open) = bytes.first().copied() {
        let close = match open {
            b'"' => Some(b'"'),
            b'\'' => Some(b'\''),
            b'`' => Some(b'`'),
            b'[' => Some(b']'),
            _ => None,
        };
        if let Some(close) = close {
            return bytes.len() >= 2 && bytes.last().copied() == Some(close);
        }
    }
    let mut characters = text.chars();
    let Some(first) = characters.next()
    else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    characters.all(|character| character.is_ascii_alphanumeric() || character == '_' || character == '$')
}

/// Returns whether a stored `CREATE INDEX` text needs caller-computed keys:
/// an expression key, a `COLLATE` term or a trailing `WHERE` predicate.
fn index_sql_is_managed(sql: &str) -> Result<bool> {
    let upper = sql.to_ascii_uppercase();
    let Some(on) = upper.find(" ON ")
    else {
        return Err(Error::Unsupported("sqlite CREATE INDEX text unrecognized"));
    };
    let rest = &sql[on + 4..];
    let Some(open) = rest.find('(')
    else {
        return Err(Error::Unsupported("sqlite CREATE INDEX text lacks columns"));
    };
    let Some(close) = rest.rfind(')')
    else {
        return Err(Error::Unsupported("sqlite CREATE INDEX text lacks closing paren"));
    };
    if rest[close + 1..].split_whitespace().any(|word| word.eq_ignore_ascii_case("where")) {
        return Ok(true);
    }
    for fragment in split_index_fragments(&rest[open + 1..close]) {
        let fragment = fragment.trim();
        let core = match fragment.rsplit_once(char::is_whitespace) {
            Some((head, tail)) if tail.eq_ignore_ascii_case("asc") || tail.eq_ignore_ascii_case("desc") => head.trim_end(),
            _ => fragment,
        };
        if core.split_whitespace().any(|word| word.eq_ignore_ascii_case("collate")) || !is_plain_index_identifier(core) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_create_index(sql: &str) -> Result<(Vec<String>, bool)> {
    let upper = sql.to_ascii_uppercase();
    let unique = upper.starts_with("CREATE UNIQUE INDEX");
    let Some(on) = upper.find(" ON ")
    else {
        return Err(Error::Unsupported("sqlite CREATE INDEX text unrecognized"));
    };
    let rest = &sql[on + 4..];
    let Some(open) = rest.find('(')
    else {
        return Err(Error::Unsupported("sqlite CREATE INDEX text lacks columns"));
    };
    let Some(close) = rest.rfind(')')
    else {
        return Err(Error::Unsupported("sqlite CREATE INDEX text lacks closing paren"));
    };
    let mut columns = Vec::new();
    for fragment in rest[open + 1..close].split(',') {
        let fragment = fragment.trim();
        // `ASC` names the default order and `DESC` is rejected by the SQL
        // surface; either way the key itself is the name before the keyword.
        let name = match fragment.rsplit_once(char::is_whitespace) {
            Some((head, tail)) if tail.eq_ignore_ascii_case("asc") || tail.eq_ignore_ascii_case("desc") => head.trim_end(),
            _ => fragment,
        };
        columns.push(unquote_identifier(name)?);
    }
    Ok((columns, unique))
}

/// 解析索引每个键项生效的排序规则：键项显式写了 `COLLATE` 就用它，裸列
/// 键则取该列的声明排序规则，表达式键没有列可依，按 `BINARY` 处理。
fn index_collations(sql: &str, columns: &[ColumnSpec]) -> Result<Vec<Collation>> {
    let upper = sql.to_ascii_uppercase();
    let Some(on) = upper.find(" ON ")
    else {
        return Err(Error::Unsupported("sqlite CREATE INDEX text unrecognized"));
    };
    let rest = &sql[on + 4..];
    let Some(open) = rest.find('(')
    else {
        return Err(Error::Unsupported("sqlite CREATE INDEX text lacks columns"));
    };
    let Some(close) = rest.rfind(')')
    else {
        return Err(Error::Unsupported("sqlite CREATE INDEX text lacks closing paren"));
    };
    let mut collations = Vec::new();
    for fragment in split_index_fragments(&rest[open + 1..close]) {
        let fragment = fragment.trim();
        let core = match fragment.rsplit_once(char::is_whitespace) {
            Some((head, tail)) if tail.eq_ignore_ascii_case("asc") || tail.eq_ignore_ascii_case("desc") => head.trim_end(),
            _ => fragment,
        };
        let words: Vec<&str> = core.split_whitespace().collect();
        let explicit =
            words.windows(2).find(|pair| pair[0].eq_ignore_ascii_case("collate")).map(|pair| unquote_identifier(pair[1])).transpose()?;
        let collation = match explicit {
            Some(name) => Collation::parse(&name).ok_or(Error::Unsupported("sqlite no such collation sequence"))?,
            None if is_plain_index_identifier(core) => {
                let name = unquote_identifier(core)?;
                columns.iter().find(|column| eq_ignore_ascii_case(&column.name, &name)).and_then(|column| column.collate).unwrap_or_default()
            }
            None => Collation::Binary,
        };
        collations.push(collation);
    }
    Ok(collations)
}

/// 按排序规则归一化一个索引键，使 `NOCASE` / `RTRIM` 下等价的值得到同一个
/// 编码，唯一性判定因而与 b-tree 比较顺序保持一致。
fn normalized_index_values(values: &[WriteValue], collations: &[Collation]) -> Vec<WriteValue> {
    values
        .iter()
        .enumerate()
        .map(|(position, value)| match (collations.get(position).copied().unwrap_or_default(), value) {
            (Collation::Nocase, WriteValue::Text(text)) => {
                WriteValue::Text(text.chars().map(|character| character.to_ascii_lowercase()).collect())
            }
            (Collation::Rtrim, WriteValue::Text(text)) => WriteValue::Text(text.trim_end_matches(' ').to_string()),
            (_, value) => value.clone(),
        })
        .collect()
}
