use std::cmp::Ordering;

use database_types::{Error, Result};

use database_formats::sqlite::{RecordLimits, RecordValue, TextEncoding, decode_record, decode_varint, validate_database};

/// Resource bounds for a read-only table b-tree scan.
#[derive(Debug, Clone, Copy)]
pub struct TableLimits {
    /// Maximum number of b-tree and overflow pages visited together.
    pub max_pages: usize,
    /// Maximum number of returned rows.
    pub max_rows: usize,
    /// Maximum record payload size per row.
    pub max_payload_bytes: usize,
    /// Maximum sum of returned record payload sizes.
    pub max_total_payload_bytes: usize,
}

impl Default for TableLimits {
    fn default() -> Self {
        Self { max_pages: 100_000, max_rows: 100_000, max_payload_bytes: 16 * 1024 * 1024, max_total_payload_bytes: 64 * 1024 * 1024 }
    }
}

/// An assembled SQLite table leaf record, before record decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    /// Signed SQLite rowid, including INTEGER PRIMARY KEY aliases.
    pub rowid: i64,
    /// Complete record bytes, including any overflow payload.
    pub payload: Vec<u8>,
}

struct Scan<'snapshot> {
    bytes: &'snapshot [u8],
    page_size: usize,
    usable: usize,
    page_count: usize,
    /// 页号→是否已访问的位图。`page_count` 由文件长度或经校验的 header
    /// 声明界定,不超实际字节数,位图预分配有界;替代 `HashSet` 消除每页
    /// 一次哈希插入的扫描开销。
    visited: Vec<bool>,
    /// 已访问页数,对应旧实现 `visited.len()` 的限额语义。
    visited_count: usize,
    limits: TableLimits,
}

impl<'snapshot> Scan<'snapshot> {
    fn page(&mut self, number: u32) -> Result<&'snapshot [u8]> {
        let index = usize::try_from(number).map_err(|_| Error::Corrupt("sqlite page number overflow"))?;
        if index == 0 || index > self.page_count {
            return Err(Error::Corrupt("sqlite page reference out of bounds"));
        }
        if self.visited_count >= self.limits.max_pages {
            return Err(Error::Unsupported("sqlite table page limit exceeded"));
        }
        if self.visited[index] {
            return Err(Error::Corrupt("sqlite repeated b-tree or overflow page"));
        }
        self.visited[index] = true;
        self.visited_count += 1;
        let start = (index - 1) * self.page_size;
        Ok(&self.bytes[start..start + self.usable])
    }

    /// 载荷在本页存放的本地字节数,按上游 SQLite 的 U 值公式。
    fn local(&self, length: usize) -> usize {
        let maximum = self.usable - 35;
        let minimum = ((self.usable - 12) * 32 / 255) - 23;
        if length <= maximum {
            return length;
        }
        let candidate = minimum + (length - minimum) % (self.usable - 4);
        if candidate <= maximum { candidate } else { minimum }
    }

    fn payload(&mut self, page: &[u8], offset: usize, length: usize) -> Result<(Vec<u8>, usize)> {
        let local = self.local(length);
        let end = offset.checked_add(local).ok_or(Error::Corrupt("sqlite cell length overflow"))?;
        let initial = page.get(offset..end).ok_or(Error::Corrupt("sqlite cell payload truncated"))?;
        let overflow_pages = (length - local).div_ceil(self.usable - 4);
        if overflow_pages > self.limits.max_pages.saturating_sub(self.visited_count) {
            return Err(Error::Unsupported("sqlite table page limit exceeded"));
        }
        let mut payload = Vec::new();
        payload.try_reserve_exact(length).map_err(|_| Error::Unsupported("sqlite payload allocation failed"))?;
        payload.extend_from_slice(initial);
        if local == length {
            return Ok((payload, end));
        }
        let mut next = read_u32(page, end)?;
        while payload.len() < length {
            let overflow = self.page(next)?;
            next = read_u32(overflow, 0)?;
            let count = (length - payload.len()).min(self.usable - 4);
            payload.extend_from_slice(&overflow[4..4 + count]);
        }
        if next != 0 {
            return Err(Error::Corrupt("sqlite overflow chain exceeds payload"));
        }
        Ok((payload, end + 4))
    }
}

/// Reads raw record payloads from an index b-tree in key order.
///
/// WITHOUT ROWID tables store their rows in an index b-tree rooted at the
/// schema `rootpage`, and secondary indexes use the same layout with a
/// trailing rowid column. Interior cells hold real keys, so the walk emits
/// them between child subtrees and rejects any out-of-order pair.
pub fn read_index_table(bytes: &[u8], root_page: u32, limits: TableLimits) -> Result<Vec<Vec<u8>>> {
    read_index_table_with(bytes, root_page, limits, &[])
}

/// Reads an index b-tree whose key positions use the given collations.
///
/// 不传排序规则时（`&[]`）键序按“任意内置排序规则下都合法”判定：只有三种
/// 规则下都逆序才算损坏，这样无需拿到 schema 也能读 `NOCASE` / `RTRIM`
/// 索引。
pub fn read_index_table_with(
    bytes: &[u8],
    root_page: u32,
    limits: TableLimits,
    collations: &[crate::sqlite::writer::Collation],
) -> Result<Vec<Vec<u8>>> {
    let page_size = validate_database(bytes)?;
    if bytes.is_empty() {
        return Err(Error::Corrupt("sqlite index root in empty database"));
    }
    let encoding = match read_u32(bytes, 56)? {
        2 => TextEncoding::Utf16Le,
        3 => TextEncoding::Utf16Be,
        _ => TextEncoding::Utf8,
    };
    let declared = read_u32(bytes, 28)? as usize;
    let page_count = if declared != 0 && read_u32(bytes, 24)? == read_u32(bytes, 92)? { declared } else { bytes.len() / page_size };
    let mut scan = Scan {
        bytes,
        page_size,
        usable: page_size - usize::from(bytes[20]),
        page_count,
        visited: vec![false; page_count + 1],
        visited_count: 0,
        limits,
    };
    let mut pending = vec![IndexTask::Page(root_page)];
    let mut payloads = Vec::new();
    let mut previous: Option<Vec<u8>> = None;
    let mut total_payload = 0usize;
    while let Some(task) = pending.pop() {
        let number = match task {
            IndexTask::Key(payload) => {
                if payloads.len() >= limits.max_rows {
                    return Err(Error::Unsupported("sqlite index row limit exceeded"));
                }
                if let Some(left) = &previous {
                    let ordering = compare_records_with(left, &payload, encoding, collations)?;
                    let violated = if collations.is_empty() {
                        ordering != Ordering::Less
                            && compare_records_with(left, &payload, encoding, &[crate::sqlite::writer::Collation::Nocase])? != Ordering::Less
                            && compare_records_with(left, &payload, encoding, &[crate::sqlite::writer::Collation::Rtrim])? != Ordering::Less
                    }
                    else {
                        ordering != Ordering::Less
                    };
                    if violated {
                        return Err(Error::Corrupt("sqlite index key order violated"));
                    }
                }
                previous = Some(payload.clone());
                payloads.push(payload);
                continue;
            }
            IndexTask::Page(number) => number,
        };
        let page = scan.page(number)?;
        let header = if number == 1 { 100 } else { 0 };
        let kind = *page.get(header).ok_or(Error::Corrupt("sqlite b-tree header truncated"))?;
        let header_size = match kind {
            2 => 12,
            10 => 8,
            _ => return Err(Error::Unsupported("sqlite b-tree is not an index page")),
        };
        let count = read_u16(page, header + 3)?;
        let pointers_end = header + header_size + count * 2;
        if pointers_end > page.len() || page[header + 7] > 60 {
            return Err(Error::Corrupt("sqlite b-tree header invalid"));
        }
        let raw_start = read_u16(page, header + 5)?;
        let content_start = if raw_start == 0 { 65536 } else { raw_start };
        if content_start < pointers_end || content_start > page.len() {
            return Err(Error::Corrupt("sqlite b-tree content boundary invalid"));
        }
        let mut tasks = Vec::with_capacity(count * 2 + 1);
        for index in 0..count {
            let cell = read_u16(page, header + header_size + index * 2)?;
            if cell < content_start || cell >= page.len() {
                return Err(Error::Corrupt("sqlite cell pointer out of bounds"));
            }
            let (child, payload_start) = if kind == 2 { (Some(read_u32(page, cell)?), cell + 4) } else { (None, cell) };
            let (length, length_size) = decode_varint(&page[payload_start..])?;
            let length = usize::try_from(length).map_err(|_| Error::Unsupported("sqlite payload size exceeds host"))?;
            if length > limits.max_payload_bytes {
                return Err(Error::Unsupported("sqlite index payload limit exceeded"));
            }
            total_payload = total_payload.checked_add(length).ok_or(Error::Unsupported("sqlite index total payload overflow"))?;
            if total_payload > limits.max_total_payload_bytes {
                return Err(Error::Unsupported("sqlite index total payload limit exceeded"));
            }
            let (payload, _end) = scan.payload(page, payload_start + length_size, length)?;
            if let Some(child) = child {
                tasks.push(IndexTask::Page(child));
            }
            tasks.push(IndexTask::Key(payload));
        }
        if kind == 2 {
            tasks.push(IndexTask::Page(read_u32(page, header + 8)?));
        }
        if pending.len().saturating_add(tasks.len()).saturating_add(scan.visited_count) > limits.max_pages {
            return Err(Error::Unsupported("sqlite index page limit exceeded"));
        }
        pending.extend(tasks.into_iter().rev());
    }
    Ok(payloads)
}

enum IndexTask {
    Page(u32),
    Key(Vec<u8>),
}

/// SQLite BINARY order: NULL, numbers, text, then blob, with byte-wise ties.
fn compare_records(left: &[u8], right: &[u8], encoding: TextEncoding) -> Result<Ordering> {
    compare_records_with(left, right, encoding, &[])
}

/// 按每个键位的排序规则比较两条索引记录，缺省位置按 `BINARY`。
pub(crate) fn compare_records_with(
    left: &[u8],
    right: &[u8],
    encoding: TextEncoding,
    collations: &[crate::sqlite::writer::Collation],
) -> Result<Ordering> {
    let limits = |payload: &[u8]| RecordLimits { max_payload_bytes: payload.len(), max_columns: 4096 };
    let left_values = decode_record(left, encoding, limits(left))?;
    let right_values = decode_record(right, encoding, limits(right))?;
    for (position, (left, right)) in left_values.iter().zip(right_values.iter()).enumerate() {
        let ordering = match (left, right) {
            (RecordValue::Text(left), RecordValue::Text(right)) => {
                collations.get(position).copied().unwrap_or_default().compare_text(left, right)
            }
            _ => compare_values(left, right),
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(left_values.len().cmp(&right_values.len()))
}

fn compare_values(left: &RecordValue<'_>, right: &RecordValue<'_>) -> Ordering {
    fn rank(value: &RecordValue<'_>) -> u8 {
        match value {
            RecordValue::Null => 0,
            RecordValue::Integer(_) | RecordValue::Real(_) => 1,
            RecordValue::Text(_) => 2,
            RecordValue::Blob(_) => 3,
        }
    }
    match (left, right) {
        (RecordValue::Null, RecordValue::Null) => Ordering::Equal,
        (RecordValue::Integer(left), RecordValue::Integer(right)) => left.cmp(right),
        (RecordValue::Real(left), RecordValue::Real(right)) => left.partial_cmp(right).unwrap_or(Ordering::Equal),
        (RecordValue::Integer(left), RecordValue::Real(right)) => (*left as f64).partial_cmp(right).unwrap_or(Ordering::Equal),
        (RecordValue::Real(left), RecordValue::Integer(right)) => left.partial_cmp(&(*right as f64)).unwrap_or(Ordering::Equal),
        (RecordValue::Text(left), RecordValue::Text(right)) => left.as_bytes().cmp(right.as_bytes()),
        (RecordValue::Blob(left), RecordValue::Blob(right)) => left.cmp(right),
        _ => rank(left).cmp(&rank(right)),
    }
}

/// Reads a rowid table from a validated, quiescent main-file snapshot.
/// Index b-trees (including WITHOUT ROWID tables) are explicitly unsupported.
/// This does not recover WAL/journals or perform a full integrity check.
pub fn read_table(bytes: &[u8], root_page: u32, limits: TableLimits) -> Result<Vec<TableRow>> {
    read_table_inner(bytes, root_page, limits, None, None, None)
}

/// 读取 rowid 落在闭区间 `[lower, upper]` 内的记录,按 rowid 升序返回。
/// 与 [`read_table`] 共用同一套损坏检测与限额,但只物化区间内的载荷,
/// 并按查询界剪枝内部节点:与区间不相交的子树不再下探。点查即
/// `lower == upper`。
pub fn read_table_range(bytes: &[u8], root_page: u32, lower: i64, upper: i64, limits: TableLimits) -> Result<Vec<TableRow>> {
    if lower > upper {
        return Ok(Vec::new());
    }
    read_table_inner(bytes, root_page, limits, Some(lower), Some(upper), None)
}

/// 按 rowid 升序读取一张 rowid 表的前 `max_rows` 行,读满即停。
/// 与 [`read_table`] 共用同一套损坏检测,但不再遍历整棵 b-tree:
/// 满足 `ORDER BY <rowid 序> LIMIT n` 的有界扫描只访问覆盖前 n 行
/// 所需的页。`max_rows == 0` 直接返回空。
pub fn read_table_head(bytes: &[u8], root_page: u32, max_rows: usize, limits: TableLimits) -> Result<Vec<TableRow>> {
    if max_rows == 0 {
        return Ok(Vec::new());
    }
    read_table_inner(bytes, root_page, limits, None, None, Some(max_rows))
}

fn read_table_inner(
    bytes: &[u8],
    root_page: u32,
    limits: TableLimits,
    floor: Option<i64>,
    ceil: Option<i64>,
    stop_after: Option<usize>,
) -> Result<Vec<TableRow>> {
    let page_size = validate_database(bytes)?;
    if bytes.is_empty() {
        return Err(Error::Corrupt("sqlite table root in empty database"));
    }
    let declared = read_u32(bytes, 28)? as usize;
    let page_count = if declared != 0 && read_u32(bytes, 24)? == read_u32(bytes, 92)? { declared } else { bytes.len() / page_size };
    let mut scan = Scan {
        bytes,
        page_size,
        usable: page_size - usize::from(bytes[20]),
        page_count,
        visited: vec![false; page_count + 1],
        visited_count: 0,
        limits,
    };
    let mut pending = vec![(root_page, None::<i64>, None::<i64>)];
    let mut rows: Vec<TableRow> = Vec::new();
    let mut total_payload = 0usize;
    // 每页的 cell 跨度与子页任务复用同一缓冲,避免逐页堆分配。
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut children: Vec<(u32, Option<i64>, Option<i64>)> = Vec::new();
    while let Some((number, lower, upper)) = pending.pop() {
        let page = scan.page(number)?;
        let header = if number == 1 { 100 } else { 0 };
        let kind = *page.get(header).ok_or(Error::Corrupt("sqlite b-tree header truncated"))?;
        let header_size = match kind {
            5 => 12,
            13 => 8,
            2 | 10 => return Err(Error::Unsupported("sqlite index b-tree scan")),
            _ => return Err(Error::Corrupt("sqlite table b-tree page type invalid")),
        };
        let count = read_u16(page, header + 3)?;
        let pointers_end = header + header_size + count * 2;
        if pointers_end > page.len() || page[header + 7] > 60 {
            return Err(Error::Corrupt("sqlite b-tree header invalid"));
        }
        let raw_start = read_u16(page, header + 5)?;
        let content_start = if raw_start == 0 { 65536 } else { raw_start };
        if content_start < pointers_end || content_start > page.len() {
            return Err(Error::Corrupt("sqlite b-tree content boundary invalid"));
        }
        let mut previous = lower;
        spans.clear();
        children.clear();
        spans.reserve(count);
        for index in 0..count {
            let cell = read_u16(page, header + header_size + index * 2)?;
            if cell < content_start || cell >= page.len() {
                return Err(Error::Corrupt("sqlite cell pointer out of bounds"));
            }
            if kind == 5 {
                let child = read_u32(page, cell)?;
                let (key, consumed) = decode_varint(page.get(cell + 4..).ok_or(Error::Corrupt("sqlite interior cell truncated"))?)?;
                let key = key as i64;
                check_key(key, previous, upper)?;
                children.push((child, previous, Some(key)));
                previous = Some(key);
                spans.push((cell, cell + 4 + consumed));
            }
            else {
                if rows.len() >= limits.max_rows {
                    return Err(Error::Unsupported("sqlite table row limit exceeded"));
                }
                let (length, length_size) = decode_varint(&page[cell..])?;
                let length = usize::try_from(length).map_err(|_| Error::Unsupported("sqlite payload size exceeds host"))?;
                if length > limits.max_payload_bytes {
                    return Err(Error::Unsupported("sqlite table payload limit exceeded"));
                }
                total_payload = total_payload.checked_add(length).ok_or(Error::Unsupported("sqlite table total payload overflow"))?;
                if total_payload > limits.max_total_payload_bytes {
                    return Err(Error::Unsupported("sqlite table total payload limit exceeded"));
                }
                let (rowid, rowid_size) = decode_varint(&page[cell + length_size..])?;
                let rowid = rowid as i64;
                check_key(rowid, previous, upper)?;
                if rows.last().is_some_and(|last| last.rowid >= rowid) {
                    return Err(Error::Corrupt("sqlite table rows out of order"));
                }
                previous = Some(rowid);
                let prefix = length_size + rowid_size;
                let local = scan.local(length);
                let span_end = cell + prefix + local + if length > local { 4 } else { 0 };
                if span_end > page.len() {
                    return Err(Error::Corrupt("sqlite cell payload truncated"));
                }
                spans.push((cell, span_end));
                if floor.is_some_and(|bound| rowid < bound) || ceil.is_some_and(|bound| rowid > bound) {
                    continue;
                }
                let (payload, _end) = scan.payload(page, cell + prefix, length)?;
                rows.push(TableRow { rowid, payload });
                if stop_after.is_some_and(|bound| rows.len() >= bound) {
                    return Ok(rows);
                }
            }
        }
        spans.sort_unstable();
        if spans.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(Error::Corrupt("sqlite b-tree cells overlap"));
        }
        if kind == 5 {
            children.push((read_u32(page, header + 8)?, previous, upper));
            if pending.len().saturating_add(children.len()).saturating_add(scan.visited_count) > limits.max_pages {
                return Err(Error::Unsupported("sqlite table page limit exceeded"));
            }
            pending.extend(children.iter().rev().copied().filter(|(_, lo, hi)| range_intersects(*lo, *hi, floor, ceil)));
        }
    }
    Ok(rows)
}

/// 子树校验界 `(lo, hi]`(`lo` 不含、`hi` 含)与查询闭区间 `[floor, ceil]`
/// 是否可能相交;`None` 界表示无界。区间读取据此剪枝不相交的子树。
fn range_intersects(lo: Option<i64>, hi: Option<i64>, floor: Option<i64>, ceil: Option<i64>) -> bool {
    hi.is_none_or(|bound| floor.is_none_or(|lower| bound >= lower)) && lo.is_none_or(|bound| ceil.is_none_or(|upper| bound < upper))
}

fn check_key(key: i64, lower: Option<i64>, upper: Option<i64>) -> Result<()> {
    if lower.is_some_and(|bound| key <= bound) || upper.is_some_and(|bound| key > bound) {
        return Err(Error::Corrupt("sqlite b-tree key outside ordered range"));
    }
    Ok(())
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<usize> {
    let value = bytes.get(offset..offset + 2).ok_or(Error::Corrupt("sqlite b-tree field truncated"))?;
    Ok(usize::from(u16::from_be_bytes(value.try_into().unwrap())))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let value = bytes.get(offset..offset + 4).ok_or(Error::Corrupt("sqlite page field truncated"))?;
    Ok(u32::from_be_bytes(value.try_into().unwrap()))
}
