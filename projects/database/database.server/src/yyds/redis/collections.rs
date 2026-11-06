//! Redis 的 hash 与 list 值类型，落在既有扁平键值合同之上。
//!
//! `.yykv` v3 的字节布局不能改动，`Record` 也没有类型字段。这里把类型编码在
//! **命名空间** 上：字符串继续使用逻辑库命名空间（`redis:<db>`），hash 落在
//! `redis:<db>:hash`，list 落在 `redis:<db>:list`。命名空间本来就是 `Record.key`
//! 的一部分，所以不新增字段、不升格式版本，重启后随快照恢复。
//!
//! 这样不会把既有字符串键误判成集合：两类数据位于互不相交的命名空间，
//! 字符串内容再特殊也不会被当成 hash/list；判定类型只是逐个命名空间探测可见记录，
//! 同一逻辑键在任一时刻至多存在一种表示（写入时强制互斥，删除时清干净）。

use std::io;

use database_execution::cluster::KeyValueExecutor;
use database_query::cluster_kv::{KeyValueAction, KeyValueCommand, KeyValueResult};
use database_types::cluster::Namespace;

use super::{
    connection::{write_bulk, write_error, write_null, write_null_array},
    resp::Request,
};

/// 一个逻辑键当前承载的值类型。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyKind {
    /// 字符串，沿用原命名空间，行为与升级前完全一致。
    String,
    /// 哈希，字段按插入顺序保存在一个值 blob 内。
    Hash,
    /// 列表，元素按从表头到表尾的顺序保存在一个值 blob 内。
    List,
}

impl KeyKind {
    /// 所有已知类型，按探测顺序排列（字符串优先，保持既有语义）。
    pub(crate) const ALL: [KeyKind; 3] = [KeyKind::String, KeyKind::Hash, KeyKind::List];

    /// `TYPE` 回复里使用的类型名。
    pub(crate) fn name(self) -> &'static str {
        match self {
            KeyKind::String => "string",
            KeyKind::Hash => "hash",
            KeyKind::List => "list",
        }
    }

    /// 该类型使用的存储命名空间。
    ///
    /// 字符串沿用调用方传入的逻辑库命名空间，其它类型加类型后缀，因此既有数据无需迁移。
    pub(crate) fn namespace(self, base: &Namespace) -> Namespace {
        match self {
            KeyKind::String => base.clone(),
            KeyKind::Hash => Namespace(format!("{}:hash", base.0)),
            KeyKind::List => Namespace(format!("{}:list", base.0)),
        }
    }
}

/// 探测一个逻辑键当前属于哪种类型。
///
/// 一次探测最多命中一种表示；`Ok(None)` 表示键不存在。执行错误转成可回复的消息，
/// 由调用方决定如何写成 RESP 错误。
pub(crate) fn find_kind(executor: &dyn KeyValueExecutor, base: &Namespace, key: &[u8]) -> Result<Option<KeyKind>, String> {
    for kind in KeyKind::ALL {
        let command = KeyValueCommand { namespace: kind.namespace(base), key: key.to_vec(), action: KeyValueAction::Exists };
        match executor.execute(None, command) {
            Ok(KeyValueResult::Exists(true)) => return Ok(Some(kind)),
            Ok(KeyValueResult::Exists(false)) => {}
            Ok(_) => return Err("type probe returned an unexpected result".into()),
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(None)
}

/// 集合键的一次读取结果。
///
/// `Rejected` 表示错误回复已经写进连接，调用方不应再追加任何字节。
enum Loaded<T> {
    /// 键不存在或已过期。
    Missing,
    /// 解码成功的值。
    Found(T),
    /// 已回复错误（`WRONGTYPE`、存储错误或损坏）。
    Rejected,
}

/// 读取一个集合键的原始值 blob。
fn load_value(executor: &dyn KeyValueExecutor, namespace: &Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let command = KeyValueCommand { namespace: namespace.clone(), key: key.to_vec(), action: KeyValueAction::Get };
    match executor.execute(None, command) {
        Ok(KeyValueResult::Get(value)) => Ok(value),
        Ok(_) => Err("collection lookup returned an unexpected result".into()),
        Err(error) => Err(error.to_string()),
    }
}

/// 写回集合键，保留既有过期时间（Redis 修改集合内容不会清掉 TTL）。
fn store_value(executor: &dyn KeyValueExecutor, namespace: &Namespace, key: &[u8], value: Vec<u8>) -> Result<(), String> {
    let command =
        KeyValueCommand { namespace: namespace.clone(), key: key.to_vec(), action: KeyValueAction::PutKeepingExpiry(value) };
    match executor.execute(None, command) {
        Ok(KeyValueResult::Put { .. }) => Ok(()),
        Ok(_) => Err("collection write returned an unexpected result".into()),
        Err(error) => Err(error.to_string()),
    }
}

/// 删除集合键，集合清空时使用（Redis 不允许空的 hash / list 继续存在）。
fn remove_value(executor: &dyn KeyValueExecutor, namespace: &Namespace, key: &[u8]) -> Result<bool, String> {
    let command = KeyValueCommand { namespace: namespace.clone(), key: key.to_vec(), action: KeyValueAction::Delete };
    match executor.execute(None, command) {
        Ok(KeyValueResult::Delete { removed }) => Ok(removed),
        Ok(_) => Err("collection delete returned an unexpected result".into()),
        Err(error) => Err(error.to_string()),
    }
}

/// `WRONGTYPE ...` 错误，与真机使用同一个错误前缀与文案。
pub(crate) fn write_wrong_type(stream: &mut dyn io::Write) -> io::Result<()> {
    stream.write_all(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

fn write_arity_error(stream: &mut dyn io::Write, command: &str) -> io::Result<()> {
    write_error(stream, &format!("wrong number of arguments for '{command}' command"))
}

fn write_integer(stream: &mut dyn io::Write, value: i64) -> io::Result<()> {
    write!(stream, ":{value}\r\n")
}

fn write_array_header(stream: &mut dyn io::Write, length: usize) -> io::Result<()> {
    write!(stream, "*{length}\r\n")
}

fn write_storage_error(stream: &mut dyn io::Write, message: &str) -> io::Result<()> {
    write_error(stream, message)
}

/// u32 长度前缀的读写，用于两类 blob 的自描述编码。
fn read_u32(bytes: &[u8]) -> Option<(u32, &[u8])> {
    if bytes.len() < 4 {
        return None;
    }
    let head = [bytes[0], bytes[1], bytes[2], bytes[3]];
    Some((u32::from_le_bytes(head), &bytes[4..]))
}

fn read_field(bytes: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    let (length, tail) = read_u32(bytes)?;
    let length = length as usize;
    if tail.len() < length {
        return None;
    }
    Some((tail[..length].to_vec(), &tail[length..]))
}

fn push_u32(out: &mut Vec<u8>, value: usize) -> Option<()> {
    out.extend_from_slice(&u32::try_from(value).ok()?.to_le_bytes());
    Some(())
}

/// hash 值编码：`u32 字段数`，随后每字段 `u32 名长 + 名 + u32 值长 + 值`。
///
/// 保持插入顺序，更新已有字段时原位替换，对应真机小哈希（listpack）的字段顺序。
pub(crate) fn encode_hash(fields: &[(Vec<u8>, Vec<u8>)]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    push_u32(&mut out, fields.len())?;
    for (field, value) in fields {
        push_u32(&mut out, field.len())?;
        out.extend_from_slice(field);
        push_u32(&mut out, value.len())?;
        out.extend_from_slice(value);
    }
    Some(out)
}

/// 解码 hash 值；长度不合法或数据截断时返回 `None`。
pub(crate) fn decode_hash(bytes: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    let (count, mut rest) = read_u32(bytes)?;
    let mut fields = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (field, tail) = read_field(rest)?;
        let (value, tail) = read_field(tail)?;
        fields.push((field, value));
        rest = tail;
    }
    if rest.is_empty() { Some(fields) } else { None }
}

/// list 值编码：`u32 元素数`，随后每个元素 `u32 长度 + 字节`，顺序从表头到表尾。
pub(crate) fn encode_list(elements: &[Vec<u8>]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    push_u32(&mut out, elements.len())?;
    for element in elements {
        push_u32(&mut out, element.len())?;
        out.extend_from_slice(element);
    }
    Some(out)
}

/// 解码 list 值；长度不合法或数据截断时返回 `None`。
pub(crate) fn decode_list(bytes: &[u8]) -> Option<Vec<Vec<u8>>> {
    let (count, mut rest) = read_u32(bytes)?;
    let mut elements = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (element, tail) = read_field(rest)?;
        elements.push(element);
        rest = tail;
    }
    if rest.is_empty() { Some(elements) } else { None }
}

/// hash 命令名集合。
pub(crate) fn is_hash_command(command: &[u8]) -> bool {
    for name in [b"HSET".as_slice(), b"HGET", b"HGETALL", b"HDEL", b"HLEN"] {
        if command.eq_ignore_ascii_case(name) {
            return true;
        }
    }
    false
}

/// list 命令名集合。
pub(crate) fn is_list_command(command: &[u8]) -> bool {
    for name in [b"LPUSH".as_slice(), b"RPUSH", b"LRANGE", b"LPOP", b"RPOP", b"LLEN"] {
        if command.eq_ignore_ascii_case(name) {
            return true;
        }
    }
    false
}

/// 载入 hash 字段并校验类型。
fn load_hash(
    stream: &mut dyn io::Write,
    executor: &dyn KeyValueExecutor,
    base: &Namespace,
    key: &[u8],
) -> io::Result<Loaded<Vec<(Vec<u8>, Vec<u8>)>>> {
    match find_kind(executor, base, key) {
        Ok(None) => return Ok(Loaded::Missing),
        Ok(Some(KeyKind::Hash)) => {}
        Ok(Some(_)) => {
            write_wrong_type(stream)?;
            return Ok(Loaded::Rejected);
        }
        Err(message) => {
            write_storage_error(stream, &message)?;
            return Ok(Loaded::Rejected);
        }
    }
    match load_value(executor, &KeyKind::Hash.namespace(base), key) {
        Ok(Some(bytes)) => match decode_hash(&bytes) {
            Some(fields) => Ok(Loaded::Found(fields)),
            None => {
                write_error(stream, "stored hash value is corrupt")?;
                Ok(Loaded::Rejected)
            }
        },
        Ok(None) => Ok(Loaded::Missing),
        Err(message) => {
            write_storage_error(stream, &message)?;
            Ok(Loaded::Rejected)
        }
    }
}

/// 载入 list 元素并校验类型。
fn load_list(
    stream: &mut dyn io::Write,
    executor: &dyn KeyValueExecutor,
    base: &Namespace,
    key: &[u8],
) -> io::Result<Loaded<Vec<Vec<u8>>>> {
    match find_kind(executor, base, key) {
        Ok(None) => return Ok(Loaded::Missing),
        Ok(Some(KeyKind::List)) => {}
        Ok(Some(_)) => {
            write_wrong_type(stream)?;
            return Ok(Loaded::Rejected);
        }
        Err(message) => {
            write_storage_error(stream, &message)?;
            return Ok(Loaded::Rejected);
        }
    }
    match load_value(executor, &KeyKind::List.namespace(base), key) {
        Ok(Some(bytes)) => match decode_list(&bytes) {
            Some(elements) => Ok(Loaded::Found(elements)),
            None => {
                write_error(stream, "stored list value is corrupt")?;
                Ok(Loaded::Rejected)
            }
        },
        Ok(None) => Ok(Loaded::Missing),
        Err(message) => {
            write_storage_error(stream, &message)?;
            Ok(Loaded::Rejected)
        }
    }
}

/// 整数参数校验，与真机 `ERR value is not an integer or out of range` 对齐。
fn parse_index(raw: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(raw).ok()?;
    let value = text.parse::<i64>().ok()?;
    if value.to_string() != text { None } else { Some(value) }
}

/// `HSET` / `HGET` / `HGETALL` / `HDEL` / `HLEN` 的分派入口。
pub(crate) fn handle_hash(
    stream: &mut dyn io::Write,
    request: &Request<'_>,
    executor: Option<&dyn KeyValueExecutor>,
    base: &Namespace,
    protocol: u8,
) -> io::Result<()> {
    let command = request.arguments[0];
    let Some(executor) = executor
    else {
        return write_error(stream, "unsupported command");
    };
    if command.eq_ignore_ascii_case(b"HSET") {
        if request.arguments.len() < 4 || (request.arguments.len() - 2) % 2 != 0 {
            return write_arity_error(stream, "hset");
        }
        let key = request.arguments[1];
        // 字符串或 list 键一律 WRONGTYPE，集合类型之间也不隐式转换。
        match find_kind(executor, base, key) {
            Ok(None) | Ok(Some(KeyKind::Hash)) => {}
            Ok(Some(_)) => return write_wrong_type(stream),
            Err(message) => return write_storage_error(stream, &message),
        }
        let mut fields = match load_value(executor, &KeyKind::Hash.namespace(base), key) {
            Ok(Some(bytes)) => match decode_hash(&bytes) {
                Some(fields) => fields,
                None => return write_error(stream, "stored hash value is corrupt"),
            },
            Ok(None) => Vec::new(),
            Err(message) => return write_storage_error(stream, &message),
        };
        let mut added = 0_i64;
        for pair in request.arguments[2..].chunks(2) {
            let field = pair[0].to_vec();
            let value = pair[1].to_vec();
            match fields.iter_mut().find(|(name, _)| *name == field) {
                Some(entry) => entry.1 = value,
                None => {
                    fields.push((field, value));
                    added += 1;
                }
            }
        }
        let Some(bytes) = encode_hash(&fields)
        else {
            return write_error(stream, "hash exceeds the storage limit");
        };
        if let Err(message) = store_value(executor, &KeyKind::Hash.namespace(base), key, bytes) {
            return write_storage_error(stream, &message);
        }
        return write_integer(stream, added);
    }
    if command.eq_ignore_ascii_case(b"HGET") {
        if request.arguments.len() != 3 {
            return write_arity_error(stream, "hget");
        }
        let key = request.arguments[1];
        return match load_hash(stream, executor, base, key)? {
            Loaded::Missing => write_null(stream, protocol),
            Loaded::Found(fields) => match fields.iter().find(|(name, _)| name.as_slice() == request.arguments[2]) {
                Some((_, value)) => write_bulk(stream, value),
                None => write_null(stream, protocol),
            },
            Loaded::Rejected => Ok(()),
        };
    }
    if command.eq_ignore_ascii_case(b"HGETALL") {
        if request.arguments.len() != 2 {
            return write_arity_error(stream, "hgetall");
        }
        let key = request.arguments[1];
        return match load_hash(stream, executor, base, key)? {
            // 键不存在时真机回空数组，而不是 nil。
            Loaded::Missing => write_array_header(stream, 0),
            Loaded::Found(fields) => {
                write_array_header(stream, fields.len() * 2)?;
                for (field, value) in fields {
                    write_bulk(stream, &field)?;
                    write_bulk(stream, &value)?;
                }
                Ok(())
            }
            Loaded::Rejected => Ok(()),
        };
    }
    if command.eq_ignore_ascii_case(b"HDEL") {
        if request.arguments.len() < 3 {
            return write_arity_error(stream, "hdel");
        }
        let key = request.arguments[1];
        let fields = match load_hash(stream, executor, base, key)? {
            Loaded::Missing => return write_integer(stream, 0),
            Loaded::Found(fields) => fields,
            Loaded::Rejected => return Ok(()),
        };
        let mut fields = fields;
        let mut removed = 0_i64;
        let names = &request.arguments[2..];
        fields.retain(|(name, _)| {
            let hit = names.iter().any(|candidate| candidate == name);
            if hit {
                removed += 1;
            }
            !hit
        });
        if fields.is_empty() {
            if let Err(message) = remove_value(executor, &KeyKind::Hash.namespace(base), key) {
                return write_storage_error(stream, &message);
            }
        }
        else {
            let Some(bytes) = encode_hash(&fields)
            else {
                return write_error(stream, "hash exceeds the storage limit");
            };
            if let Err(message) = store_value(executor, &KeyKind::Hash.namespace(base), key, bytes) {
                return write_storage_error(stream, &message);
            }
        }
        return write_integer(stream, removed);
    }
    // HLEN
    if request.arguments.len() != 2 {
        return write_arity_error(stream, "hlen");
    }
    let key = request.arguments[1];
    match load_hash(stream, executor, base, key)? {
        Loaded::Missing => write_integer(stream, 0),
        Loaded::Found(fields) => write_integer(stream, fields.len() as i64),
        Loaded::Rejected => Ok(()),
    }
}

/// `LPUSH` / `RPUSH` / `LRANGE` / `LPOP` / `RPOP` / `LLEN` 的分派入口。
pub(crate) fn handle_list(
    stream: &mut dyn io::Write,
    request: &Request<'_>,
    executor: Option<&dyn KeyValueExecutor>,
    base: &Namespace,
    protocol: u8,
) -> io::Result<()> {
    let command = request.arguments[0];
    let Some(executor) = executor
    else {
        return write_error(stream, "unsupported command");
    };
    if command.eq_ignore_ascii_case(b"LPUSH") || command.eq_ignore_ascii_case(b"RPUSH") {
        let insert_at_head = command.eq_ignore_ascii_case(b"LPUSH");
        let name = if insert_at_head { "lpush" } else { "rpush" };
        if request.arguments.len() < 3 {
            return write_arity_error(stream, name);
        }
        let key = request.arguments[1];
        match find_kind(executor, base, key) {
            Ok(None) | Ok(Some(KeyKind::List)) => {}
            Ok(Some(_)) => return write_wrong_type(stream),
            Err(message) => return write_storage_error(stream, &message),
        }
        let mut elements = match load_value(executor, &KeyKind::List.namespace(base), key) {
            Ok(Some(bytes)) => match decode_list(&bytes) {
                Some(elements) => elements,
                None => return write_error(stream, "stored list value is corrupt"),
            },
            Ok(None) => Vec::new(),
            Err(message) => return write_storage_error(stream, &message),
        };
        for value in &request.arguments[2..] {
            if insert_at_head {
                elements.insert(0, value.to_vec());
            }
            else {
                elements.push(value.to_vec());
            }
        }
        let Some(bytes) = encode_list(&elements)
        else {
            return write_error(stream, "list exceeds the storage limit");
        };
        if let Err(message) = store_value(executor, &KeyKind::List.namespace(base), key, bytes) {
            return write_storage_error(stream, &message);
        }
        return write_integer(stream, elements.len() as i64);
    }
    if command.eq_ignore_ascii_case(b"LRANGE") {
        if request.arguments.len() != 4 {
            return write_arity_error(stream, "lrange");
        }
        let (Some(start), Some(stop)) = (parse_index(request.arguments[2]), parse_index(request.arguments[3]))
        else {
            return write_error(stream, "value is not an integer or out of range");
        };
        let key = request.arguments[1];
        let elements = match load_list(stream, executor, base, key)? {
            Loaded::Missing => return write_array_header(stream, 0),
            Loaded::Found(elements) => elements,
            Loaded::Rejected => return Ok(()),
        };
        let length = elements.len() as i64;
        let mut begin = if start < 0 { length.saturating_add(start) } else { start };
        let mut end = if stop < 0 { length.saturating_add(stop) } else { stop };
        begin = begin.max(0);
        end = end.min(length - 1);
        if begin > end || begin >= length || end < 0 {
            return write_array_header(stream, 0);
        }
        let slice = &elements[begin as usize..=end as usize];
        write_array_header(stream, slice.len())?;
        for element in slice {
            write_bulk(stream, element)?;
        }
        return Ok(());
    }
    if command.eq_ignore_ascii_case(b"LPOP") || command.eq_ignore_ascii_case(b"RPOP") {
        let pop_head = command.eq_ignore_ascii_case(b"LPOP");
        let name = if pop_head { "lpop" } else { "rpop" };
        if request.arguments.len() < 2 || request.arguments.len() > 3 {
            return write_arity_error(stream, name);
        }
        let with_count = request.arguments.len() == 3;
        let count = if with_count {
            match parse_index(request.arguments[2]) {
                Some(count) if count > 0 => count as usize,
                Some(_) => return write_error(stream, "value is out of range, must be positive"),
                None => return write_error(stream, "value is not an integer or out of range"),
            }
        }
        else {
            1
        };
        let key = request.arguments[1];
        let elements = match load_list(stream, executor, base, key)? {
            Loaded::Missing => {
                return if with_count { write_null_array(stream, protocol) } else { write_null(stream, protocol) };
            }
            Loaded::Found(elements) => elements,
            Loaded::Rejected => return Ok(()),
        };
        let take = count.min(elements.len());
        let popped: Vec<Vec<u8>> = if pop_head {
            elements[..take].to_vec()
        }
        else {
            // 真机 `RPOP key count` 从表尾开始逐个弹出，回复顺序是尾端在前
            // （`[a,b,c,d]` 取 2 回 `[d,c]`），因此这里要再倒序一次。
            elements[elements.len() - take..].iter().rev().cloned().collect()
        };
        let mut remaining = elements;
        if pop_head {
            remaining.drain(..take);
        }
        else {
            remaining.truncate(remaining.len() - take);
        }
        if remaining.is_empty() {
            if let Err(message) = remove_value(executor, &KeyKind::List.namespace(base), key) {
                return write_storage_error(stream, &message);
            }
        }
        else {
            let Some(bytes) = encode_list(&remaining)
            else {
                return write_error(stream, "list exceeds the storage limit");
            };
            if let Err(message) = store_value(executor, &KeyKind::List.namespace(base), key, bytes) {
                return write_storage_error(stream, &message);
            }
        }
        if with_count {
            write_array_header(stream, popped.len())?;
            for element in &popped {
                write_bulk(stream, element)?;
            }
            return Ok(());
        }
        // 单值弹出：真机保持 bulk 回复，`count` 缺省时不会回数组。
        return match popped.first() {
            Some(element) => write_bulk(stream, element),
            None => write_null(stream, protocol),
        };
    }
    // LLEN
    if request.arguments.len() != 2 {
        return write_arity_error(stream, "llen");
    }
    let key = request.arguments[1];
    match load_list(stream, executor, base, key)? {
        Loaded::Missing => write_integer(stream, 0),
        Loaded::Found(elements) => write_integer(stream, elements.len() as i64),
        Loaded::Rejected => Ok(()),
    }
}
