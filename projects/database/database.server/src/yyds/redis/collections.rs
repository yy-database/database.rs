//! Redis 的 hash / list / set / zset 值类型，落在既有扁平键值合同之上。
//!
//! `.yykv` v3 的字节布局不能改动，`Record` 也没有类型字段。这里把类型编码在
//! **命名空间** 上：字符串继续使用逻辑库命名空间（`redis:<db>`），hash 落在
//! `redis:<db>:hash`，list 落在 `redis:<db>:list`，set 落在 `redis:<db>:set`，
//! zset 落在 `redis:<db>:zset`。命名空间本来就是 `Record.key` 的一部分，所以不新增
//! 字段、不升格式版本，重启后随快照恢复。
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
    /// 集合，成员按插入顺序保存在一个值 blob 内（真机小集合同样保持插入顺序）。
    Set,
    /// 有序集合，成员与 `f64` 分数保存在一个值 blob 内，读取时按分数再按成员排序。
    ZSet,
}

impl KeyKind {
    /// 所有已知类型，按探测顺序排列（字符串优先，保持既有语义）。
    pub(crate) const ALL: [KeyKind; 5] = [KeyKind::String, KeyKind::Hash, KeyKind::List, KeyKind::Set, KeyKind::ZSet];

    /// `TYPE` 回复里使用的类型名。
    pub(crate) fn name(self) -> &'static str {
        match self {
            KeyKind::String => "string",
            KeyKind::Hash => "hash",
            KeyKind::List => "list",
            KeyKind::Set => "set",
            KeyKind::ZSet => "zset",
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
            KeyKind::Set => Namespace(format!("{}:set", base.0)),
            KeyKind::ZSet => Namespace(format!("{}:zset", base.0)),
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

/// set 值编码：成员列表格式与 list 相同（`u32 成员数` + 每个 `u32 长度 + 字节`），
/// 顺序保留插入顺序，便于小集合的输出稳定。
pub(crate) fn encode_set(members: &[Vec<u8>]) -> Option<Vec<u8>> {
    encode_list(members)
}

/// 解码 set 值；成员重复检查在写入侧完成，读取侧只做结构校验。
pub(crate) fn decode_set(bytes: &[u8]) -> Option<Vec<Vec<u8>>> {
    decode_list(bytes)
}

/// zset 值编码：`u32 成员数`，随后每个成员 `u32 长度 + 成员 + 8 字节 f64 分数`。
pub(crate) fn encode_zset(entries: &[(Vec<u8>, f64)]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    push_u32(&mut out, entries.len())?;
    for (member, score) in entries {
        push_u32(&mut out, member.len())?;
        out.extend_from_slice(member);
        out.extend_from_slice(&score.to_bits().to_le_bytes());
    }
    Some(out)
}

/// 解码 zset 值；尾随字节非空或数据截断时返回 `None`。
pub(crate) fn decode_zset(bytes: &[u8]) -> Option<Vec<(Vec<u8>, f64)>> {
    let (count, mut rest) = read_u32(bytes)?;
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (member, tail) = read_field(rest)?;
        if tail.len() < 8 {
            return None;
        }
        let bits = u64::from_le_bytes([tail[0], tail[1], tail[2], tail[3], tail[4], tail[5], tail[6], tail[7]]);
        entries.push((member, f64::from_bits(bits)));
        rest = &tail[8..];
    }
    if rest.is_empty() { Some(entries) } else { None }
}

/// hash 命令名集合。
pub(crate) fn is_hash_command(command: &[u8]) -> bool {
    for name in [
        b"HSET".as_slice(),
        b"HGET",
        b"HGETALL",
        b"HDEL",
        b"HLEN",
        b"HINCRBY",
        b"HKEYS",
        b"HVALS",
        b"HEXISTS",
        b"HINCRBYFLOAT",
        b"HSCAN",
    ] {
        if command.eq_ignore_ascii_case(name) {
            return true;
        }
    }
    false
}

/// list 命令名集合。
pub(crate) fn is_list_command(command: &[u8]) -> bool {
    for name in [b"LPUSH".as_slice(), b"RPUSH", b"LRANGE", b"LPOP", b"RPOP", b"LLEN", b"LINDEX"] {
        if command.eq_ignore_ascii_case(name) {
            return true;
        }
    }
    false
}

/// set 命令名集合。
pub(crate) fn is_set_command(command: &[u8]) -> bool {
    for name in [
        b"SADD".as_slice(),
        b"SREM",
        b"SMEMBERS",
        b"SCARD",
        b"SISMEMBER",
        b"SINTER",
        b"SUNION",
        b"SDIFF",
        b"SINTERSTORE",
        b"SUNIONSTORE",
        b"SDIFFSTORE",
        b"SMOVE",
        b"SRANDMEMBER",
        b"SSCAN",
    ] {
        if command.eq_ignore_ascii_case(name) {
            return true;
        }
    }
    false
}

/// zset 命令名集合。
pub(crate) fn is_zset_command(command: &[u8]) -> bool {
    for name in [
        b"ZADD".as_slice(),
        b"ZSCORE",
        b"ZREM",
        b"ZCARD",
        b"ZRANGE",
        b"ZRANGEBYSCORE",
        b"ZREVRANGEBYSCORE",
        b"ZRANGEBYLEX",
        b"ZREVRANGEBYLEX",
        b"ZRANK",
        b"ZREVRANK",
        b"ZINCRBY",
        b"ZCOUNT",
        b"ZREMRANGEBYRANK",
        b"ZREMRANGEBYSCORE",
        b"ZSCAN",
    ] {
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

/// 载入 set 成员并校验类型。
fn load_set(
    stream: &mut dyn io::Write,
    executor: &dyn KeyValueExecutor,
    base: &Namespace,
    key: &[u8],
) -> io::Result<Loaded<Vec<Vec<u8>>>> {
    match find_kind(executor, base, key) {
        Ok(None) => return Ok(Loaded::Missing),
        Ok(Some(KeyKind::Set)) => {}
        Ok(Some(_)) => {
            write_wrong_type(stream)?;
            return Ok(Loaded::Rejected);
        }
        Err(message) => {
            write_storage_error(stream, &message)?;
            return Ok(Loaded::Rejected);
        }
    }
    match load_value(executor, &KeyKind::Set.namespace(base), key) {
        Ok(Some(bytes)) => match decode_set(&bytes) {
            Some(members) => Ok(Loaded::Found(members)),
            None => {
                write_error(stream, "stored set value is corrupt")?;
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

/// 载入 zset 成员并校验类型。
fn load_zset(
    stream: &mut dyn io::Write,
    executor: &dyn KeyValueExecutor,
    base: &Namespace,
    key: &[u8],
) -> io::Result<Loaded<Vec<(Vec<u8>, f64)>>> {
    match find_kind(executor, base, key) {
        Ok(None) => return Ok(Loaded::Missing),
        Ok(Some(KeyKind::ZSet)) => {}
        Ok(Some(_)) => {
            write_wrong_type(stream)?;
            return Ok(Loaded::Rejected);
        }
        Err(message) => {
            write_storage_error(stream, &message)?;
            return Ok(Loaded::Rejected);
        }
    }
    match load_value(executor, &KeyKind::ZSet.namespace(base), key) {
        Ok(Some(bytes)) => match decode_zset(&bytes) {
            Some(entries) => Ok(Loaded::Found(entries)),
            None => {
                write_error(stream, "stored zset value is corrupt")?;
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
pub(crate) fn parse_index(raw: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(raw).ok()?;
    let value = text.parse::<i64>().ok()?;
    if value.to_string() != text { None } else { Some(value) }
}

/// zset 分数解析：接受浮点字面量，拒绝 `NaN`（真机不允许 NaN 分数）。
fn parse_score(raw: &[u8]) -> Option<f64> {
    let text = std::str::from_utf8(raw).ok()?;
    let score = text.parse::<f64>().ok()?;
    if score.is_nan() { None } else { Some(score) }
}

/// zset 排序：先按分数升序，同分再按成员字节字典序。
fn sort_zset(entries: &mut [(Vec<u8>, f64)]) {
    entries.sort_by(|left, right| left.1.total_cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
}

/// 分数回复格式：最短十进制表示，与真机 `ZSCORE` 一致（整数分数不带 `.0`）。
fn write_score(stream: &mut dyn io::Write, score: f64) -> io::Result<()> {
    write_bulk(stream, score.to_string().as_bytes())
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
    if command.eq_ignore_ascii_case(b"HINCRBY") {
        if request.arguments.len() != 4 {
            return write_arity_error(stream, "hincrby");
        }
        let delta = match parse_index(request.arguments[3]) {
            Some(delta) => delta,
            None => return write_error(stream, "value is not an integer or out of range"),
        };
        let key = request.arguments[1];
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
        let field = request.arguments[2].to_vec();
        let current = match fields.iter().find(|(name, _)| *name == field) {
            Some((_, value)) => match parse_index(value) {
                Some(current) => current,
                None => return write_error(stream, "hash value is not an integer"),
            },
            None => 0,
        };
        let Some(updated) = current.checked_add(delta)
        else {
            return write_error(stream, "increment or decrement would overflow");
        };
        let encoded = updated.to_string().into_bytes();
        match fields.iter_mut().find(|(name, _)| *name == field) {
            Some(entry) => entry.1 = encoded,
            None => fields.push((field, encoded)),
        }
        let Some(bytes) = encode_hash(&fields)
        else {
            return write_error(stream, "hash exceeds the storage limit");
        };
        if let Err(message) = store_value(executor, &KeyKind::Hash.namespace(base), key, bytes) {
            return write_storage_error(stream, &message);
        }
        return write_integer(stream, updated);
    }
    if command.eq_ignore_ascii_case(b"HKEYS") || command.eq_ignore_ascii_case(b"HVALS") {
        let by_name = command.eq_ignore_ascii_case(b"HKEYS");
        if request.arguments.len() != 2 {
            let name = if by_name { "hkeys" } else { "hvals" };
            return write_arity_error(stream, name);
        }
        let key = request.arguments[1];
        let fields = match load_hash(stream, executor, base, key)? {
            Loaded::Missing => Vec::new(),
            Loaded::Found(fields) => fields,
            Loaded::Rejected => return Ok(()),
        };
        write_array_header(stream, fields.len())?;
        for (field, value) in &fields {
            write_bulk(stream, if by_name { field } else { value })?;
        }
        return Ok(());
    }
    if command.eq_ignore_ascii_case(b"HEXISTS") {
        if request.arguments.len() != 3 {
            return write_arity_error(stream, "hexists");
        }
        let key = request.arguments[1];
        return match load_hash(stream, executor, base, key)? {
            Loaded::Missing => write_integer(stream, 0),
            Loaded::Found(fields) => {
                write_integer(stream, i64::from(fields.iter().any(|(name, _)| name.as_slice() == request.arguments[2])))
            }
            Loaded::Rejected => Ok(()),
        };
    }
    if command.eq_ignore_ascii_case(b"HINCRBYFLOAT") {
        if request.arguments.len() != 4 {
            return write_arity_error(stream, "hincrbyfloat");
        }
        let Some(increment) = parse_float_literal(std::str::from_utf8(request.arguments[3]).unwrap_or(""))
        else {
            return write_error(stream, "value is not a valid float");
        };
        let key = request.arguments[1];
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
        let field = request.arguments[2].to_vec();
        let current = match fields.iter().find(|(name, _)| *name == field) {
            Some((_, value)) => match parse_float_literal(std::str::from_utf8(value).unwrap_or("")) {
                Some(current) => current,
                None => return write_error(stream, "hash value is not a float"),
            },
            None => 0.0,
        };
        let updated = current + increment;
        if !updated.is_finite() {
            return write_error(stream, "increment would produce NaN or Infinity");
        }
        let encoded = updated.to_string().into_bytes();
        match fields.iter_mut().find(|(name, _)| *name == field) {
            Some(entry) => entry.1 = encoded.clone(),
            None => fields.push((field, encoded.clone())),
        }
        let Some(bytes) = encode_hash(&fields)
        else {
            return write_error(stream, "hash exceeds the storage limit");
        };
        if let Err(message) = store_value(executor, &KeyKind::Hash.namespace(base), key, bytes) {
            return write_storage_error(stream, &message);
        }
        return write_bulk(stream, &encoded);
    }
    if command.eq_ignore_ascii_case(b"HSCAN") {
        if request.arguments.len() < 3 {
            return write_arity_error(stream, "hscan");
        }
        let Some(cursor) = parse_scan_cursor(stream, request.arguments[2])?
        else {
            return Ok(());
        };
        let Some((pattern, count)) = parse_scan_tail(stream, &request.arguments, 3)?
        else {
            return Ok(());
        };
        let key = request.arguments[1];
        let fields = match load_hash(stream, executor, base, key)? {
            Loaded::Missing => Vec::new(),
            Loaded::Found(fields) => fields,
            Loaded::Rejected => return Ok(()),
        };
        return write_hscan_page(stream, &fields, cursor, count, pattern.as_deref());
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
    if command.eq_ignore_ascii_case(b"LINDEX") {
        if request.arguments.len() != 3 {
            return write_arity_error(stream, "lindex");
        }
        let index = match parse_index(request.arguments[2]) {
            Some(index) => index,
            None => return write_error(stream, "value is not an integer or out of range"),
        };
        let key = request.arguments[1];
        return match load_list(stream, executor, base, key)? {
            Loaded::Missing => write_null(stream, protocol),
            Loaded::Found(elements) => {
                let length = elements.len() as i64;
                let position = if index < 0 { length.saturating_add(index) } else { index };
                if position < 0 || position >= length {
                    write_null(stream, protocol)
                }
                else {
                    write_bulk(stream, &elements[position as usize])
                }
            }
            Loaded::Rejected => Ok(()),
        };
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

/// 集合代数运算。
#[derive(Clone, Copy)]
enum SetOp {
    Inter,
    Union,
    Diff,
}

/// 删除同一逻辑键的全部类型表示，供 `*STORE` 覆盖目标与 `COPY REPLACE` 使用。
fn remove_all_kinds(executor: &dyn KeyValueExecutor, base: &Namespace, key: &[u8]) -> Result<(), String> {
    for kind in KeyKind::ALL {
        remove_value(executor, &kind.namespace(base), key)?;
    }
    Ok(())
}

/// 无条件写入一个值（新记录不带过期时间），对应 `*STORE` 覆盖目标的真机语义。
fn store_fresh_value(
    executor: &dyn KeyValueExecutor,
    namespace: &Namespace,
    key: &[u8],
    value: Vec<u8>,
) -> Result<(), String> {
    let command = KeyValueCommand { namespace: namespace.clone(), key: key.to_vec(), action: KeyValueAction::Put(value) };
    match executor.execute(None, command) {
        Ok(KeyValueResult::Put { .. }) => Ok(()),
        Ok(_) => Err("fresh write returned an unexpected result".into()),
        Err(error) => Err(error.to_string()),
    }
}

/// 读取多个集合并校验类型；任一键类型不合法时整条命令按真机回 `WRONGTYPE`。
fn load_sets(
    stream: &mut dyn io::Write,
    executor: &dyn KeyValueExecutor,
    base: &Namespace,
    keys: &[&[u8]],
) -> io::Result<Option<Vec<Vec<Vec<u8>>>>> {
    let mut sets = Vec::with_capacity(keys.len());
    for key in keys {
        match load_set(stream, executor, base, key)? {
            Loaded::Missing => sets.push(Vec::new()),
            Loaded::Found(members) => sets.push(members),
            Loaded::Rejected => return Ok(None),
        }
    }
    Ok(Some(sets))
}

/// 按真机语义计算交集、并集或差集，保留第一个操作数的成员顺序。
fn apply_set_op(op: SetOp, sets: &[Vec<Vec<u8>>]) -> Vec<Vec<u8>> {
    match op {
        SetOp::Union => {
            let mut union: Vec<Vec<u8>> = Vec::new();
            for set in sets {
                for member in set {
                    if !union.iter().any(|existing| existing == member) {
                        union.push(member.clone());
                    }
                }
            }
            union
        }
        SetOp::Inter => {
            let mut result = sets.first().cloned().unwrap_or_default();
            result.retain(|member| sets.iter().all(|set| set.iter().any(|candidate| candidate == member)));
            result
        }
        SetOp::Diff => {
            let mut result = sets.first().cloned().unwrap_or_default();
            for set in &sets[1..] {
                result.retain(|member| !set.iter().any(|candidate| candidate == member));
            }
            result
        }
    }
}

/// 进程内伪随机源：没有引入 `rand` 依赖，用时钟纳秒做种子跑 xorshift64。
fn random_state() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(1);
    nanos ^ 0x9E37_79B9_7F4A_7C15
}

fn next_random(state: &mut u64) -> u64 {
    let mut value = *state;
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    *state = value;
    value
}

/// 解析集合扫描的游标：只接受规范十进制写法。
fn parse_scan_cursor(stream: &mut dyn io::Write, raw: &[u8]) -> io::Result<Option<usize>> {
    let text = match std::str::from_utf8(raw) {
        Ok(text) if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) => text,
        _ => {
            write_error(stream, "invalid cursor")?;
            return Ok(None);
        }
    };
    match text.parse::<usize>() {
        Ok(cursor) => Ok(Some(cursor)),
        Err(_) => {
            write_error(stream, "invalid cursor")?;
            Ok(None)
        }
    }
}

/// 解析 `MATCH pattern` 与 `COUNT count` 选项。
fn parse_scan_tail(
    stream: &mut dyn io::Write,
    arguments: &[&[u8]],
    start: usize,
) -> io::Result<Option<(Option<Vec<u8>>, usize)>> {
    let mut pattern: Option<Vec<u8>> = None;
    let mut count = 10usize;
    let mut index = start;
    while index < arguments.len() {
        if index + 1 >= arguments.len() {
            write_error(stream, "syntax error")?;
            return Ok(None);
        }
        if arguments[index].eq_ignore_ascii_case(b"MATCH") {
            pattern = Some(arguments[index + 1].to_vec());
        }
        else if arguments[index].eq_ignore_ascii_case(b"COUNT") {
            let Ok(value) = std::str::from_utf8(arguments[index + 1]).unwrap_or("").parse::<u64>()
            else {
                write_error(stream, "syntax error")?;
                return Ok(None);
            };
            if value == 0 {
                write_error(stream, "syntax error")?;
                return Ok(None);
            }
            count = value.min(usize::MAX as u64) as usize;
        }
        else {
            write_error(stream, "syntax error")?;
            return Ok(None);
        }
        index += 2;
    }
    Ok(Some((pattern, count)))
}

/// 写出一段集合扫描结果：`[next-cursor, [members...]]`。
fn write_scan_page(
    stream: &mut dyn io::Write,
    items: &[Vec<u8>],
    cursor: usize,
    count: usize,
    pattern: Option<&[u8]>,
) -> io::Result<()> {
    if cursor >= items.len() {
        stream.write_all(b"*2\r\n")?;
        write_bulk(stream, b"0")?;
        return write_array_header(stream, 0);
    }
    let end = cursor.saturating_add(count).min(items.len());
    let next = if end >= items.len() { "0".to_string() } else { end.to_string() };
    let matched: Vec<&Vec<u8>> =
        items[cursor..end].iter().filter(|item| pattern.map_or(true, |pattern| glob_match_local(pattern, item))).collect();
    stream.write_all(b"*2\r\n")?;
    write_bulk(stream, next.as_bytes())?;
    write_array_header(stream, matched.len())?;
    for item in matched {
        write_bulk(stream, item)?;
    }
    Ok(())
}

/// 与 `connection::glob_match` 相同的简易 glob，用于集合内扫描。
fn glob_match_local(pattern: &[u8], text: &[u8]) -> bool {
    let (mut pattern_index, mut text_index) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while text_index < text.len() {
        if pattern_index < pattern.len() {
            match pattern[pattern_index] {
                b'*' => {
                    star = Some((pattern_index, text_index));
                    pattern_index += 1;
                    continue;
                }
                b'?' => {
                    pattern_index += 1;
                    text_index += 1;
                    continue;
                }
                b'\\' if pattern_index + 1 < pattern.len() && pattern[pattern_index + 1] == text[text_index] => {
                    pattern_index += 2;
                    text_index += 1;
                    continue;
                }
                byte if byte == text[text_index] => {
                    pattern_index += 1;
                    text_index += 1;
                    continue;
                }
                _ => {}
            }
        }
        match star {
            Some((star_index, resume)) => {
                pattern_index = star_index + 1;
                text_index = resume + 1;
                star = Some((star_index, text_index));
            }
            None => return false,
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

/// `SADD` / `SREM` / `SMEMBERS` / `SCARD` / `SISMEMBER` 及集合代数、移动、随机读取与扫描的分派入口。
pub(crate) fn handle_set(
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
    if command.eq_ignore_ascii_case(b"SADD") {
        if request.arguments.len() < 3 {
            return write_arity_error(stream, "sadd");
        }
        let key = request.arguments[1];
        match find_kind(executor, base, key) {
            Ok(None) | Ok(Some(KeyKind::Set)) => {}
            Ok(Some(_)) => return write_wrong_type(stream),
            Err(message) => return write_storage_error(stream, &message),
        }
        let mut members = match load_value(executor, &KeyKind::Set.namespace(base), key) {
            Ok(Some(bytes)) => match decode_set(&bytes) {
                Some(members) => members,
                None => return write_error(stream, "stored set value is corrupt"),
            },
            Ok(None) => Vec::new(),
            Err(message) => return write_storage_error(stream, &message),
        };
        let mut added = 0_i64;
        for member in &request.arguments[2..] {
            if !members.iter().any(|existing| existing == member) {
                members.push(member.to_vec());
                added += 1;
            }
        }
        // 全是重复成员时不重写值，已有集合连同 TTL 原样保留。
        if added > 0 {
            let Some(bytes) = encode_set(&members)
            else {
                return write_error(stream, "set exceeds the storage limit");
            };
            if let Err(message) = store_value(executor, &KeyKind::Set.namespace(base), key, bytes) {
                return write_storage_error(stream, &message);
            }
        }
        return write_integer(stream, added);
    }
    if command.eq_ignore_ascii_case(b"SREM") {
        if request.arguments.len() < 3 {
            return write_arity_error(stream, "srem");
        }
        let key = request.arguments[1];
        let mut members = match load_set(stream, executor, base, key)? {
            Loaded::Missing => return write_integer(stream, 0),
            Loaded::Found(members) => members,
            Loaded::Rejected => return Ok(()),
        };
        let mut removed = 0_i64;
        members.retain(|existing| {
            let hit = request.arguments[2..].iter().any(|candidate| candidate == existing);
            if hit {
                removed += 1;
            }
            !hit
        });
        if members.is_empty() {
            if let Err(message) = remove_value(executor, &KeyKind::Set.namespace(base), key) {
                return write_storage_error(stream, &message);
            }
        }
        else {
            let Some(bytes) = encode_set(&members)
            else {
                return write_error(stream, "set exceeds the storage limit");
            };
            if let Err(message) = store_value(executor, &KeyKind::Set.namespace(base), key, bytes) {
                return write_storage_error(stream, &message);
            }
        }
        return write_integer(stream, removed);
    }
    if command.eq_ignore_ascii_case(b"SMEMBERS") {
        if request.arguments.len() != 2 {
            return write_arity_error(stream, "smembers");
        }
        let key = request.arguments[1];
        return match load_set(stream, executor, base, key)? {
            Loaded::Missing => write_array_header(stream, 0),
            Loaded::Found(members) => {
                write_array_header(stream, members.len())?;
                for member in &members {
                    write_bulk(stream, member)?;
                }
                Ok(())
            }
            Loaded::Rejected => Ok(()),
        };
    }
    if command.eq_ignore_ascii_case(b"SCARD") {
        if request.arguments.len() != 2 {
            return write_arity_error(stream, "scard");
        }
        let key = request.arguments[1];
        return match load_set(stream, executor, base, key)? {
            Loaded::Missing => write_integer(stream, 0),
            Loaded::Found(members) => write_integer(stream, members.len() as i64),
            Loaded::Rejected => Ok(()),
        };
    }
    if command.eq_ignore_ascii_case(b"SINTER")
        || command.eq_ignore_ascii_case(b"SUNION")
        || command.eq_ignore_ascii_case(b"SDIFF")
    {
        if request.arguments.len() < 2 {
            let name = String::from_utf8_lossy(command).to_ascii_lowercase();
            return write_arity_error(stream, &name);
        }
        let op = if command.eq_ignore_ascii_case(b"SINTER") {
            SetOp::Inter
        }
        else if command.eq_ignore_ascii_case(b"SUNION") {
            SetOp::Union
        }
        else {
            SetOp::Diff
        };
        let keys: Vec<&[u8]> = request.arguments[1..].to_vec();
        let Some(sets) = load_sets(stream, executor, base, &keys)?
        else {
            return Ok(());
        };
        let result = apply_set_op(op, &sets);
        write_array_header(stream, result.len())?;
        for member in &result {
            write_bulk(stream, member)?;
        }
        return Ok(());
    }
    if command.eq_ignore_ascii_case(b"SINTERSTORE")
        || command.eq_ignore_ascii_case(b"SUNIONSTORE")
        || command.eq_ignore_ascii_case(b"SDIFFSTORE")
    {
        if request.arguments.len() < 3 {
            let name = String::from_utf8_lossy(command).to_ascii_lowercase();
            return write_arity_error(stream, &name);
        }
        let op = if command.eq_ignore_ascii_case(b"SINTERSTORE") {
            SetOp::Inter
        }
        else if command.eq_ignore_ascii_case(b"SUNIONSTORE") {
            SetOp::Union
        }
        else {
            SetOp::Diff
        };
        let destination = request.arguments[1];
        let keys: Vec<&[u8]> = request.arguments[2..].to_vec();
        let Some(sets) = load_sets(stream, executor, base, &keys)?
        else {
            return Ok(());
        };
        let result = apply_set_op(op, &sets);
        // `*STORE` 覆盖目标且不继承旧过期时间；结果为空时目标键整体删除。
        if let Err(message) = remove_all_kinds(executor, base, destination) {
            return write_storage_error(stream, &message);
        }
        if !result.is_empty() {
            let Some(bytes) = encode_set(&result)
            else {
                return write_error(stream, "set exceeds the storage limit");
            };
            if let Err(message) = store_fresh_value(executor, &KeyKind::Set.namespace(base), destination, bytes) {
                return write_storage_error(stream, &message);
            }
        }
        return write_integer(stream, result.len() as i64);
    }
    if command.eq_ignore_ascii_case(b"SMOVE") {
        if request.arguments.len() != 4 {
            return write_arity_error(stream, "smove");
        }
        let source = request.arguments[1];
        let destination = request.arguments[2];
        let member = request.arguments[3];
        if source == destination {
            return match load_set(stream, executor, base, source)? {
                Loaded::Missing => write_integer(stream, 0),
                Loaded::Found(members) => {
                    write_integer(stream, i64::from(members.iter().any(|existing| existing.as_slice() == member)))
                }
                Loaded::Rejected => Ok(()),
            };
        }
        // 源键不存在时真机直接回 0，不再探测目标类型。
        let mut members = match load_set(stream, executor, base, source)? {
            Loaded::Missing => return write_integer(stream, 0),
            Loaded::Found(members) => members,
            Loaded::Rejected => return Ok(()),
        };
        match find_kind(executor, base, destination) {
            Ok(None) | Ok(Some(KeyKind::Set)) => {}
            Ok(Some(_)) => return write_wrong_type(stream),
            Err(message) => return write_storage_error(stream, &message),
        }
        let Some(position) = members.iter().position(|existing| existing.as_slice() == member)
        else {
            return write_integer(stream, 0);
        };
        members.remove(position);
        if members.is_empty() {
            if let Err(message) = remove_value(executor, &KeyKind::Set.namespace(base), source) {
                return write_storage_error(stream, &message);
            }
        }
        else {
            let Some(bytes) = encode_set(&members)
            else {
                return write_error(stream, "set exceeds the storage limit");
            };
            if let Err(message) = store_value(executor, &KeyKind::Set.namespace(base), source, bytes) {
                return write_storage_error(stream, &message);
            }
        }
        let mut destination_members = match load_value(executor, &KeyKind::Set.namespace(base), destination) {
            Ok(Some(bytes)) => match decode_set(&bytes) {
                Some(members) => members,
                None => return write_error(stream, "stored set value is corrupt"),
            },
            Ok(None) => Vec::new(),
            Err(message) => return write_storage_error(stream, &message),
        };
        if !destination_members.iter().any(|existing| existing.as_slice() == member) {
            destination_members.push(member.to_vec());
        }
        let Some(bytes) = encode_set(&destination_members)
        else {
            return write_error(stream, "set exceeds the storage limit");
        };
        if let Err(message) = store_value(executor, &KeyKind::Set.namespace(base), destination, bytes) {
            return write_storage_error(stream, &message);
        }
        return write_integer(stream, 1);
    }
    if command.eq_ignore_ascii_case(b"SRANDMEMBER") {
        if request.arguments.len() < 2 || request.arguments.len() > 3 {
            return write_arity_error(stream, "srandmember");
        }
        let key = request.arguments[1];
        let members = match load_set(stream, executor, base, key)? {
            Loaded::Missing => {
                return if request.arguments.len() == 3 {
                    write_array_header(stream, 0)
                }
                else {
                    write_null(stream, protocol)
                };
            }
            Loaded::Found(members) => members,
            Loaded::Rejected => return Ok(()),
        };
        let count = match request.arguments.get(2) {
            None => {
                if members.is_empty() {
                    return write_null(stream, protocol);
                }
                let mut state = random_state();
                let index = (next_random(&mut state) as usize) % members.len();
                return write_bulk(stream, &members[index]);
            }
            Some(raw) => match parse_index(raw) {
                Some(count) => count,
                None => return write_error(stream, "value is not an integer or out of range"),
            },
        };
        if count >= 0 {
            let take = (count as usize).min(members.len());
            let mut order: Vec<usize> = (0..members.len()).collect();
            let mut state = random_state();
            // Fisher-Yates 洗牌后取前 take 个，保证不重复。
            for index in (1..order.len()).rev() {
                let swap = (next_random(&mut state) as usize) % (index + 1);
                order.swap(index, swap);
            }
            write_array_header(stream, take)?;
            for index in order.into_iter().take(take) {
                write_bulk(stream, &members[index])?;
            }
            return Ok(());
        }
        if members.is_empty() {
            return write_array_header(stream, 0);
        }
        write_array_header(stream, count.unsigned_abs() as usize)?;
        let mut state = random_state();
        for _ in 0..count.unsigned_abs() {
            let index = (next_random(&mut state) as usize) % members.len();
            write_bulk(stream, &members[index])?;
        }
        return Ok(());
    }
    if command.eq_ignore_ascii_case(b"SSCAN") {
        if request.arguments.len() < 3 {
            return write_arity_error(stream, "sscan");
        }
        let Some(cursor) = parse_scan_cursor(stream, request.arguments[2])?
        else {
            return Ok(());
        };
        let Some((pattern, count)) = parse_scan_tail(stream, &request.arguments, 3)?
        else {
            return Ok(());
        };
        let key = request.arguments[1];
        let members = match load_set(stream, executor, base, key)? {
            Loaded::Missing => Vec::new(),
            Loaded::Found(members) => members,
            Loaded::Rejected => return Ok(()),
        };
        return write_scan_page(stream, &members, cursor, count, pattern.as_deref());
    }
    // SISMEMBER
    if request.arguments.len() != 3 {
        return write_arity_error(stream, "sismember");
    }
    let key = request.arguments[1];
    match load_set(stream, executor, base, key)? {
        Loaded::Missing => write_integer(stream, 0),
        Loaded::Found(members) => {
            let present = members.iter().any(|member| member.as_slice() == request.arguments[2]);
            write_integer(stream, i64::from(present))
        }
        Loaded::Rejected => Ok(()),
    }
}

/// 分数区间端点：`(` 前缀表示开区间，`-inf` / `+inf` 表示无穷。
#[derive(Clone, Copy)]
struct ScoreBound {
    value: f64,
    exclusive: bool,
}

/// 解析浮点字面量，接受 `-inf` / `+inf` / `inf` 并拒绝 `NaN`。
fn parse_float_literal(text: &str) -> Option<f64> {
    let normalized = text.to_ascii_lowercase();
    if normalized == "-inf" {
        return Some(f64::NEG_INFINITY);
    }
    if normalized == "+inf" || normalized == "inf" {
        return Some(f64::INFINITY);
    }
    let value = text.parse::<f64>().ok()?;
    if value.is_nan() { None } else { Some(value) }
}

fn parse_score_bound(raw: &[u8]) -> Option<ScoreBound> {
    let text = std::str::from_utf8(raw).ok()?;
    if let Some(rest) = text.strip_prefix('(') {
        let value = parse_float_literal(rest)?;
        return Some(ScoreBound { value, exclusive: true });
    }
    let value = parse_float_literal(text)?;
    Some(ScoreBound { value, exclusive: false })
}

fn score_at_least(score: f64, bound: ScoreBound) -> bool {
    if bound.exclusive { score > bound.value } else { score >= bound.value }
}

fn score_at_most(score: f64, bound: ScoreBound) -> bool {
    if bound.exclusive { score < bound.value } else { score <= bound.value }
}

fn in_score_range(score: f64, min: ScoreBound, max: ScoreBound) -> bool {
    score_at_least(score, min) && score_at_most(score, max)
}

/// 字典区间端点：`-` / `+` 表示无界，`[x` 闭区间，`(x` 开区间。
#[derive(Clone)]
enum LexBound {
    NegInf,
    PosInf,
    Value { bytes: Vec<u8>, exclusive: bool },
}

fn parse_lex_bound(raw: &[u8]) -> Option<LexBound> {
    match raw {
        b"-" => Some(LexBound::NegInf),
        b"+" => Some(LexBound::PosInf),
        _ => {
            let (exclusive, rest) = match raw.split_first() {
                Some((b'[', rest)) => (false, rest),
                Some((b'(', rest)) => (true, rest),
                _ => return None,
            };
            Some(LexBound::Value { bytes: rest.to_vec(), exclusive })
        }
    }
}

fn lex_at_least(member: &[u8], bound: &LexBound) -> bool {
    match bound {
        LexBound::NegInf => true,
        LexBound::PosInf => false,
        LexBound::Value { bytes, exclusive } => {
            if *exclusive {
                member > bytes.as_slice()
            }
            else {
                member >= bytes.as_slice()
            }
        }
    }
}

fn lex_at_most(member: &[u8], bound: &LexBound) -> bool {
    match bound {
        LexBound::NegInf => false,
        LexBound::PosInf => true,
        LexBound::Value { bytes, exclusive } => {
            if *exclusive {
                member < bytes.as_slice()
            }
            else {
                member <= bytes.as_slice()
            }
        }
    }
}

/// 把 `start` / `stop` 归一化为可切片的排名区间；空区间回 `None`。
fn entry_rank_range(start: i64, stop: i64, length: usize) -> Option<(usize, usize)> {
    let length = length as i64;
    let begin = if start < 0 { length.saturating_add(start) } else { start }.max(0);
    let end = if stop < 0 { length.saturating_add(stop) } else { stop }.min(length - 1);
    if begin > end || begin >= length || end < 0 { None } else { Some((begin as usize, end as usize)) }
}

/// 按 `LIMIT` 截取一段结果；offset 为负按 0 处理，count 为负表示取到末尾。
fn apply_limit<T: Clone>(items: &[T], offset: i64, count: i64) -> Vec<T> {
    let offset = offset.max(0) as usize;
    if offset >= items.len() {
        return Vec::new();
    }
    let remaining = &items[offset..];
    if count < 0 {
        return remaining.to_vec();
    }
    remaining[..(count as usize).min(remaining.len())].to_vec()
}

/// 输出一段 zset 结果，`WITHSCORES` 时成员与分数交替排列。
fn write_zset_entries(stream: &mut dyn io::Write, entries: &[(Vec<u8>, f64)], with_scores: bool) -> io::Result<()> {
    write_array_header(stream, if with_scores { entries.len() * 2 } else { entries.len() })?;
    for (member, score) in entries {
        write_bulk(stream, member)?;
        if with_scores {
            write_score(stream, *score)?;
        }
    }
    Ok(())
}

/// 解析 `ZRANGEBYSCORE` 的 `WITHSCORES` 与 `LIMIT offset count` 选项。
fn parse_score_range_options(
    stream: &mut dyn io::Write,
    arguments: &[&[u8]],
    start: usize,
) -> io::Result<Option<(bool, Option<(i64, i64)>)>> {
    let mut with_scores = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut index = start;
    while index < arguments.len() {
        if arguments[index].eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
            index += 1;
        }
        else if arguments[index].eq_ignore_ascii_case(b"LIMIT") {
            if index + 2 >= arguments.len() {
                write_error(stream, "syntax error")?;
                return Ok(None);
            }
            let (Some(offset), Some(count)) =
                (parse_index(arguments[index + 1]), parse_index(arguments[index + 2]))
            else {
                write_error(stream, "value is not an integer or out of range")?;
                return Ok(None);
            };
            limit = Some((offset, count));
            index += 3;
        }
        else {
            write_error(stream, "syntax error")?;
            return Ok(None);
        }
    }
    Ok(Some((with_scores, limit)))
}

/// 解析 `ZRANGEBYLEX` 的 `LIMIT offset count` 选项，字典族不接受 `WITHSCORES`。
fn parse_lex_options(
    stream: &mut dyn io::Write,
    arguments: &[&[u8]],
    start: usize,
) -> io::Result<Option<Option<(i64, i64)>>> {
    let mut limit: Option<(i64, i64)> = None;
    let mut index = start;
    while index < arguments.len() {
        if arguments[index].eq_ignore_ascii_case(b"LIMIT") && index + 2 < arguments.len() {
            let (Some(offset), Some(count)) =
                (parse_index(arguments[index + 1]), parse_index(arguments[index + 2]))
            else {
                write_error(stream, "value is not an integer or out of range")?;
                return Ok(None);
            };
            limit = Some((offset, count));
            index += 3;
        }
        else {
            write_error(stream, "syntax error")?;
            return Ok(None);
        }
    }
    Ok(Some(limit))
}

/// 写出一段 hash 扫描结果：`[next-cursor, [field, value, ...]]`。
fn write_hscan_page(
    stream: &mut dyn io::Write,
    fields: &[(Vec<u8>, Vec<u8>)],
    cursor: usize,
    count: usize,
    pattern: Option<&[u8]>,
) -> io::Result<()> {
    if cursor >= fields.len() {
        stream.write_all(b"*2\r\n")?;
        write_bulk(stream, b"0")?;
        return write_array_header(stream, 0);
    }
    let end = cursor.saturating_add(count).min(fields.len());
    let next = if end >= fields.len() { "0".to_string() } else { end.to_string() };
    let matched: Vec<&(Vec<u8>, Vec<u8>)> = fields[cursor..end]
        .iter()
        .filter(|(field, _)| pattern.map_or(true, |pattern| glob_match_local(pattern, field)))
        .collect();
    stream.write_all(b"*2\r\n")?;
    write_bulk(stream, next.as_bytes())?;
    write_array_header(stream, matched.len() * 2)?;
    for (field, value) in matched {
        write_bulk(stream, field)?;
        write_bulk(stream, value)?;
    }
    Ok(())
}

/// 写出一段 zset 扫描结果：`[next-cursor, [member, score, ...]]`。
fn write_zscan_page(
    stream: &mut dyn io::Write,
    entries: &[(Vec<u8>, f64)],
    cursor: usize,
    count: usize,
    pattern: Option<&[u8]>,
) -> io::Result<()> {
    if cursor >= entries.len() {
        stream.write_all(b"*2\r\n")?;
        write_bulk(stream, b"0")?;
        return write_array_header(stream, 0);
    }
    let end = cursor.saturating_add(count).min(entries.len());
    let next = if end >= entries.len() { "0".to_string() } else { end.to_string() };
    let matched: Vec<&(Vec<u8>, f64)> = entries[cursor..end]
        .iter()
        .filter(|(member, _)| pattern.map_or(true, |pattern| glob_match_local(pattern, member)))
        .collect();
    stream.write_all(b"*2\r\n")?;
    write_bulk(stream, next.as_bytes())?;
    write_array_header(stream, matched.len() * 2)?;
    for (member, score) in matched {
        write_bulk(stream, member)?;
        write_score(stream, *score)?;
    }
    Ok(())
}

/// `ZADD` / `ZSCORE` / `ZREM` / `ZCARD` / `ZRANGE` 及排序族、排名、计数与扫描的分派入口。
pub(crate) fn handle_zset(
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
    if command.eq_ignore_ascii_case(b"ZADD") {
        if request.arguments.len() < 4 || (request.arguments.len() - 2) % 2 != 0 {
            return write_arity_error(stream, "zadd");
        }
        let key = request.arguments[1];
        match find_kind(executor, base, key) {
            Ok(None) | Ok(Some(KeyKind::ZSet)) => {}
            Ok(Some(_)) => return write_wrong_type(stream),
            Err(message) => return write_storage_error(stream, &message),
        }
        let mut entries = match load_value(executor, &KeyKind::ZSet.namespace(base), key) {
            Ok(Some(bytes)) => match decode_zset(&bytes) {
                Some(entries) => entries,
                None => return write_error(stream, "stored zset value is corrupt"),
            },
            Ok(None) => Vec::new(),
            Err(message) => return write_storage_error(stream, &message),
        };
        let mut added = 0_i64;
        for pair in request.arguments[2..].chunks(2) {
            // 全部分数先校验：任一非法都不写入，避免半截修改。
            let Some(score) = parse_score(pair[0])
            else {
                return write_error(stream, "value is not a valid float");
            };
            let member = pair[1].to_vec();
            match entries.iter_mut().find(|(existing, _)| *existing == member) {
                Some(entry) => entry.1 = score,
                None => {
                    entries.push((member, score));
                    added += 1;
                }
            }
        }
        let Some(bytes) = encode_zset(&entries)
        else {
            return write_error(stream, "zset exceeds the storage limit");
        };
        if let Err(message) = store_value(executor, &KeyKind::ZSet.namespace(base), key, bytes) {
            return write_storage_error(stream, &message);
        }
        return write_integer(stream, added);
    }
    if command.eq_ignore_ascii_case(b"ZSCORE") {
        if request.arguments.len() != 3 {
            return write_arity_error(stream, "zscore");
        }
        let key = request.arguments[1];
        return match load_zset(stream, executor, base, key)? {
            Loaded::Missing => write_null(stream, protocol),
            Loaded::Found(entries) => match entries.iter().find(|(member, _)| member.as_slice() == request.arguments[2]) {
                Some((_, score)) => write_score(stream, *score),
                None => write_null(stream, protocol),
            },
            Loaded::Rejected => Ok(()),
        };
    }
    if command.eq_ignore_ascii_case(b"ZREM") {
        if request.arguments.len() < 3 {
            return write_arity_error(stream, "zrem");
        }
        let key = request.arguments[1];
        let mut entries = match load_zset(stream, executor, base, key)? {
            Loaded::Missing => return write_integer(stream, 0),
            Loaded::Found(entries) => entries,
            Loaded::Rejected => return Ok(()),
        };
        let mut removed = 0_i64;
        entries.retain(|(member, _)| {
            let hit = request.arguments[2..].iter().any(|candidate| candidate == member);
            if hit {
                removed += 1;
            }
            !hit
        });
        if entries.is_empty() {
            if let Err(message) = remove_value(executor, &KeyKind::ZSet.namespace(base), key) {
                return write_storage_error(stream, &message);
            }
        }
        else {
            let Some(bytes) = encode_zset(&entries)
            else {
                return write_error(stream, "zset exceeds the storage limit");
            };
            if let Err(message) = store_value(executor, &KeyKind::ZSet.namespace(base), key, bytes) {
                return write_storage_error(stream, &message);
            }
        }
        return write_integer(stream, removed);
    }
    if command.eq_ignore_ascii_case(b"ZCARD") {
        if request.arguments.len() != 2 {
            return write_arity_error(stream, "zcard");
        }
        let key = request.arguments[1];
        return match load_zset(stream, executor, base, key)? {
            Loaded::Missing => write_integer(stream, 0),
            Loaded::Found(entries) => write_integer(stream, entries.len() as i64),
            Loaded::Rejected => Ok(()),
        };
    }
    if command.eq_ignore_ascii_case(b"ZRANGEBYSCORE") || command.eq_ignore_ascii_case(b"ZREVRANGEBYSCORE") {
        let reverse = command.eq_ignore_ascii_case(b"ZREVRANGEBYSCORE");
        if request.arguments.len() < 4 {
            let name = if reverse { "zrevrangebyscore" } else { "zrangebyscore" };
            return write_arity_error(stream, name);
        }
        let key = request.arguments[1];
        let (high, low) =
            if reverse { (request.arguments[2], request.arguments[3]) } else { (request.arguments[3], request.arguments[2]) };
        let (Some(min), Some(max)) = (parse_score_bound(low), parse_score_bound(high))
        else {
            return write_error(stream, "min or max is not a float");
        };
        let Some((with_scores, limit)) = parse_score_range_options(stream, &request.arguments, 4)?
        else {
            return Ok(());
        };
        let mut entries = match load_zset(stream, executor, base, key)? {
            Loaded::Missing => return write_array_header(stream, 0),
            Loaded::Found(entries) => entries,
            Loaded::Rejected => return Ok(()),
        };
        sort_zset(&mut entries);
        let mut selected: Vec<(Vec<u8>, f64)> =
            entries.into_iter().filter(|(_, score)| in_score_range(*score, min, max)).collect();
        if reverse {
            selected.reverse();
        }
        if let Some((offset, count)) = limit {
            selected = apply_limit(&selected, offset, count);
        }
        return write_zset_entries(stream, &selected, with_scores);
    }
    if command.eq_ignore_ascii_case(b"ZRANGEBYLEX") || command.eq_ignore_ascii_case(b"ZREVRANGEBYLEX") {
        let reverse = command.eq_ignore_ascii_case(b"ZREVRANGEBYLEX");
        if request.arguments.len() < 4 {
            let name = if reverse { "zrevrangebylex" } else { "zrangebylex" };
            return write_arity_error(stream, name);
        }
        let key = request.arguments[1];
        let (high, low) =
            if reverse { (request.arguments[2], request.arguments[3]) } else { (request.arguments[3], request.arguments[2]) };
        let (Some(min), Some(max)) = (parse_lex_bound(low), parse_lex_bound(high))
        else {
            return write_error(stream, "min or max not valid string range item");
        };
        let Some(limit) = parse_lex_options(stream, &request.arguments, 4)?
        else {
            return Ok(());
        };
        let entries = match load_zset(stream, executor, base, key)? {
            Loaded::Missing => return write_array_header(stream, 0),
            Loaded::Found(entries) => entries,
            Loaded::Rejected => return Ok(()),
        };
        // 字典族假定所有分数相同，因此只按成员字节比较。
        let mut members: Vec<Vec<u8>> = entries.into_iter().map(|(member, _)| member).collect();
        members.sort();
        members.retain(|member| lex_at_least(member, &min) && lex_at_most(member, &max));
        if reverse {
            members.reverse();
        }
        if let Some((offset, count)) = limit {
            members = apply_limit(&members, offset, count);
        }
        write_array_header(stream, members.len())?;
        for member in &members {
            write_bulk(stream, member)?;
        }
        return Ok(());
    }
    if command.eq_ignore_ascii_case(b"ZRANK") || command.eq_ignore_ascii_case(b"ZREVRANK") {
        let reverse = command.eq_ignore_ascii_case(b"ZREVRANK");
        if request.arguments.len() != 3 && request.arguments.len() != 4 {
            let name = if reverse { "zrevrank" } else { "zrank" };
            return write_arity_error(stream, name);
        }
        let with_score = request.arguments.len() == 4;
        if with_score && !request.arguments[3].eq_ignore_ascii_case(b"WITHSCORE") {
            return write_error(stream, "syntax error");
        }
        let key = request.arguments[1];
        let mut entries = match load_zset(stream, executor, base, key)? {
            Loaded::Missing => {
                return if with_score { write_null_array(stream, protocol) } else { write_null(stream, protocol) };
            }
            Loaded::Found(entries) => entries,
            Loaded::Rejected => return Ok(()),
        };
        sort_zset(&mut entries);
        match entries.iter().position(|(member, _)| member.as_slice() == request.arguments[2]) {
            Some(index) => {
                let rank = if reverse { entries.len() - 1 - index } else { index } as i64;
                if with_score {
                    stream.write_all(b"*2\r\n")?;
                    write_integer(stream, rank)?;
                    write_score(stream, entries[index].1)?;
                }
                else {
                    write_integer(stream, rank)?;
                }
            }
            None => {
                if with_score {
                    write_null_array(stream, protocol)?;
                }
                else {
                    write_null(stream, protocol)?;
                }
            }
        }
        return Ok(());
    }
    if command.eq_ignore_ascii_case(b"ZINCRBY") {
        if request.arguments.len() != 4 {
            return write_arity_error(stream, "zincrby");
        }
        let Some(increment) = parse_score(request.arguments[2])
        else {
            return write_error(stream, "value is not a valid float");
        };
        let key = request.arguments[1];
        match find_kind(executor, base, key) {
            Ok(None) | Ok(Some(KeyKind::ZSet)) => {}
            Ok(Some(_)) => return write_wrong_type(stream),
            Err(message) => return write_storage_error(stream, &message),
        }
        let mut entries = match load_value(executor, &KeyKind::ZSet.namespace(base), key) {
            Ok(Some(bytes)) => match decode_zset(&bytes) {
                Some(entries) => entries,
                None => return write_error(stream, "stored zset value is corrupt"),
            },
            Ok(None) => Vec::new(),
            Err(message) => return write_storage_error(stream, &message),
        };
        let member = request.arguments[3].to_vec();
        let new_score = match entries.iter().find(|(existing, _)| *existing == member) {
            Some((_, score)) => *score + increment,
            None => increment,
        };
        if new_score.is_nan() {
            return write_error(stream, "resulting score is not a number (NaN)");
        }
        match entries.iter_mut().find(|(existing, _)| *existing == member) {
            Some(entry) => entry.1 = new_score,
            None => entries.push((member, new_score)),
        }
        let Some(bytes) = encode_zset(&entries)
        else {
            return write_error(stream, "zset exceeds the storage limit");
        };
        if let Err(message) = store_value(executor, &KeyKind::ZSet.namespace(base), key, bytes) {
            return write_storage_error(stream, &message);
        }
        return write_score(stream, new_score);
    }
    if command.eq_ignore_ascii_case(b"ZCOUNT") {
        if request.arguments.len() != 4 {
            return write_arity_error(stream, "zcount");
        }
        let (Some(min), Some(max)) = (parse_score_bound(request.arguments[2]), parse_score_bound(request.arguments[3]))
        else {
            return write_error(stream, "min or max is not a float");
        };
        let key = request.arguments[1];
        return match load_zset(stream, executor, base, key)? {
            Loaded::Missing => write_integer(stream, 0),
            Loaded::Found(entries) => {
                write_integer(stream, entries.iter().filter(|(_, score)| in_score_range(*score, min, max)).count() as i64)
            }
            Loaded::Rejected => Ok(()),
        };
    }
    if command.eq_ignore_ascii_case(b"ZREMRANGEBYRANK") || command.eq_ignore_ascii_case(b"ZREMRANGEBYSCORE") {
        let by_score = command.eq_ignore_ascii_case(b"ZREMRANGEBYSCORE");
        if request.arguments.len() != 4 {
            let name = if by_score { "zremrangebyscore" } else { "zremrangebyrank" };
            return write_arity_error(stream, name);
        }
        let mut min = ScoreBound { value: f64::NEG_INFINITY, exclusive: false };
        let mut max = ScoreBound { value: f64::INFINITY, exclusive: false };
        let mut rank_range: Option<(i64, i64)> = None;
        if by_score {
            match (parse_score_bound(request.arguments[2]), parse_score_bound(request.arguments[3])) {
                (Some(low), Some(high)) => {
                    min = low;
                    max = high;
                }
                _ => return write_error(stream, "min or max is not a float"),
            }
        }
        else {
            let (Some(low), Some(high)) = (parse_index(request.arguments[2]), parse_index(request.arguments[3]))
            else {
                return write_error(stream, "value is not an integer or out of range");
            };
            rank_range = Some((low, high));
        }
        let key = request.arguments[1];
        let mut removed = 0_i64;
        let mut entries = match load_zset(stream, executor, base, key)? {
            Loaded::Missing => return write_integer(stream, 0),
            Loaded::Found(entries) => entries,
            Loaded::Rejected => return Ok(()),
        };
        if by_score {
            entries.retain(|(_, score)| {
                let hit = in_score_range(*score, min, max);
                if hit {
                    removed += 1;
                }
                !hit
            });
        }
        else {
            sort_zset(&mut entries);
            if let Some((start, stop)) = rank_range {
                if let Some((begin, end)) = entry_rank_range(start, stop, entries.len()) {
                    entries.drain(begin..=end);
                    removed = (end - begin + 1) as i64;
                }
            }
        }
        if entries.is_empty() {
            if let Err(message) = remove_value(executor, &KeyKind::ZSet.namespace(base), key) {
                return write_storage_error(stream, &message);
            }
        }
        else {
            let Some(bytes) = encode_zset(&entries)
            else {
                return write_error(stream, "zset exceeds the storage limit");
            };
            if let Err(message) = store_value(executor, &KeyKind::ZSet.namespace(base), key, bytes) {
                return write_storage_error(stream, &message);
            }
        }
        return write_integer(stream, removed);
    }
    if command.eq_ignore_ascii_case(b"ZSCAN") {
        if request.arguments.len() < 3 {
            return write_arity_error(stream, "zscan");
        }
        let Some(cursor) = parse_scan_cursor(stream, request.arguments[2])?
        else {
            return Ok(());
        };
        let Some((pattern, count)) = parse_scan_tail(stream, &request.arguments, 3)?
        else {
            return Ok(());
        };
        let key = request.arguments[1];
        let entries = match load_zset(stream, executor, base, key)? {
            Loaded::Missing => Vec::new(),
            Loaded::Found(entries) => entries,
            Loaded::Rejected => return Ok(()),
        };
        return write_zscan_page(stream, &entries, cursor, count, pattern.as_deref());
    }
    // ZRANGE
    let with_scores = match request.arguments.len() {
        4 => false,
        5 if request.arguments[4].eq_ignore_ascii_case(b"WITHSCORES") => true,
        5 => return write_error(stream, "syntax error"),
        _ => return write_arity_error(stream, "zrange"),
    };
    let (Some(start), Some(stop)) = (parse_index(request.arguments[2]), parse_index(request.arguments[3]))
    else {
        return write_error(stream, "value is not an integer or out of range");
    };
    let key = request.arguments[1];
    let mut entries = match load_zset(stream, executor, base, key)? {
        Loaded::Missing => return write_array_header(stream, 0),
        Loaded::Found(entries) => entries,
        Loaded::Rejected => return Ok(()),
    };
    sort_zset(&mut entries);
    let length = entries.len() as i64;
    let mut begin = if start < 0 { length.saturating_add(start) } else { start };
    let end = if stop < 0 { length.saturating_add(stop) } else { stop };
    begin = begin.max(0);
    let end = end.min(length - 1);
    if begin > end || begin >= length || end < 0 {
        return write_array_header(stream, 0);
    }
    let slice = &entries[begin as usize..=end as usize];
    write_array_header(stream, if with_scores { slice.len() * 2 } else { slice.len() })?;
    for (member, score) in slice {
        write_bulk(stream, member)?;
        if with_scores {
            write_score(stream, *score)?;
        }
    }
    Ok(())
}
