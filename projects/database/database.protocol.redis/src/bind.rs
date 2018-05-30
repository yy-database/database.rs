//! Redis 存储命令绑定：把 RESP 参数绑成中立 KV 命令形状，不直接访问分片。
//!
//! 本模块不依赖任何产品品牌 crate。上层（`server` / 网关）再把
//! [`BoundCommand`] 映射到执行层的键值合同。

/// 在真正执行前被拒绝的绑定错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindError {
    /// 未提供命令名。
    EmptyCommand,
    /// 该命令尚无支持的执行绑定。
    UnsupportedCommand,
    /// 参数个数与命令形状不符。
    WrongArity,
    /// 选项列表不是合法 Redis 语法（冲突、重复或缺少参数，如 `NX XX`、悬空的 `EX`）。
    Syntax,
    /// `SET` 选项需要尚未实现的语义。
    UnsupportedOptions,
    /// 增量或过期值不是规范整数写法。
    InvalidInteger,
    /// `EX` / `PX` 过期值为零或负数。
    InvalidExpire,
}

/// `SET` 的 `NX` / `XX` 写条件。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SetCondition {
    /// 无条件：始终写入。
    #[default]
    Always,
    /// `NX`：仅当键不存在时写入。
    IfAbsent,
    /// `XX`：仅当键已存在时写入。
    IfPresent,
}

/// `SET ... EX <s> | PX <ms> | EXAT <s> | PXAT <ms>` 请求的过期语义。
///
/// 过期是落在 KV 记录上的墙钟 Unix 毫秒截止时间：`EXAT` / `PXAT` 直接给出截止点，
/// `EX` / `PX` 则相对服务器当前时间偏移。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetExpiry {
    /// `EX <seconds>`，相对当前时间。
    Seconds(u64),
    /// `PX <milliseconds>`，相对当前时间。
    Milliseconds(u64),
    /// `EXAT <unix-seconds>`，绝对截止时间。
    AtSeconds(u64),
    /// `PXAT <unix-milliseconds>`，绝对截止时间。
    AtMilliseconds(u64),
}

/// 已解析的 `SET key value [NX|XX] [EX <s>|PX <ms>] [KEEPTTL]` 请求。
///
/// 值本身是普通 KV 写入；过期由连接层单独记账（[`BoundAction`] 不含 TTL），
/// 因此连接层应消费本结构，而不是靠参数个数猜测。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetRequest {
    /// 客户端原文键字节。
    pub key: Vec<u8>,
    /// 客户端原文值字节。
    pub value: Vec<u8>,
    /// `NX` / `XX` 写条件。
    pub condition: SetCondition,
    /// 相对或绝对过期；无 TTL 选项时为 `None`。
    pub expiry: Option<SetExpiry>,
    /// 覆盖写入时是否保留原过期。
    pub keep_ttl: bool,
    /// 是否在回复里带回旧值（`GET` 选项）。
    pub get: bool,
}

/// 本绑定器产出的中立键值动作（不含产品品牌语义）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundAction {
    /// 读取当前值。
    Get,
    /// 判断键是否存在。
    Exists,
    /// 原子读取并删除。
    GetDelete,
    /// 原子读取并替换。
    GetSet(Vec<u8>),
    /// 无条件写入。
    Put(Vec<u8>),
    /// 仅当键不存在时写入。
    PutIfAbsent(Vec<u8>),
    /// 仅当键已存在时写入。
    PutIfPresent(Vec<u8>),
    /// 删除。
    Delete,
    /// 对十进制整数值做有符号增量（缺省键视为 0）。
    IncrementBy(i64),
}

/// 一条已绑定、可交给上层执行的键值命令。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundCommand {
    /// 逻辑命名空间（由上层决定如何映射到存储分区）。
    pub namespace: String,
    /// 不透明键字节。
    pub key: Vec<u8>,
    /// 请求的动作。
    pub action: BoundAction,
}

/// 解析完整的 `SET` 选项列表。
///
/// 选项大小写不敏感、顺序任意，与 Redis 一致。`NX XX`、不同种过期选项同时出现、
/// `KEEPTTL` 与过期选项同时出现、缺少参数的尾部选项视为语法错误；同一种选项重复出现
/// 时保留最后一次。非正或非规范整数的过期值在写入前拒绝。
pub fn bind_set(arguments: &[&[u8]]) -> Result<SetRequest, BindError> {
    if arguments.len() < 3 {
        return Err(BindError::WrongArity);
    }
    let mut condition = SetCondition::Always;
    let mut expiry = None;
    let mut expiry_kind: Option<&[u8]> = None;
    let mut keep_ttl = false;
    let mut get = false;
    let mut index = 3;
    while index < arguments.len() {
        let option = arguments[index];
        if option.eq_ignore_ascii_case(b"NX") {
            if condition == SetCondition::IfPresent {
                return Err(BindError::Syntax);
            }
            condition = SetCondition::IfAbsent;
            index += 1;
        }
        else if option.eq_ignore_ascii_case(b"XX") {
            if condition == SetCondition::IfAbsent {
                return Err(BindError::Syntax);
            }
            condition = SetCondition::IfPresent;
            index += 1;
        }
        else if option.eq_ignore_ascii_case(b"KEEPTTL") {
            if expiry.is_some() {
                return Err(BindError::Syntax);
            }
            keep_ttl = true;
            index += 1;
        }
        else if option.eq_ignore_ascii_case(b"GET") {
            get = true;
            index += 1;
        }
        else if option.eq_ignore_ascii_case(b"EX")
            || option.eq_ignore_ascii_case(b"PX")
            || option.eq_ignore_ascii_case(b"EXAT")
            || option.eq_ignore_ascii_case(b"PXAT")
        {
            // 真机允许同一种过期选项重复出现（最后一次生效），不同种同时出现才是语法错误。
            if keep_ttl || expiry_kind.is_some_and(|kind| !kind.eq_ignore_ascii_case(option)) {
                return Err(BindError::Syntax);
            }
            let Some(raw) = arguments.get(index + 1)
            else {
                return Err(BindError::Syntax);
            };
            let Some(value) = parse_canonical_integer(raw)
            else {
                return Err(BindError::InvalidInteger);
            };
            if value <= 0 {
                return Err(BindError::InvalidExpire);
            }
            let magnitude = value as u64;
            expiry = Some(if option.eq_ignore_ascii_case(b"PX") {
                SetExpiry::Milliseconds(magnitude)
            }
            else if option.eq_ignore_ascii_case(b"EXAT") {
                SetExpiry::AtSeconds(magnitude)
            }
            else if option.eq_ignore_ascii_case(b"PXAT") {
                SetExpiry::AtMilliseconds(magnitude)
            }
            else {
                SetExpiry::Seconds(magnitude)
            });
            expiry_kind = Some(option);
            index += 2;
        }
        else {
            // `IDLE` 等选项需要本绑定器尚未实现的语义，显式拒绝而不是静默丢弃。
            return Err(BindError::UnsupportedOptions);
        }
    }
    Ok(SetRequest { key: arguments[1].to_vec(), value: arguments[2].to_vec(), condition, expiry, keep_ttl, get })
}

/// Redis 拒绝非规范整数拼写（`+1`、` 1`、`01`）。
fn parse_canonical_integer(bytes: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(bytes).ok()?;
    let value = text.parse::<i64>().ok()?;
    if value.to_string() != text {
        return None;
    }
    Some(value)
}

/// 把一条二进制 Redis 请求绑成中立键值命令。
pub fn bind(arguments: &[&[u8]], namespace: &str) -> Result<BoundCommand, BindError> {
    let command = arguments.first().ok_or(BindError::EmptyCommand)?;
    let action = if command.eq_ignore_ascii_case(b"GET") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        BoundAction::Get
    }
    else if command.eq_ignore_ascii_case(b"EXISTS") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        BoundAction::Exists
    }
    else if command.eq_ignore_ascii_case(b"GETDEL") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        BoundAction::GetDelete
    }
    else if command.eq_ignore_ascii_case(b"GETSET") {
        if arguments.len() != 3 {
            return Err(BindError::WrongArity);
        }
        BoundAction::GetSet(arguments[2].to_vec())
    }
    else if command.eq_ignore_ascii_case(b"SET") {
        // 只有写条件进入执行层：`BoundAction` 不含过期字段，TTL 由连接层记账。
        let request = bind_set(arguments)?;
        match request.condition {
            SetCondition::Always => BoundAction::Put(request.value),
            SetCondition::IfAbsent => BoundAction::PutIfAbsent(request.value),
            SetCondition::IfPresent => BoundAction::PutIfPresent(request.value),
        }
    }
    else if command.eq_ignore_ascii_case(b"DEL") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        BoundAction::Delete
    }
    else if command.eq_ignore_ascii_case(b"INCR")
        || command.eq_ignore_ascii_case(b"INCRBY")
        || command.eq_ignore_ascii_case(b"DECR")
        || command.eq_ignore_ascii_case(b"DECRBY")
    {
        let unit = command.eq_ignore_ascii_case(b"INCR") || command.eq_ignore_ascii_case(b"DECR");
        let increment = command.eq_ignore_ascii_case(b"INCR") || command.eq_ignore_ascii_case(b"INCRBY");
        if arguments.len() != if unit { 2 } else { 3 } {
            return Err(BindError::WrongArity);
        }
        let magnitude = if unit {
            1
        }
        else {
            let text = std::str::from_utf8(arguments[2]).map_err(|_| BindError::InvalidInteger)?;
            let value = text.parse::<i64>().map_err(|_| BindError::InvalidInteger)?;
            if value.to_string() != text {
                return Err(BindError::InvalidInteger);
            }
            value
        };
        let delta = if increment { magnitude } else { magnitude.checked_neg().ok_or(BindError::InvalidInteger)? };
        BoundAction::IncrementBy(delta)
    }
    else {
        return Err(BindError::UnsupportedCommand);
    };
    Ok(BoundCommand { namespace: namespace.to_owned(), key: arguments[1].to_vec(), action })
}

/// 把 Redis `MGET` 绑成一批只读键值命令（原子批读由上层保证）。
pub fn bind_mget(arguments: &[&[u8]], namespace: &str) -> Result<Vec<BoundCommand>, BindError> {
    let command = arguments.first().ok_or(BindError::EmptyCommand)?;
    if !command.eq_ignore_ascii_case(b"MGET") {
        return Err(BindError::UnsupportedCommand);
    }
    if arguments.len() < 2 {
        return Err(BindError::WrongArity);
    }
    Ok(arguments[1..]
        .iter()
        .map(|key| BoundCommand { namespace: namespace.to_owned(), key: (*key).to_vec(), action: BoundAction::Get })
        .collect())
}
