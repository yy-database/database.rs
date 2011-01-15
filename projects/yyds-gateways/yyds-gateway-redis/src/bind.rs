//! Redis storage command binding without direct shard access.

use yyds_types::{KeyValueAction, KeyValueCommand, Namespace};

/// A storage command rejected before execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindError {
    /// No command token was supplied.
    EmptyCommand,
    /// This command has no supported execution binding.
    UnsupportedCommand,
    /// Argument count differs from the supported command shape.
    WrongArity,
    /// The option list is not valid Redis syntax (conflicting, repeated or missing
    /// arguments such as `NX XX` or a trailing `EX`).
    Syntax,
    /// SET options require semantics that are not implemented yet.
    UnsupportedOptions,
    /// Increment delta or expiry value is not a canonical integer.
    InvalidInteger,
    /// `EX` / `PX` expiry is zero or negative.
    InvalidExpire,
}

/// Condition requested by the `NX` / `XX` options of `SET`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SetCondition {
    /// No condition: the write always applies.
    #[default]
    Always,
    /// `NX`: store only when the key is absent.
    IfAbsent,
    /// `XX`: store only when the key already exists.
    IfPresent,
}

/// Expiry requested by `SET ... EX <s> | PX <ms> | EXAT <s> | PXAT <ms>`.
///
/// Expiry is a wall-clock Unix-millisecond deadline stored on the KV record
/// itself, so the absolute forms are honoured directly: `EXAT` / `PXAT` name
/// the deadline, `EX` / `PX` are offsets from the server's current time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetExpiry {
    /// `EX <seconds>` relative to now.
    Seconds(u64),
    /// `PX <milliseconds>` relative to now.
    Milliseconds(u64),
    /// `EXAT <unix-seconds>` absolute deadline.
    AtSeconds(u64),
    /// `PXAT <unix-milliseconds>` absolute deadline.
    AtMilliseconds(u64),
}

/// Parsed `SET key value [NX|XX] [EX <s>|PX <ms>] [KEEPTTL]` request.
///
/// The value of a `SET` is a plain KV write, while the expiry is gateway key-space
/// state (`KeyValueAction` carries no TTL), so the connection layer consumes this
/// structure instead of guessing from the argument count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetRequest {
    /// Key bytes exactly as the client sent them.
    pub key: Vec<u8>,
    /// Value bytes exactly as the client sent them.
    pub value: Vec<u8>,
    /// `NX` / `XX` write condition.
    pub condition: SetCondition,
    /// Relative expiry, absent when the command carries no TTL option.
    pub expiry: Option<SetExpiry>,
    /// Whether the previous expiry must survive the overwrite.
    pub keep_ttl: bool,
}

/// Parses the full `SET` option list.
///
/// Options are matched case-insensitively in any order, exactly as Redis does.
/// Repeated or conflicting combinations (`NX XX`, `EX 1 EX 2`, `EX 1 KEEPTTL`,
/// a trailing option without its argument) are syntax errors, and a non-positive
/// or non-integer expiry is rejected before anything is written.
pub fn bind_set(arguments: &[&[u8]]) -> Result<SetRequest, BindError> {
    if arguments.len() < 3 {
        return Err(BindError::WrongArity);
    }
    let mut condition = SetCondition::Always;
    let mut expiry = None;
    let mut keep_ttl = false;
    let mut index = 3;
    while index < arguments.len() {
        let option = arguments[index];
        if option.eq_ignore_ascii_case(b"NX") {
            if condition != SetCondition::Always {
                return Err(BindError::Syntax);
            }
            condition = SetCondition::IfAbsent;
            index += 1;
        }
        else if option.eq_ignore_ascii_case(b"XX") {
            if condition != SetCondition::Always {
                return Err(BindError::Syntax);
            }
            condition = SetCondition::IfPresent;
            index += 1;
        }
        else if option.eq_ignore_ascii_case(b"KEEPTTL") {
            if keep_ttl || expiry.is_some() {
                return Err(BindError::Syntax);
            }
            keep_ttl = true;
            index += 1;
        }
        else if option.eq_ignore_ascii_case(b"EX")
            || option.eq_ignore_ascii_case(b"PX")
            || option.eq_ignore_ascii_case(b"EXAT")
            || option.eq_ignore_ascii_case(b"PXAT")
        {
            if expiry.is_some() || keep_ttl {
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
            index += 2;
        }
        else {
            // `GET` / `IDLE` and any other option would require semantics this
            // gateway does not implement, so they are refused explicitly rather
            // than dropped.
            return Err(BindError::UnsupportedOptions);
        }
    }
    Ok(SetRequest { key: arguments[1].to_vec(), value: arguments[2].to_vec(), condition, expiry, keep_ttl })
}

/// Redis rejects non-canonical integer spellings (`+1`, ` 1`, `01`).
fn parse_canonical_integer(bytes: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(bytes).ok()?;
    let value = text.parse::<i64>().ok()?;
    if value.to_string() != text {
        return None;
    }
    Some(value)
}

/// Binds one binary Redis request into a protocol-neutral YYDS command.
pub fn bind(arguments: &[&[u8]], namespace: &Namespace) -> Result<KeyValueCommand, BindError> {
    let command = arguments.first().ok_or(BindError::EmptyCommand)?;
    let action = if command.eq_ignore_ascii_case(b"GET") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        KeyValueAction::Get
    }
    else if command.eq_ignore_ascii_case(b"EXISTS") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        KeyValueAction::Exists
    }
    else if command.eq_ignore_ascii_case(b"GETDEL") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        KeyValueAction::GetDelete
    }
    else if command.eq_ignore_ascii_case(b"GETSET") {
        if arguments.len() != 3 {
            return Err(BindError::WrongArity);
        }
        KeyValueAction::GetSet(arguments[2].to_vec())
    }
    else if command.eq_ignore_ascii_case(b"SET") {
        // Only the write condition reaches the executor: `KeyValueAction` has no
        // expiry field, so TTL bookkeeping belongs to the connection layer.
        let request = bind_set(arguments)?;
        match request.condition {
            SetCondition::Always => KeyValueAction::Put(request.value),
            SetCondition::IfAbsent => KeyValueAction::PutIfAbsent(request.value),
            SetCondition::IfPresent => KeyValueAction::PutIfPresent(request.value),
        }
    }
    else if command.eq_ignore_ascii_case(b"DEL") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        KeyValueAction::Delete
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
        KeyValueAction::IncrementBy(delta)
    }
    else {
        return Err(BindError::UnsupportedCommand);
    };
    Ok(KeyValueCommand { namespace: namespace.clone(), key: arguments[1].to_vec(), action })
}

/// Binds a Redis `MGET` request for the YYDS atomic read batch contract.
pub fn bind_mget(arguments: &[&[u8]], namespace: &Namespace) -> Result<Vec<KeyValueCommand>, BindError> {
    let command = arguments.first().ok_or(BindError::EmptyCommand)?;
    if !command.eq_ignore_ascii_case(b"MGET") {
        return Err(BindError::UnsupportedCommand);
    }
    if arguments.len() < 2 {
        return Err(BindError::WrongArity);
    }
    Ok(arguments[1..]
        .iter()
        .map(|key| KeyValueCommand { namespace: namespace.clone(), key: (*key).to_vec(), action: KeyValueAction::Get })
        .collect())
}
