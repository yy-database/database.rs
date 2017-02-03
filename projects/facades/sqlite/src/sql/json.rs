//! SQLite JSON 标量函数与 `->` / `->>` 运算符。
//!
//! 期望值来自 better-sqlite3 12.11.1（SQLite 3.53.2）与 CPython 3.49.1 的
//! `sqlite3`，不是本仓实现的自我参照。JSON 函数按 SQLite 3.5x 的宽松规则
//! 接受 JSON5 输入，数字节点在标准写法下保存原始文本，从而让 `->` 与容器
//! 重排都复现 SQLite 的逐字输出。`json_valid(x)` 只认严格 JSON。

use super::{engine::Cell, error::SqlError, sql::ScalarFunction};

/// 解析错误的固定消息。
const MALFORMED_MESSAGE: &str = "malformed JSON";

fn malformed() -> SqlError {
    SqlError::new("malformed-json", MALFORMED_MESSAGE)
}

fn bad_path(path: &str) -> SqlError {
    SqlError::new("bad-json-path", format!("bad JSON path: '{path}'"))
}

fn argument_error(message: impl Into<String>) -> SqlError {
    SqlError::new("json-argument", message)
}

/// 一个 JSON 值。对象按书写顺序保存成员，重复键也各自保留。
#[derive(Clone, Debug, PartialEq)]
enum JsonValue {
    Null,
    Bool(bool),
    Number(JsonNumber),
    Text(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

/// 数字节点：`kind` 由写法决定（`json_type` 只看写法），`raw` 保留原始文本，
/// 只有来自 SQL 值的数字才按 SQLite 的实数格式重新渲染。
#[derive(Clone, Debug, PartialEq)]
struct JsonNumber {
    raw: Option<String>,
    kind: NumberKind,
    integer: Option<i64>,
    real: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NumberKind {
    Integer,
    Real,
}

impl JsonNumber {
    fn from_integer(value: i64) -> Self {
        Self { raw: None, kind: NumberKind::Integer, integer: Some(value), real: value as f64 }
    }

    fn from_real(value: f64) -> Self {
        Self { raw: None, kind: NumberKind::Real, integer: None, real: value }
    }
}

/// 一条 JSON 路径由若干步组成。
#[derive(Clone, Debug, PartialEq, Eq)]
enum PathStep {
    Key(String),
    Index(usize),
    Append,
    FromEnd(usize),
}

/// 路径解析失败。`incomplete` 表示路径在 `[` 内提前结束，SQLite 把它当作
/// “没有这条路径”而不是错误。
struct PathError {
    incomplete: bool,
}

fn incomplete_path() -> PathError {
    PathError { incomplete: true }
}

fn invalid_path() -> PathError {
    PathError { incomplete: false }
}

/// 执行一个 JSON 标量函数。`json_arguments` 标记实参中直接来自 JSON 函数的
/// 位置，这些文本按 JSON 值嵌入，而不是当作普通字符串。
pub(crate) fn eval(function: ScalarFunction, args: &[Cell], json_arguments: &[bool]) -> Result<Cell, SqlError> {
    match function {
        ScalarFunction::JsonValid => json_valid(args),
        ScalarFunction::JsonQuote => Ok(json_quote(&args[0])),
        ScalarFunction::JsonArray => Ok(Cell::Text(serialize(&JsonValue::Array(
            args.iter().enumerate().map(|(index, cell)| from_cell(cell, json_arguments.get(index).copied().unwrap_or(false))).collect(),
        )))),
        ScalarFunction::JsonObject => json_object(args, json_arguments),
        ScalarFunction::JsonType => json_type(args),
        ScalarFunction::JsonArrayLength => json_array_length(args),
        ScalarFunction::JsonExtract => json_extract(args),
        ScalarFunction::JsonRemove => json_remove(args),
        ScalarFunction::JsonSet => json_assign(args, json_arguments, Mode::Set),
        ScalarFunction::JsonInsert => json_assign(args, json_arguments, Mode::Insert),
        ScalarFunction::JsonReplace => json_assign(args, json_arguments, Mode::Replace),
        ScalarFunction::JsonPatch => json_patch(args),
        _ => Err(argument_error(format!("{}() is not a JSON function", function.name()))),
    }
}

/// `->` 返回选中值的 JSON 文本，`->>` 再把它转成 SQL 值。
pub(crate) fn arrow(left: &Cell, right: &Cell, text: bool) -> Result<Cell, SqlError> {
    if matches!(left, Cell::Null) || matches!(right, Cell::Null) {
        return Ok(Cell::Null);
    }
    let document = match left {
        Cell::Text(value) => parse_document(value, ParseMode::Lenient)?,
        Cell::Blob(bytes) => parse_document(&String::from_utf8_lossy(bytes), ParseMode::Lenient)?,
        _ => return Err(malformed()),
    };
    let path = match right {
        Cell::Integer(value) => {
            if *value >= 0 {
                JsonPath::new(vec![PathStep::Index(*value as usize)])
            }
            else {
                JsonPath::new(vec![PathStep::FromEnd((-*value) as usize)])
            }
        }
        Cell::Text(value) => match arrow_path(value) {
            Ok(path) => path,
            Err(error) if error.incomplete => return Ok(Cell::Null),
            Err(_) => return Err(bad_path(value)),
        },
        Cell::Blob(bytes) => {
            let value = String::from_utf8_lossy(bytes).into_owned();
            match arrow_path(&value) {
                Ok(path) => path,
                Err(error) if error.incomplete => return Ok(Cell::Null),
                Err(_) => return Err(bad_path(&value)),
            }
        }
        // 实数、BLOB 以外的类型都不是合法的路径操作数。
        _ => return Ok(Cell::Null),
    };
    let Some(selected) = lookup(&document, &path.steps).cloned()
    else {
        return Ok(Cell::Null);
    };
    Ok(if text { to_cell(&selected) } else { Cell::Text(serialize(&selected)) })
}

/// `->` 的文本操作数：以 `$` 开头按路径解析，以 `[` 开头补上 `$`，其余按
/// 单个对象标签补齐。标签形式构造失败时 SQLite 返回 `NULL`。
fn arrow_path(text: &str) -> Result<JsonPath, PathError> {
    if text.starts_with('$') {
        return Ok(JsonPath::new(parse_path_steps(text)?));
    }
    if text.starts_with('[') {
        return Ok(JsonPath::new(parse_path_steps(&format!("${text}"))?));
    }
    let quoted = format!("$.\"{text}\"");
    Ok(JsonPath::new(parse_path_steps(&quoted).map_err(|_| incomplete_path())?))
}

/// 解析后的路径。
struct JsonPath {
    steps: Vec<PathStep>,
}

impl JsonPath {
    fn new(steps: Vec<PathStep>) -> Self {
        Self { steps }
    }
}

/// `json_valid(x)` 只认严格 JSON。双参数形式按位检查输入形态：bit1 严格 JSON、
/// bit2 JSON5、bit4/8 JSONB（文本永远不满足），`FLAGS` 必须落在 1..=15。
fn json_valid(args: &[Cell]) -> Result<Cell, SqlError> {
    if args.len() == 2 {
        let flags = flags_value(&args[1]);
        if !(1..=15).contains(&flags) {
            return Err(argument_error("FLAGS parameter to json_valid() must be between 1 and 15"));
        }
        if matches!(args[0], Cell::Null) {
            return Ok(Cell::Null);
        }
        let text = json_text(&args[0]).unwrap_or_default();
        let strict = flags & 1 != 0 && parse_document(&text, ParseMode::Strict).is_ok();
        let lenient = flags & 2 != 0 && parse_document(&text, ParseMode::Lenient).is_ok();
        return Ok(Cell::Integer(i64::from(strict || lenient)));
    }
    match &args[0] {
        Cell::Null => Ok(Cell::Null),
        cell => {
            let text = json_text(cell).unwrap_or_default();
            Ok(Cell::Integer(i64::from(parse_document(&text, ParseMode::Strict).is_ok())))
        }
    }
}

/// `FLAGS` 按 `sqlite3_value_int64` 的规则取整：实数和文本都向零截断。
fn flags_value(cell: &Cell) -> i64 {
    match cell {
        Cell::Null => 0,
        Cell::Integer(value) => *value,
        Cell::Real(value) => {
            if value.is_finite() {
                *value as i64
            }
            else {
                0
            }
        }
        Cell::Text(text) => text
            .parse::<i64>()
            .unwrap_or_else(|_| text.parse::<f64>().map(|value| if value.is_finite() { value as i64 } else { 0 }).unwrap_or(0)),
        Cell::Blob(bytes) => String::from_utf8_lossy(bytes)
            .parse::<i64>()
            .unwrap_or_else(|_| String::from_utf8_lossy(bytes).parse::<f64>().map(|value| value as i64).unwrap_or(0)),
    }
}

fn json_quote(cell: &Cell) -> Cell {
    match cell {
        Cell::Null => Cell::Text("null".to_string()),
        Cell::Integer(value) => Cell::Text(value.to_string()),
        Cell::Real(value) => Cell::Text(number_text(&JsonNumber::from_real(*value))),
        Cell::Text(value) => Cell::Text(quote_string(value)),
        // BLOB 只在 JSONB 形态下有意义，本实现不覆盖 JSONB。
        Cell::Blob(_) => Cell::Text("null".to_string()),
    }
}

fn json_object(args: &[Cell], json_arguments: &[bool]) -> Result<Cell, SqlError> {
    if args.len() % 2 != 0 {
        return Err(argument_error("json_object() requires an even number of arguments"));
    }
    let mut members = Vec::with_capacity(args.len() / 2);
    for (index, pair) in args.chunks(2).enumerate() {
        let Cell::Text(label) = &pair[0]
        else {
            return Err(argument_error("json_object() labels must be TEXT"));
        };
        let tagged = json_arguments.get(index * 2 + 1).copied().unwrap_or(false);
        members.push((label.clone(), from_cell(&pair[1], tagged)));
    }
    Ok(Cell::Text(serialize(&JsonValue::Object(members))))
}

fn json_type(args: &[Cell]) -> Result<Cell, SqlError> {
    if matches!(args[0], Cell::Null) {
        return Ok(Cell::Null);
    }
    let document = parse_argument(&args[0])?;
    let node = if args.len() == 2 {
        if matches!(args[1], Cell::Null) {
            return Ok(Cell::Null);
        }
        let Some(path) = path_argument(&args[1])?
        else {
            return Ok(Cell::Null);
        };
        match lookup(&document, &path.steps) {
            Some(node) => node,
            None => return Ok(Cell::Null),
        }
    }
    else {
        &document
    };
    Ok(Cell::Text(type_name(node).to_string()))
}

fn json_array_length(args: &[Cell]) -> Result<Cell, SqlError> {
    if matches!(args[0], Cell::Null) {
        return Ok(Cell::Null);
    }
    let document = parse_argument(&args[0])?;
    let node = if args.len() == 2 {
        if matches!(args[1], Cell::Null) {
            return Ok(Cell::Null);
        }
        let Some(path) = path_argument(&args[1])?
        else {
            return Ok(Cell::Null);
        };
        match lookup(&document, &path.steps) {
            Some(node) => node,
            None => return Ok(Cell::Null),
        }
    }
    else {
        &document
    };
    Ok(Cell::Integer(match node {
        JsonValue::Array(items) => items.len() as i64,
        _ => 0,
    }))
}

fn json_extract(args: &[Cell]) -> Result<Cell, SqlError> {
    if matches!(args[0], Cell::Null) {
        return Ok(Cell::Null);
    }
    let document = parse_argument(&args[0])?;
    let mut paths = Vec::with_capacity(args.len() - 1);
    for argument in &args[1..] {
        if matches!(argument, Cell::Null) {
            return Ok(Cell::Null);
        }
        paths.push(path_argument(argument)?);
    }
    if paths.len() == 1 {
        let selected = paths[0].as_ref().and_then(|path| lookup(&document, &path.steps)).cloned().unwrap_or(JsonValue::Null);
        return Ok(to_cell(&selected));
    }
    let selected = paths
        .iter()
        .map(|path| path.as_ref().and_then(|path| lookup(&document, &path.steps)).cloned().unwrap_or(JsonValue::Null))
        .collect::<Vec<_>>();
    Ok(Cell::Text(serialize(&JsonValue::Array(selected))))
}

fn json_remove(args: &[Cell]) -> Result<Cell, SqlError> {
    if matches!(args[0], Cell::Null) {
        return Ok(Cell::Null);
    }
    let mut document = parse_argument(&args[0])?;
    for argument in &args[1..] {
        if matches!(argument, Cell::Null) {
            return Ok(Cell::Null);
        }
        let Some(path) = path_argument(argument)?
        else {
            continue;
        };
        if path.steps.is_empty() || remove_path(&mut document, &path.steps) {
            return Ok(Cell::Null);
        }
    }
    Ok(Cell::Text(serialize(&document)))
}

/// 写入模式：`json_set` 既能插入也能替换，`json_insert` 只插入，`json_replace`
/// 只替换。
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Set,
    Insert,
    Replace,
}

fn json_assign(args: &[Cell], json_arguments: &[bool], mode: Mode) -> Result<Cell, SqlError> {
    if matches!(args[0], Cell::Null) {
        return Ok(Cell::Null);
    }
    let mut document = parse_argument(&args[0])?;
    for (index, pair) in args[1..].chunks(2).enumerate() {
        if matches!(pair[0], Cell::Null) {
            continue;
        }
        let Some(path) = path_argument(&pair[0])?
        else {
            continue;
        };
        let tagged = json_arguments.get(index * 2 + 2).copied().unwrap_or(false);
        assign_path(&mut document, &path.steps, &from_cell(&pair[1], tagged), mode);
    }
    Ok(Cell::Text(serialize(&document)))
}

fn json_patch(args: &[Cell]) -> Result<Cell, SqlError> {
    if matches!(args[0], Cell::Null) || matches!(args[1], Cell::Null) {
        return Ok(Cell::Null);
    }
    let mut target = parse_argument(&args[0])?;
    let patch = parse_argument(&args[1])?;
    merge_patch(&mut target, patch);
    Ok(Cell::Text(serialize(&target)))
}

/// RFC 7386：对象按成员合并，`null` 删除成员，其他形态整体替换。
fn merge_patch(target: &mut JsonValue, patch: JsonValue) {
    match patch {
        JsonValue::Object(patch_members) => match target {
            JsonValue::Object(members) => {
                for (key, value) in patch_members {
                    if matches!(value, JsonValue::Null) {
                        members.retain(|(existing, _)| existing != &key);
                    }
                    else if let Some(index) = members.iter().position(|(existing, _)| existing == &key) {
                        merge_patch(&mut members[index].1, value);
                    }
                    else {
                        members.push((key, value));
                    }
                }
            }
            _ => *target = JsonValue::Object(patch_members),
        },
        other => *target = other,
    }
}

/// 解析第一个参数为文档，`NULL` 由调用方先行处理。
fn parse_argument(cell: &Cell) -> Result<JsonValue, SqlError> {
    let text = json_text(cell).ok_or_else(malformed)?;
    parse_document(&text, ParseMode::Lenient)
}

/// 解析路径参数。`None` 表示路径在 `[` 内提前结束，SQLite 视为不存在。
fn path_argument(cell: &Cell) -> Result<Option<JsonPath>, SqlError> {
    let Some(text) = json_text(cell)
    else {
        return Ok(None);
    };
    match parse_path_steps(&text) {
        Ok(steps) => Ok(Some(JsonPath::new(steps))),
        Err(error) if error.incomplete => Ok(None),
        Err(_) => Err(bad_path(&text)),
    }
}

/// SQL 值转换成的 JSON 文本：`BLOB` 按 JSON null 处理，数字使用 SQLite 的实数格式。
/// `tagged` 表示该文本来自 JSON 函数，直接按 JSON 值嵌入。
fn from_cell(cell: &Cell, tagged: bool) -> JsonValue {
    match cell {
        Cell::Null => JsonValue::Null,
        Cell::Integer(value) => JsonValue::Number(JsonNumber::from_integer(*value)),
        Cell::Real(value) => JsonValue::Number(JsonNumber::from_real(*value)),
        Cell::Text(value) => {
            if tagged {
                if let Ok(value) = parse_document(value, ParseMode::Lenient) {
                    return value;
                }
            }
            JsonValue::Text(value.clone())
        }
        Cell::Blob(_) => JsonValue::Null,
    }
}

/// JSON 值转换回 SQL 值：布尔折叠成 `1`/`0`，容器序列化成文本。
fn to_cell(value: &JsonValue) -> Cell {
    match value {
        JsonValue::Null => Cell::Null,
        JsonValue::Bool(value) => Cell::Integer(i64::from(*value)),
        JsonValue::Number(number) => match number.kind {
            NumberKind::Integer => match number.integer {
                Some(value) => Cell::Integer(value),
                None => Cell::Real(number.real),
            },
            NumberKind::Real => Cell::Real(number.real),
        },
        JsonValue::Text(text) => Cell::Text(text.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Cell::Text(serialize(value)),
    }
}

fn type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(true) => "true",
        JsonValue::Bool(false) => "false",
        JsonValue::Number(number) => match number.kind {
            NumberKind::Integer => "integer",
            NumberKind::Real => "real",
        },
        JsonValue::Text(_) => "text",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

/// SQL 值的文本形态，用于把 JSON 文档参数与路径参数读成字符串。
fn json_text(cell: &Cell) -> Option<String> {
    match cell {
        Cell::Null => None,
        Cell::Text(text) => Some(text.clone()),
        Cell::Blob(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        Cell::Integer(value) => Some(value.to_string()),
        Cell::Real(value) => Some(real_text(*value)),
    }
}

/// SQLite 把实数转成文本时保留小数点：`1.0` 是 `1.0` 而不是 `1`。
fn real_text(value: f64) -> String {
    if !value.is_finite() {
        return if value.is_nan() {
            "null".to_string()
        }
        else if value > 0.0 {
            "9.0e+999".to_string()
        }
        else {
            "-9.0e+999".to_string()
        };
    }
    let text = format!("{value}");
    if text.contains('.') || text.contains('e') || text.contains('E') { text } else { format!("{text}.0") }
}

/// 数字序列化：解析输入的数字保留原始文本，SQL 值生成的数字按 SQLite 规则渲染。
fn number_text(number: &JsonNumber) -> String {
    if let Some(raw) = &number.raw {
        return raw.clone();
    }
    match number.kind {
        NumberKind::Integer => match number.integer {
            Some(value) => value.to_string(),
            None => sqlite_real_text(number.real),
        },
        NumberKind::Real => sqlite_real_text(number.real),
    }
}

/// SQLite 的 JSON 实数格式：取最短可回读数字串，指数在 `-4 <= e < 17` 时用定点，
/// 并保证小数点存在。
fn sqlite_real_text(value: f64) -> String {
    if value == 0.0 {
        return "0.0".to_string();
    }
    if !value.is_finite() {
        return if value.is_nan() {
            "null".to_string()
        }
        else if value > 0.0 {
            "9.0e+999".to_string()
        }
        else {
            "-9.0e+999".to_string()
        };
    }
    let sign = if value < 0.0 { "-" } else { "" };
    let scientific = format!("{:e}", value.abs());
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let digits: String = mantissa.chars().filter(|character| *character != '.').collect();
    let mut out = String::from(sign);
    if exponent < -4 || exponent >= 17 {
        out.push(digits.chars().next().unwrap_or('0'));
        out.push('.');
        if digits.len() > 1 {
            out.push_str(&digits[1..]);
        }
        else {
            out.push('0');
        }
        out.push('e');
        out.push_str(&format!("{exponent:+03}"));
    }
    else if exponent >= 0 {
        let integer_len = (exponent + 1) as usize;
        if digits.len() >= integer_len {
            out.push_str(&digits[..integer_len]);
            out.push('.');
            if digits.len() > integer_len {
                out.push_str(&digits[integer_len..]);
            }
            else {
                out.push('0');
            }
        }
        else {
            out.push_str(&digits);
            for _ in digits.len()..integer_len {
                out.push('0');
            }
            out.push_str(".0");
        }
    }
    else {
        out.push_str("0.");
        for _ in 0..(-exponent - 1) {
            out.push('0');
        }
        out.push_str(&digits);
    }
    out
}

/// 序列化一个 JSON 值：容器紧凑输出，字符串按 SQLite 的转义规则引用。
fn serialize(value: &JsonValue) -> String {
    let mut out = String::new();
    write_value(&mut out, value);
    out
}

fn write_value(out: &mut String, value: &JsonValue) {
    match value {
        JsonValue::Null => out.push_str("null"),
        JsonValue::Bool(true) => out.push_str("true"),
        JsonValue::Bool(false) => out.push_str("false"),
        JsonValue::Number(number) => out.push_str(&number_text(number)),
        JsonValue::Text(text) => out.push_str(&quote_string(text)),
        JsonValue::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_value(out, item);
            }
            out.push(']');
        }
        JsonValue::Object(members) => {
            out.push('{');
            for (index, (key, value)) in members.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&quote_string(key));
                out.push(':');
                write_value(out, value);
            }
            out.push('}');
        }
    }
}

/// 字符串引用：`"`、`\`、常用控制字符与 `0x7f` 使用短转义，其余控制字符用
/// `\u00XX`，非 ASCII 原样保留。
fn quote_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            character if (character as u32) < 0x20 || character as u32 == 0x7f => {
                out.push_str(&format!("\\u{:04x}", character as u32));
            }
            character => out.push(character),
        }
    }
    out.push('"');
    out
}

/// 解析模式：`Strict` 只认标准 JSON，`Lenient` 额外接受 JSON5 写法。
#[derive(Clone, Copy, PartialEq, Eq)]
enum ParseMode {
    Strict,
    Lenient,
}

/// 解析一个文档；结尾空白之后还有内容即为错误。
fn parse_document(text: &str, mode: ParseMode) -> Result<JsonValue, SqlError> {
    let mut parser = Parser { bytes: text.as_bytes(), at: 0, lenient: mode == ParseMode::Lenient };
    parser.skip_whitespace();
    let value = parser.value().map_err(|_| malformed())?;
    parser.skip_whitespace();
    if parser.at != parser.bytes.len() {
        return Err(malformed());
    }
    Ok(value)
}

struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
    lenient: bool,
}

impl Parser<'_> {
    /// 跳过空白；宽松模式下同时跳过 `//` 行注释与 `/* */` 块注释。
    fn skip_whitespace(&mut self) {
        loop {
            while matches!(self.bytes.get(self.at), Some(b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)) {
                self.at += 1;
            }
            if !self.lenient {
                return;
            }
            match (self.bytes.get(self.at), self.bytes.get(self.at + 1)) {
                (Some(b'/'), Some(b'/')) => {
                    self.at += 2;
                    while !matches!(self.bytes.get(self.at), Some(b'\n') | None) {
                        self.at += 1;
                    }
                }
                (Some(b'/'), Some(b'*')) => {
                    self.at += 2;
                    while self.at + 1 < self.bytes.len() && !matches!((self.bytes[self.at], self.bytes[self.at + 1]), (b'*', b'/')) {
                        self.at += 1;
                    }
                    self.at = (self.at + 2).min(self.bytes.len());
                }
                _ => return,
            }
        }
    }

    fn value(&mut self) -> Result<JsonValue, ()> {
        self.skip_whitespace();
        match self.bytes.get(self.at) {
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(b'"') => Ok(JsonValue::Text(self.string(b'"')?)),
            Some(b'\'') if self.lenient => Ok(JsonValue::Text(self.string(b'\'')?)),
            Some(b't') => self.literal(b"true", JsonValue::Bool(true)),
            Some(b'f') => self.literal(b"false", JsonValue::Bool(false)),
            Some(b'n') => self.literal(b"null", JsonValue::Null),
            Some(b'N') if self.lenient => self.literal(b"NaN", JsonValue::Null),
            Some(b'I') if self.lenient => self.literal(b"Infinity", JsonValue::Number(infinity_number(false))),
            Some(b'-' | b'0'..=b'9') => Ok(JsonValue::Number(self.number()?)),
            Some(b'+' | b'.') if self.lenient => Ok(JsonValue::Number(self.number()?)),
            _ => Err(()),
        }
    }

    fn literal(&mut self, word: &[u8], value: JsonValue) -> Result<JsonValue, ()> {
        if self.bytes.get(self.at..self.at + word.len()) != Some(word) {
            return Err(());
        }
        self.at += word.len();
        Ok(value)
    }

    fn number(&mut self) -> Result<JsonNumber, ()> {
        let start = self.at;
        let mut negative = false;
        let mut standard = true;
        match self.bytes.get(self.at) {
            Some(b'+') if self.lenient => {
                self.at += 1;
                standard = false;
            }
            Some(b'-') => {
                self.at += 1;
                negative = true;
            }
            _ => {}
        }
        if self.lenient && self.bytes.get(self.at) == Some(&b'I') {
            if self.bytes.get(self.at..self.at + 8) != Some(b"Infinity") {
                return Err(());
            }
            self.at += 8;
            return Ok(infinity_number(negative));
        }
        // JSON5 允许十六进制整数。
        if self.lenient && self.bytes.get(self.at) == Some(&b'0') && matches!(self.bytes.get(self.at + 1), Some(b'x' | b'X')) {
            self.at += 2;
            let digits = self.at;
            while matches!(self.bytes.get(self.at), Some(b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')) {
                self.at += 1;
            }
            if digits == self.at {
                return Err(());
            }
            let text = std::str::from_utf8(&self.bytes[digits..self.at]).map_err(|_| ())?;
            let value = i64::from_str_radix(text, 16).map_err(|_| ())?;
            return Ok(JsonNumber {
                raw: None,
                kind: NumberKind::Integer,
                integer: Some(if negative { -value } else { value }),
                real: if negative { -(value as f64) } else { value as f64 },
            });
        }
        let integer_digits = self.at;
        match self.bytes.get(self.at) {
            Some(b'0') => {
                self.at += 1;
                if matches!(self.bytes.get(self.at), Some(b'0'..=b'9')) {
                    return Err(());
                }
            }
            Some(b'1'..=b'9') => {
                while matches!(self.bytes.get(self.at), Some(b'0'..=b'9')) {
                    self.at += 1;
                }
            }
            Some(b'.') if self.lenient => standard = false,
            _ => return Err(()),
        }
        let has_integer_digits = self.at > integer_digits;
        let mut real = false;
        if self.bytes.get(self.at) == Some(&b'.') {
            self.at += 1;
            real = true;
            let fraction = self.at;
            while matches!(self.bytes.get(self.at), Some(b'0'..=b'9')) {
                self.at += 1;
            }
            // 严格 JSON 要求小数点后至少一位；JSON5 允许 `1.`。
            if self.at == fraction {
                if !self.lenient {
                    return Err(());
                }
                standard = false;
            }
            if !has_integer_digits {
                standard = false;
            }
        }
        if matches!(self.bytes.get(self.at), Some(b'e' | b'E')) {
            self.at += 1;
            real = true;
            if matches!(self.bytes.get(self.at), Some(b'+' | b'-')) {
                self.at += 1;
            }
            if !matches!(self.bytes.get(self.at), Some(b'0'..=b'9')) {
                return Err(());
            }
            while matches!(self.bytes.get(self.at), Some(b'0'..=b'9')) {
                self.at += 1;
            }
        }
        let raw = std::str::from_utf8(&self.bytes[start..self.at]).map_err(|_| ())?.to_string();
        let value = raw.parse::<f64>().map_err(|_| ())?;
        Ok(JsonNumber {
            raw: if standard { Some(raw.clone()) } else { None },
            kind: if real { NumberKind::Real } else { NumberKind::Integer },
            integer: if real { None } else { raw.parse::<i64>().ok() },
            real: value,
        })
    }

    fn array(&mut self) -> Result<JsonValue, ()> {
        self.at += 1;
        self.skip_whitespace();
        let mut items = Vec::new();
        if self.bytes.get(self.at) == Some(&b']') {
            self.at += 1;
            return Ok(JsonValue::Array(items));
        }
        loop {
            items.push(self.value()?);
            self.skip_whitespace();
            match self.bytes.get(self.at) {
                Some(b',') => {
                    self.at += 1;
                    if self.lenient {
                        self.skip_whitespace();
                        if self.bytes.get(self.at) == Some(&b']') {
                            self.at += 1;
                            return Ok(JsonValue::Array(items));
                        }
                    }
                }
                Some(b']') => {
                    self.at += 1;
                    return Ok(JsonValue::Array(items));
                }
                _ => return Err(()),
            }
        }
    }

    fn object(&mut self) -> Result<JsonValue, ()> {
        self.at += 1;
        self.skip_whitespace();
        let mut members = Vec::new();
        if self.bytes.get(self.at) == Some(&b'}') {
            self.at += 1;
            return Ok(JsonValue::Object(members));
        }
        loop {
            self.skip_whitespace();
            let key = self.key()?;
            self.skip_whitespace();
            if self.bytes.get(self.at) != Some(&b':') {
                return Err(());
            }
            self.at += 1;
            let value = self.value()?;
            members.push((key, value));
            self.skip_whitespace();
            match self.bytes.get(self.at) {
                Some(b',') => {
                    self.at += 1;
                    if self.lenient {
                        self.skip_whitespace();
                        if self.bytes.get(self.at) == Some(&b'}') {
                            self.at += 1;
                            return Ok(JsonValue::Object(members));
                        }
                    }
                }
                Some(b'}') => {
                    self.at += 1;
                    return Ok(JsonValue::Object(members));
                }
                _ => return Err(()),
            }
        }
    }

    /// 对象键：标准 JSON 只认双引号字符串，JSON5 还接受单引号字符串与
    /// ECMAScript 标识符名的裸键。
    fn key(&mut self) -> Result<String, ()> {
        match self.bytes.get(self.at) {
            Some(b'"') => self.string(b'"'),
            Some(b'\'') if self.lenient => self.string(b'\''),
            Some(byte) if self.lenient && is_identifier_start(*byte) => {
                let start = self.at;
                self.at += 1;
                while matches!(self.bytes.get(self.at), Some(byte) if is_identifier_continue(*byte)) {
                    self.at += 1;
                }
                std::str::from_utf8(&self.bytes[start..self.at]).map(str::to_string).map_err(|_| ())
            }
            _ => Err(()),
        }
    }

    fn string(&mut self, quote: u8) -> Result<String, ()> {
        read_json_string(self.bytes, &mut self.at, quote, self.lenient)
    }
}

/// JSON5 裸键允许的起始字符：`$`、`_`、ASCII 字母与非 ASCII 字节。
fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte == b'$' || byte >= 0x80
}

/// 标识符后续字符额外允许数字。
fn is_identifier_continue(byte: u8) -> bool {
    is_identifier_start(byte) || byte.is_ascii_digit()
}

/// `Infinity` 的规范化文本与数值。
fn infinity_number(negative: bool) -> JsonNumber {
    JsonNumber {
        raw: Some(if negative { "-9e999".to_string() } else { "9e999".to_string() }),
        kind: NumberKind::Real,
        integer: None,
        real: if negative { f64::NEG_INFINITY } else { f64::INFINITY },
    }
}

/// 读取一个 JSON 字符串，处理标准转义与代理对；宽松模式额外接受单引号、
/// `\'`、`\v`、`\0` 与 `\xHH`。
fn read_json_string(bytes: &[u8], at: &mut usize, quote: u8, lenient: bool) -> Result<String, ()> {
    if bytes.get(*at) != Some(&quote) {
        return Err(());
    }
    *at += 1;
    let mut out = String::new();
    loop {
        let Some(byte) = bytes.get(*at)
        else {
            return Err(());
        };
        match *byte {
            byte if byte == quote => {
                *at += 1;
                return Ok(out);
            }
            b'\\' => {
                *at += 1;
                let escape = *bytes.get(*at).ok_or(())?;
                *at += 1;
                match escape {
                    b'"' => out.push('"'),
                    b'\'' if lenient => out.push('\''),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'b' => out.push('\u{8}'),
                    b'f' => out.push('\u{c}'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'v' if lenient => out.push('\u{b}'),
                    b'0' if lenient => out.push('\0'),
                    b'x' if lenient => out.push(read_hex_byte(bytes, at)? as char),
                    b'u' => out.push(read_unicode_escape(bytes, at)?),
                    _ => return Err(()),
                }
            }
            byte if byte < 0x20 => return Err(()),
            _ => {
                let start = *at;
                while let Some(byte) = bytes.get(*at) {
                    if *byte == quote || *byte == b'\\' || *byte < 0x20 {
                        break;
                    }
                    *at += 1;
                }
                let text = std::str::from_utf8(&bytes[start..*at]).map_err(|_| ())?;
                out.push_str(text);
            }
        }
    }
}

/// `\xHH`：两位十六进制字节。
fn read_hex_byte(bytes: &[u8], at: &mut usize) -> Result<u8, ()> {
    let mut value = 0u8;
    for _ in 0..2 {
        let byte = *bytes.get(*at).ok_or(())?;
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return Err(()),
        };
        value = value * 16 + digit;
        *at += 1;
    }
    Ok(value)
}

fn read_unicode_escape(bytes: &[u8], at: &mut usize) -> Result<char, ()> {
    let value = read_hex4(bytes, at)?;
    let high = value >= 0xd800 && value <= 0xdbff;
    if high {
        if bytes.get(*at) != Some(&b'\\') || bytes.get(*at + 1) != Some(&b'u') {
            return Err(());
        }
        *at += 2;
        let low = read_hex4(bytes, at)?;
        if !(0xdc00..=0xdfff).contains(&low) {
            return Err(());
        }
        let combined = 0x10000 + ((value as u32 - 0xd800) << 10) + (low as u32 - 0xdc00);
        return char::from_u32(combined).ok_or(());
    }
    char::from_u32(value as u32).ok_or(())
}

fn read_hex4(bytes: &[u8], at: &mut usize) -> Result<u16, ()> {
    let mut value = 0u16;
    for _ in 0..4 {
        let byte = *bytes.get(*at).ok_or(())?;
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return Err(()),
        };
        value = value * 16 + u16::from(digit);
        *at += 1;
    }
    Ok(value)
}

/// 解析一条 `$` 开头的 JSON 路径。
fn parse_path_steps(text: &str) -> Result<Vec<PathStep>, PathError> {
    let bytes = text.as_bytes();
    if bytes.first() != Some(&b'$') {
        return Err(invalid_path());
    }
    let mut at = 1usize;
    let mut steps = Vec::new();
    while at < bytes.len() {
        match bytes[at] {
            b'.' => {
                at += 1;
                if at >= bytes.len() {
                    return Err(invalid_path());
                }
                if bytes[at] == b'"' {
                    let key = read_json_string(bytes, &mut at, b'"', false).map_err(|_| invalid_path())?;
                    steps.push(PathStep::Key(key));
                }
                else {
                    let start = at;
                    while at < bytes.len() && bytes[at] != b'.' && bytes[at] != b'[' {
                        at += 1;
                    }
                    if start == at {
                        return Err(invalid_path());
                    }
                    steps.push(PathStep::Key(text[start..at].to_string()));
                }
            }
            b'[' => {
                at += 1;
                if at >= bytes.len() {
                    return Err(incomplete_path());
                }
                if bytes[at] == b'#' {
                    at += 1;
                    if at < bytes.len() && bytes[at] == b'-' {
                        at += 1;
                        let start = at;
                        while at < bytes.len() && bytes[at].is_ascii_digit() {
                            at += 1;
                        }
                        if start == at {
                            return Err(invalid_path());
                        }
                        let count: usize = text[start..at].parse().map_err(|_| invalid_path())?;
                        if at >= bytes.len() {
                            return Err(incomplete_path());
                        }
                        if bytes[at] != b']' {
                            return Err(invalid_path());
                        }
                        at += 1;
                        steps.push(PathStep::FromEnd(count));
                    }
                    else {
                        if at >= bytes.len() {
                            return Err(incomplete_path());
                        }
                        if bytes[at] != b']' {
                            return Err(invalid_path());
                        }
                        at += 1;
                        steps.push(PathStep::Append);
                    }
                }
                else {
                    let start = at;
                    while at < bytes.len() && bytes[at].is_ascii_digit() {
                        at += 1;
                    }
                    if start == at {
                        return Err(invalid_path());
                    }
                    let index: usize = text[start..at].parse().map_err(|_| invalid_path())?;
                    if at >= bytes.len() {
                        return Err(incomplete_path());
                    }
                    if bytes[at] != b']' {
                        return Err(invalid_path());
                    }
                    at += 1;
                    steps.push(PathStep::Index(index));
                }
            }
            _ => return Err(invalid_path()),
        }
    }
    Ok(steps)
}

/// 按路径读取节点；任一环节类型不符或越界都返回 `None`。
fn lookup<'a>(value: &'a JsonValue, steps: &[PathStep]) -> Option<&'a JsonValue> {
    let mut current = value;
    for step in steps {
        current = match (step, current) {
            (PathStep::Key(key), JsonValue::Object(members)) => members.iter().find(|(name, _)| name == key).map(|(_, value)| value)?,
            (PathStep::Index(index), JsonValue::Array(items)) => items.get(*index)?,
            (PathStep::FromEnd(count), JsonValue::Array(items)) => {
                if *count == 0 {
                    return None;
                }
                items.get(items.len().checked_sub(*count)?)?
            }
            // `$[#]` 只在写入时有意义。
            _ => return None,
        };
    }
    Some(current)
}

/// 按路径删除；返回 `true` 表示删除目标是根节点本身。
fn remove_path(value: &mut JsonValue, steps: &[PathStep]) -> bool {
    if steps.is_empty() {
        return true;
    }
    let (last, parents) = steps.split_last().unwrap();
    let mut current = value;
    for step in parents {
        current = match (step, current) {
            (PathStep::Key(key), JsonValue::Object(members)) => match members.iter_mut().find(|(name, _)| name == key) {
                Some((_, value)) => value,
                None => return false,
            },
            (PathStep::Index(index), JsonValue::Array(items)) => match items.get_mut(*index) {
                Some(value) => value,
                None => return false,
            },
            (PathStep::FromEnd(count), JsonValue::Array(items)) => {
                if *count == 0 {
                    return false;
                }
                let Some(index) = items.len().checked_sub(*count)
                else {
                    return false;
                };
                match items.get_mut(index) {
                    Some(value) => value,
                    None => return false,
                }
            }
            _ => return false,
        };
    }
    match (last, current) {
        (PathStep::Key(key), JsonValue::Object(members)) => {
            members.retain(|(name, _)| name != key);
            false
        }
        (PathStep::Index(index), JsonValue::Array(items)) => {
            if *index < items.len() {
                items.remove(*index);
            }
            false
        }
        (PathStep::FromEnd(count), JsonValue::Array(items)) => {
            if *count > 0 {
                if let Some(index) = items.len().checked_sub(*count) {
                    items.remove(index);
                }
            }
            false
        }
        _ => false,
    }
}

/// 按模式写入：中间缺失的对象成员在 `set` / `insert` 下会补建容器。
fn assign_path(value: &mut JsonValue, steps: &[PathStep], new: &JsonValue, mode: Mode) {
    if steps.is_empty() {
        if mode != Mode::Insert {
            *value = new.clone();
        }
        return;
    }
    let (first, rest) = steps.split_first().unwrap();
    match (first, value) {
        (PathStep::Key(key), JsonValue::Object(members)) => {
            if let Some(index) = members.iter().position(|(name, _)| name == key) {
                if rest.is_empty() {
                    if mode != Mode::Insert {
                        members[index].1 = new.clone();
                    }
                }
                else {
                    assign_path(&mut members[index].1, rest, new, mode);
                }
            }
            else if mode != Mode::Replace {
                if rest.is_empty() {
                    members.push((key.clone(), new.clone()));
                }
                else {
                    let mut child = JsonValue::Object(Vec::new());
                    assign_path(&mut child, rest, new, mode);
                    members.push((key.clone(), child));
                }
            }
        }
        (PathStep::Index(_) | PathStep::Append, JsonValue::Array(items)) => {
            let index = match first {
                PathStep::Append => items.len(),
                PathStep::Index(value) => *value,
                _ => unreachable!(),
            };
            if index < items.len() {
                if rest.is_empty() {
                    if mode != Mode::Insert {
                        items[index] = new.clone();
                    }
                }
                else {
                    assign_path(&mut items[index], rest, new, mode);
                }
            }
            else if index == items.len() && mode != Mode::Replace {
                if rest.is_empty() {
                    items.push(new.clone());
                }
                else {
                    let mut child = JsonValue::Object(Vec::new());
                    assign_path(&mut child, rest, new, mode);
                    items.push(child);
                }
            }
        }
        (PathStep::FromEnd(count), JsonValue::Array(items)) => {
            if *count > 0 && !rest.is_empty() {
                if let Some(index) = items.len().checked_sub(*count) {
                    assign_path(&mut items[index], rest, new, mode);
                }
            }
            else if *count > 0 && rest.is_empty() && mode != Mode::Insert {
                if let Some(index) = items.len().checked_sub(*count) {
                    items[index] = new.clone();
                }
            }
        }
        _ => {}
    }
}
