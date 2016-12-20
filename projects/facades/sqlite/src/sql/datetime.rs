//! SQLite 日期时间函数：文本解析、修饰符与格式化。
//!
//! 期望值来自 better-sqlite3 12.x（SQLite 3.53）与 CPython 3.49 的 `sqlite3`，
//! 不是本仓实现的自我参照。模块只做纯计算，宿主绑定层不参与。

use std::time::{SystemTime, UNIX_EPOCH};

use super::engine::Cell;

/// 支持的日期时间函数。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DateTimeFunction {
    /// `date(...)`。
    Date,
    /// `time(...)`。
    Time,
    /// `datetime(...)`。
    Datetime,
    /// `julianday(...)`。
    JulianDay,
    /// `unixepoch(...)`。
    UnixEpoch,
    /// `strftime(...)`。
    Strftime,
    /// `timediff(...)`。
    Timediff,
}

/// 一天的毫秒数。
const MS_PER_DAY: f64 = 86_400_000.0;
/// `1970-01-01 00:00:00` 对应的内部毫秒数。
const UNIX_EPOCH_IJD: i64 = 210_866_760_000_000;
/// 合法儒略日的上界（不含），对应 10000-01-01。
const MAX_IJD: f64 = 5_373_484.5 * MS_PER_DAY;

/// 一个日期时间值。字段缓存保留入参的原始年月日与时秒，修饰符只在
/// 需要重排时清空缓存，这样 `24:00:00` 这类只滚动一天的时刻仍按原样输出。
#[derive(Clone, Copy)]
struct Stamp {
    /// 自儒略日零点起的毫秒数。
    ijd: i64,
    /// 缓存的年月日。
    ymd: Option<(i32, i32, i32)>,
    /// 缓存的时分秒毫秒。
    hms: Option<(i32, i32, i32, i32)>,
    /// 输出秒的小数部分。
    subsec: bool,
}

impl Stamp {
    fn new(ijd: i64) -> Self {
        Self { ijd, ymd: None, hms: None, subsec: false }
    }

    fn from_civil(y: i32, m: i32, d: i32, hh: i32, mi: i32, ss: i32, ms: i32) -> Self {
        let ijd = compute_jd(y, m, d) + i64::from(hh) * 3_600_000 + i64::from(mi) * 60_000 + i64::from(ss) * 1000 + i64::from(ms);
        Self { ijd, ymd: Some((y, m, d)), hms: Some((hh, mi, ss, ms)), subsec: false }
    }

    fn set_ijd(&mut self, ijd: i64) {
        self.ijd = ijd;
        self.ymd = None;
        self.hms = None;
    }

    fn valid(&self) -> bool {
        self.ijd >= 0 && (self.ijd as f64) < MAX_IJD
    }

    /// 输出用的年月日：缓存值若是真实存在的日历日就直接使用，否则按儒略日
    /// 重算，这样 `2024-02-30` 会规范成 `2024-03-01`，而合法的 `24:00` 不滚动日期。
    fn output_ymd(&mut self) -> (i32, i32, i32) {
        if let Some(value) = self.ymd {
            if value.2 <= days_in_month(value.0, value.1) {
                return value;
            }
        }
        let value = compute_ymd(self.ijd);
        self.ymd = Some(value);
        value
    }

    fn output_hms(&mut self) -> (i32, i32, i32, i32) {
        if let Some(value) = self.hms {
            return value;
        }
        let value = compute_hms(self.ijd);
        self.hms = Some(value);
        value
    }

    fn hms_ms(&mut self) -> i64 {
        let (hh, mi, ss, ms) = self.output_hms();
        i64::from(hh) * 3_600_000 + i64::from(mi) * 60_000 + i64::from(ss) * 1000 + i64::from(ms)
    }
}

/// 执行一个日期时间函数。
pub(crate) fn eval(function: DateTimeFunction, args: &[Cell]) -> Cell {
    match function {
        DateTimeFunction::Timediff => timediff(args),
        DateTimeFunction::Strftime => strftime(args),
        _ => eval_instant(function, args),
    }
}

fn eval_instant(function: DateTimeFunction, args: &[Cell]) -> Cell {
    let mut stamp = if args.is_empty() {
        now_stamp()
    }
    else {
        match parse_value(&args[0]) {
            Some(value) => value,
            None => return Cell::Null,
        }
    };
    let original = args.first().cloned().unwrap_or_else(|| Cell::Text("now".to_string()));
    let modifiers: &[Cell] = if args.is_empty() { &[] } else { &args[1..] };
    // 入参解释型修饰符先于范围校验生效，这样超出儒略日范围的数值还能被
    // `auto` / `unixepoch` 重新解释；其余修饰符要求当前值已经合法。
    let Some(start) = apply_leading_input_type(&mut stamp, modifiers, &original)
    else {
        return Cell::Null;
    };
    if !stamp.valid() {
        return Cell::Null;
    }
    if !apply_modifiers(&mut stamp, &modifiers[start..]) {
        return Cell::Null;
    }
    if !stamp.valid() {
        return Cell::Null;
    }
    match function {
        DateTimeFunction::Date => {
            let (y, m, d) = stamp.output_ymd();
            Cell::Text(format!("{:04}-{:02}-{:02}", y, m, d))
        }
        DateTimeFunction::Time => {
            let (hh, mi, ss, ms) = stamp.output_hms();
            if stamp.subsec {
                Cell::Text(format!("{:02}:{:02}:{:02}.{:03}", hh, mi, ss, ms))
            }
            else {
                Cell::Text(format!("{:02}:{:02}:{:02}", hh, mi, ss))
            }
        }
        DateTimeFunction::Datetime => {
            let (y, m, d) = stamp.output_ymd();
            let (hh, mi, ss, ms) = stamp.output_hms();
            if stamp.subsec {
                Cell::Text(format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}", y, m, d, hh, mi, ss, ms))
            }
            else {
                Cell::Text(format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, hh, mi, ss))
            }
        }
        DateTimeFunction::JulianDay => Cell::Real(stamp.ijd as f64 / MS_PER_DAY),
        DateTimeFunction::UnixEpoch => {
            let offset = stamp.ijd - UNIX_EPOCH_IJD;
            if stamp.subsec { Cell::Real(offset as f64 / 1000.0) } else { Cell::Integer(offset.div_euclid(1000)) }
        }
        _ => Cell::Null,
    }
}

fn now_stamp() -> Stamp {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    Stamp::new(UNIX_EPOCH_IJD + elapsed.as_millis() as i64)
}

/// 解析一个日期时间入参：`NULL` 与不可解析值都返回 `None`。范围校验推迟到
/// 修饰符之后，因为 `unixepoch` / `auto` 可以把越界的数值重新解释。
fn parse_value(cell: &Cell) -> Option<Stamp> {
    match cell {
        Cell::Null | Cell::Blob(_) => None,
        Cell::Integer(value) => numeric_stamp(*value as f64),
        Cell::Real(value) => numeric_stamp(*value),
        Cell::Text(text) => parse_text(text),
    }
}

/// 数值入参按儒略日解释，与 `julianday(1.5) = 1.5` 一致。
fn numeric_stamp(value: f64) -> Option<Stamp> {
    if !value.is_finite() {
        return None;
    }
    Some(Stamp::new((value * MS_PER_DAY) as i64))
}

fn parse_text(text: &str) -> Option<Stamp> {
    if text.eq_ignore_ascii_case("now") {
        return Some(now_stamp());
    }
    let bytes = text.as_bytes();
    let mut pos = 0usize;
    if bytes.first().is_some_and(|byte| is_space(*byte)) {
        return None;
    }
    let mut ymd: Option<(i32, i32, i32)> = None;
    if !looks_like_date(bytes) && !looks_like_time(bytes) {
        return numeric_stamp(full_number(text)?);
    }
    if looks_like_date(bytes) {
        ymd = Some(parse_date(bytes, &mut pos)?);
        if pos < bytes.len() {
            match bytes[pos] {
                b'T' => {
                    pos += 1;
                    while pos < bytes.len() && is_space(bytes[pos]) {
                        pos += 1;
                    }
                    if pos == bytes.len() {
                        return None;
                    }
                }
                byte if is_space(byte) => {
                    while pos < bytes.len() && is_space(bytes[pos]) {
                        pos += 1;
                    }
                }
                _ => return None,
            }
        }
    }
    else if looks_like_time(bytes) {
        ymd = Some((2000, 1, 1));
    }
    let mut hms: Option<(i32, i32, i32, i32)> = None;
    if pos < bytes.len() && ymd.is_some() {
        hms = Some(parse_time(bytes, &mut pos)?);
    }
    let mut tz_minutes: i64 = 0;
    let mut has_tz = false;
    let mut spaces = pos;
    while spaces < bytes.len() && is_space(bytes[spaces]) {
        spaces += 1;
    }
    if spaces < bytes.len() && hms.is_some() {
        pos = spaces;
        match bytes[pos] {
            b'Z' | b'z' => {
                pos += 1;
                has_tz = true;
            }
            b'+' | b'-' | b'0'..=b'9' => {
                tz_minutes = parse_timezone(bytes, &mut pos)?;
                has_tz = true;
            }
            _ => return None,
        }
    }
    while pos < bytes.len() && is_space(bytes[pos]) {
        pos += 1;
    }
    if pos != bytes.len() {
        return None;
    }
    let (y, m, d) = ymd.unwrap_or_else(|| {
        // 只有时分的输入默认落在 `2000-01-01`。
        (2000, 1, 1)
    });
    let (hh, mi, ss, ms) = hms.unwrap_or((0, 0, 0, 0));
    let mut stamp = Stamp::from_civil(y, m, d, hh, mi, ss, ms);
    if has_tz {
        // 后缀时区是"本地时间转 UTC"：`+05:30` 表示本地时间比 UTC 快 5 小时 30 分。
        stamp.set_ijd(stamp.ijd - tz_minutes * 60_000);
        stamp.subsec = false;
    }
    Some(stamp)
}

fn looks_like_date(bytes: &[u8]) -> bool {
    bytes.len() >= 10 && bytes[..4].iter().all(u8::is_ascii_digit) && bytes[4] == b'-'
}

fn looks_like_time(bytes: &[u8]) -> bool {
    bytes.len() >= 3 && bytes[0].is_ascii_digit() && bytes[1].is_ascii_digit() && bytes[2] == b':'
}

/// 解析 `YYYY-MM-DD`，返回原样年月日；日期溢出由输出阶段规范化。
fn parse_date(bytes: &[u8], pos: &mut usize) -> Option<(i32, i32, i32)> {
    let y = four_digits(bytes, *pos)?;
    if bytes.get(*pos + 4) != Some(&b'-') {
        return None;
    }
    let m = two_digits(bytes, *pos + 5)?;
    if bytes.get(*pos + 7) != Some(&b'-') {
        return None;
    }
    let d = two_digits(bytes, *pos + 8)?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    *pos += 10;
    Some((y as i32, m as i32, d as i32))
}

/// 解析 `HH:MM[:SS[.SSS...]]`，小数秒按前三位截断。
fn parse_time(bytes: &[u8], pos: &mut usize) -> Option<(i32, i32, i32, i32)> {
    let hh = two_digits(bytes, *pos)?;
    if bytes.get(*pos + 2) != Some(&b':') {
        return None;
    }
    let mi = two_digits(bytes, *pos + 3)?;
    if !(0..=24).contains(&hh) || !(0..=59).contains(&mi) {
        return None;
    }
    *pos += 5;
    let mut ss = 0i64;
    let mut ms = 0i64;
    if bytes.get(*pos) == Some(&b':') {
        ss = i64::from(two_digits(bytes, *pos + 1)?);
        if !(0..=59).contains(&ss) {
            return None;
        }
        *pos += 3;
        if bytes.get(*pos) == Some(&b'.') {
            *pos += 1;
            let start = *pos;
            while *pos < bytes.len() && bytes[*pos].is_ascii_digit() {
                *pos += 1;
            }
            let digits = &bytes[start..*pos];
            if digits.is_empty() {
                return None;
            }
            for index in 0..3 {
                ms = ms * 10 + i64::from(digits.get(index).map_or(0, |byte| byte - b'0'));
            }
        }
    }
    Some((hh as i32, mi as i32, ss as i32, ms as i32))
}

/// 解析时区后缀 `±HH:MM[:SS[.SSS]]`，返回偏移分钟数。
fn parse_timezone(bytes: &[u8], pos: &mut usize) -> Option<i64> {
    let mut index = *pos;
    let mut negative = false;
    if matches!(bytes.get(index), Some(b'+') | Some(b'-')) {
        negative = bytes[index] == b'-';
        index += 1;
    }
    let hh = two_digits(bytes, index)?;
    if bytes.get(index + 2) != Some(&b':') {
        return None;
    }
    let mi = two_digits(bytes, index + 3)?;
    if !(0..=24).contains(&hh) || !(0..=59).contains(&mi) {
        return None;
    }
    index += 5;
    if bytes.get(index) == Some(&b':') {
        let ss = i64::from(two_digits(bytes, index + 1)?);
        if !(0..=59).contains(&ss) {
            return None;
        }
        index += 3;
    }
    let minutes = i64::from(hh) * 60 + i64::from(mi);
    *pos = index;
    Some(if negative { -minutes } else { minutes })
}

fn four_digits(bytes: &[u8], start: usize) -> Option<u32> {
    if start + 4 > bytes.len() {
        return None;
    }
    let mut value = 0u32;
    for index in 0..4 {
        let byte = bytes[start + index];
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u32::from(byte - b'0');
    }
    Some(value)
}

fn two_digits(bytes: &[u8], start: usize) -> Option<u32> {
    if start + 2 > bytes.len() {
        return None;
    }
    let first = bytes[start];
    let second = bytes[start + 1];
    if !first.is_ascii_digit() || !second.is_ascii_digit() {
        return None;
    }
    Some(u32::from(first - b'0') * 10 + u32::from(second - b'0'))
}

fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// 由年月日算出内部毫秒数，算式与 SQLite 的 `computeJD` 相同。
fn compute_jd(y: i32, m: i32, d: i32) -> i64 {
    let (mut year, mut month) = (y, m);
    if month <= 2 {
        year -= 1;
        month += 12;
    }
    let a = year / 100;
    let b = 2 - a + a / 4;
    let x1 = 36525 * (year + 4716) / 100;
    let x2 = 306001 * (month + 1) / 10000;
    let days = f64::from(x1 + x2 + d + b - 1524) - 0.5;
    (days * MS_PER_DAY) as i64
}

/// 由内部毫秒数还原年月日，常量格里高利修正与 SQLite 一致。
fn compute_ymd(ijd: i64) -> (i32, i32, i32) {
    let z = (ijd + 43_200_000) / 86_400_000;
    let a = ((z as f64 - 1_867_216.25) / 36_524.25) as i64;
    let a = z + 1 + a - a / 4;
    let b = a + 1524;
    let c = ((b as f64 - 122.1) / 365.25) as i64;
    let d = 36525 * (c & 32767) / 100;
    let e = ((b - d) as f64 / 30.6001) as i64;
    let x1 = (30.6001 * e as f64) as i64;
    let day = b - d - x1;
    let month = if e < 14 { e - 1 } else { e - 13 };
    let year = if month > 2 { c - 4716 } else { c - 4715 };
    (year as i32, month as i32, day as i32)
}

/// 由内部毫秒数还原时分秒毫秒。
fn compute_hms(ijd: i64) -> (i32, i32, i32, i32) {
    let remaining = (ijd + 43_200_000) % 86_400_000;
    let seconds = remaining / 1000;
    let ms = remaining % 1000;
    ((seconds / 3600) as i32, ((seconds % 3600) / 60) as i32, (seconds % 60) as i32, ms as i32)
}

fn is_leap(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i32, month: i32) -> i32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(year) {
                29
            }
            else {
                28
            }
        }
        _ => 31,
    }
}

/// 处理首位的入参解释型修饰符，返回真正开始应用普通修饰符的下标。`None`
/// 表示修饰符名字是入参解释型但解释失败（整条结果为 `NULL`）。
fn apply_leading_input_type(stamp: &mut Stamp, modifiers: &[Cell], original: &Cell) -> Option<usize> {
    let Some(Cell::Text(text)) = modifiers.first()
    else {
        return Some(0);
    };
    match apply_input_type(stamp, original, text) {
        Some(true) => Some(1),
        Some(false) => None,
        None => Some(0),
    }
}

/// 修饰符循环。返回 `false` 表示出现未知修饰符，整条表达式应得到 `NULL`。
fn apply_modifiers(stamp: &mut Stamp, modifiers: &[Cell]) -> bool {
    for cell in modifiers {
        let Cell::Text(text) = cell
        else {
            return false;
        };
        if let Some(rest) = strip_prefix_ignore_case(text, "start of ") {
            // `start of month` 归到当月 1 日，`start of year` 归到当年 1 月 1 日，
            // 三者都清空时分秒但保留缓存的年月日。
            let (mut year, mut month, day) = stamp.output_ymd();
            let day = match rest.to_ascii_lowercase().as_str() {
                "month" => 1,
                "year" => {
                    month = 1;
                    1
                }
                "day" => day,
                _ => return false,
            };
            let _ = &mut year;
            let ijd = compute_jd(year, month, day);
            stamp.set_ijd(ijd);
            stamp.ymd = Some((year, month, day));
            stamp.hms = Some((0, 0, 0, 0));
            continue;
        }
        if let Some(rest) = strip_prefix_ignore_case(text, "weekday") {
            let rest = rest.trim_start_matches(|character: char| character.is_ascii_whitespace());
            let Some((target, used)) = parse_integer_prefix(rest)
            else {
                return false;
            };
            if !(0..=6).contains(&target) {
                return false;
            }
            if !rest[used..].bytes().all(is_space) {
                return false;
            }
            let days = day_number(stamp.ijd);
            // 儒略日整数以星期一为 0，`weekday N` 的目标值以星期日为 0。
            let current = (days + 1).rem_euclid(7);
            let delta = (i64::from(target) - current).rem_euclid(7);
            let ijd = stamp.ijd + delta * 86_400_000;
            stamp.set_ijd(ijd);
            continue;
        }
        if text.eq_ignore_ascii_case("localtime") {
            if !to_localtime(stamp) {
                return false;
            }
            continue;
        }
        if text.eq_ignore_ascii_case("utc") {
            if !to_utc(stamp) {
                return false;
            }
            continue;
        }
        if text.eq_ignore_ascii_case("subsec") || text.eq_ignore_ascii_case("subsecond") {
            stamp.subsec = true;
            continue;
        }
        if let Some(offset) = parse_timezone_modifier(text) {
            let ijd = stamp.ijd + offset.millis;
            stamp.set_ijd(ijd);
            continue;
        }
        if let Some((amount, unit)) = parse_amount_modifier(text) {
            apply_amount(stamp, amount, unit);
            continue;
        }
        return false;
    }
    true
}

/// 入参解释型修饰符：`unixepoch` / `julianday` / `auto`。
/// 返回 `None` 表示这个名字不是入参解释型；`Some(false)` 表示解释失败。
fn apply_input_type(stamp: &mut Stamp, original: &Cell, name: &str) -> Option<bool> {
    let lowercase = name.to_ascii_lowercase();
    if lowercase != "unixepoch" && lowercase != "julianday" && lowercase != "auto" {
        return None;
    }
    if lowercase == "auto" {
        let numeric = match original {
            Cell::Integer(value) => Some(*value as f64),
            Cell::Real(value) if value.is_finite() => Some(*value),
            Cell::Text(text) => full_number(text),
            _ => None,
        };
        return Some(match numeric {
            // 文本日期与 `now` 走普通解析，`auto` 不改变语义。
            None => true,
            Some(value) => {
                if value >= 0.0 && value < MAX_IJD / MS_PER_DAY {
                    stamp.set_ijd((value * MS_PER_DAY) as i64);
                }
                else {
                    stamp.set_ijd((value * 1000.0) as i64 + UNIX_EPOCH_IJD);
                }
                true
            }
        });
    }
    // 数值文本（整体可解析）与数值类型都按数值解释，非数值文本解释失败。
    let value = match original {
        Cell::Integer(value) => *value as f64,
        Cell::Real(value) if value.is_finite() => *value,
        Cell::Text(text) => match full_number(text) {
            Some(value) => value,
            None => return Some(false),
        },
        _ => return Some(false),
    };
    if lowercase == "unixepoch" {
        stamp.set_ijd((value * 1000.0) as i64 + UNIX_EPOCH_IJD);
    }
    else {
        stamp.set_ijd((value * MS_PER_DAY) as i64);
    }
    Some(true)
}

#[derive(Clone, Copy)]
enum AmountUnit {
    Months,
    Years,
    Days,
    Hours,
    Minutes,
    Seconds,
}

fn apply_amount(stamp: &mut Stamp, amount: f64, unit: AmountUnit) {
    let amount = amount.clamp(-20_000.0, 20_000.0);
    match unit {
        AmountUnit::Months => {
            let whole = amount.trunc() as i32;
            let (y, m, d) = stamp.output_ymd();
            let hms = stamp.hms_ms();
            let (year, month) = shift_months(y, m, whole);
            let ijd = compute_jd(year, month, d) + hms;
            let fraction = (amount - f64::from(whole)) * 30.0 * MS_PER_DAY;
            stamp.set_ijd(ijd + round_half_away(fraction));
        }
        AmountUnit::Years => {
            let whole = amount.trunc() as i32;
            let (y, m, d) = stamp.output_ymd();
            let hms = stamp.hms_ms();
            let ijd = compute_jd(y + whole, m, d) + hms;
            let fraction = (amount - f64::from(whole)) * 365.25 * MS_PER_DAY;
            stamp.set_ijd(ijd + round_half_away(fraction));
        }
        AmountUnit::Days | AmountUnit::Hours | AmountUnit::Minutes | AmountUnit::Seconds => {
            let unit_ms = match unit {
                AmountUnit::Days => MS_PER_DAY,
                AmountUnit::Hours => 3_600_000.0,
                AmountUnit::Minutes => 60_000.0,
                _ => 1000.0,
            };
            let ijd = stamp.ijd + round_half_away(amount * unit_ms);
            stamp.set_ijd(ijd);
        }
    }
}

fn shift_months(y: i32, m: i32, count: i32) -> (i32, i32) {
    let mut year = y;
    let mut month = m + count;
    let extra = if month > 0 { (month - 1) / 12 } else { (month - 12) / 12 };
    year += extra;
    month -= extra * 12;
    (year, month)
}

fn round_half_away(value: f64) -> i64 {
    if value < 0.0 { (value - 0.5) as i64 } else { (value + 0.5) as i64 }
}

fn day_number(ijd: i64) -> i64 {
    (ijd + 43_200_000) / 86_400_000
}

fn strip_prefix_ignore_case<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    if text.len() >= prefix.len() && text.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes()) {
        Some(&text[prefix.len()..])
    }
    else {
        None
    }
}

/// 解析 `±NNN unit`，数字与单位之间至少要有一个空白。
fn parse_amount_modifier(text: &str) -> Option<(f64, AmountUnit)> {
    let bytes = text.as_bytes();
    let mut index = 0usize;
    if matches!(bytes.first(), Some(b'+') | Some(b'-')) {
        index = 1;
    }
    let start = index;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }
    if index < bytes.len() && bytes[index] == b'.' {
        index += 1;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
    }
    if index == start {
        return None;
    }
    if matches!(bytes.get(index), Some(b'e') | Some(b'E')) {
        index += 1;
        if matches!(bytes.get(index), Some(b'+') | Some(b'-')) {
            index += 1;
        }
        let exponent = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if index == exponent {
            return None;
        }
    }
    let number: f64 = text[..index].parse().ok()?;
    if index == bytes.len() || !is_space(bytes[index]) {
        return None;
    }
    while index < bytes.len() && is_space(bytes[index]) {
        index += 1;
    }
    let unit = match text[index..].to_ascii_lowercase().as_str() {
        "month" | "months" => AmountUnit::Months,
        "year" | "years" => AmountUnit::Years,
        "day" | "days" => AmountUnit::Days,
        "hour" | "hours" => AmountUnit::Hours,
        "minute" | "minutes" => AmountUnit::Minutes,
        "second" | "seconds" => AmountUnit::Seconds,
        _ => return None,
    };
    Some((number, unit))
}

#[derive(Clone, Copy)]
struct Offset {
    millis: i64,
}

/// 解析 `±HH:MM[:SS[.SSS]]` 这种时区修饰符，与数字修饰符共存于同一名字空间。
fn parse_timezone_modifier(text: &str) -> Option<Offset> {
    let bytes = text.as_bytes();
    let mut index = 0usize;
    let mut negative = false;
    if matches!(bytes.first(), Some(b'+') | Some(b'-')) {
        negative = bytes[0] == b'-';
        index = 1;
    }
    let hh = two_digits(bytes, index)?;
    if bytes.get(index + 2) != Some(&b':') {
        return None;
    }
    let mi = two_digits(bytes, index + 3)?;
    if !(0..=24).contains(&hh) || !(0..=59).contains(&mi) {
        return None;
    }
    index += 5;
    let mut ss = 0i64;
    let mut ms = 0i64;
    if bytes.get(index) == Some(&b':') {
        ss = i64::from(two_digits(bytes, index + 1)?);
        if !(0..=59).contains(&ss) {
            return None;
        }
        index += 3;
        if bytes.get(index) == Some(&b'.') {
            index += 1;
            let start = index;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
            let digits = &bytes[start..index];
            if digits.is_empty() {
                return None;
            }
            for position in 0..3 {
                ms = ms * 10 + i64::from(digits.get(position).map_or(0, |byte| byte - b'0'));
            }
        }
    }
    if index != bytes.len() {
        return None;
    }
    // SQLite 把 ±HH:MM 的偏移按 24 小时取模，`+24:30` 实际只加 30 分钟。
    let millis = (((i64::from(hh) * 60 + i64::from(mi)) * 60 + ss) * 1000 + ms) % 86_400_000;
    Some(Offset { millis: if negative { -millis } else { millis } })
}

fn parse_integer_prefix(text: &str) -> Option<(i32, usize)> {
    let bytes = text.as_bytes();
    let mut index = 0usize;
    let mut negative = false;
    if matches!(bytes.first(), Some(b'+') | Some(b'-')) {
        negative = bytes[0] == b'-';
        index = 1;
    }
    let start = index;
    let mut value: i64 = 0;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        value = value * 10 + i64::from(bytes[index] - b'0');
        index += 1;
    }
    if index == start || value > i64::from(i32::MAX) {
        return None;
    }
    Some((if negative { -(value as i32) } else { value as i32 }, index))
}

/// 文本整体必须是数值，用于数学函数与 `auto`。
fn full_number(text: &str) -> Option<f64> {
    let trimmed = text.trim();
    let bytes = trimmed.as_bytes();
    let mut index = 0usize;
    if matches!(bytes.first(), Some(b'+') | Some(b'-')) {
        index = 1;
    }
    let integer = index;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }
    let mut digits = index > integer;
    if index < bytes.len() && bytes[index] == b'.' {
        index += 1;
        let fraction = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        digits |= index > fraction;
    }
    if !digits {
        return None;
    }
    if matches!(bytes.get(index), Some(b'e') | Some(b'E')) {
        index += 1;
        if matches!(bytes.get(index), Some(b'+') | Some(b'-')) {
            index += 1;
        }
        let exponent = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if index == exponent {
            return None;
        }
    }
    if index != bytes.len() {
        return None;
    }
    let value: f64 = trimmed.parse().ok()?;
    if value.is_finite() { Some(value) } else { None }
}

/// ISO 星期：星期一为 1，星期日为 7。儒略日整数以星期一为 0。
fn weekday_iso(year: i32, month: i32, day: i32) -> i64 {
    day_number(compute_jd(year, month, day)).rem_euclid(7) + 1
}

fn iso_weeks_in_year(year: i32) -> i64 {
    let weekday = weekday_iso(year, 1, 1);
    if weekday == 4 || (weekday == 3 && is_leap(year)) { 53 } else { 52 }
}

fn doy_of(year: i32, month: i32, day: i32) -> i64 {
    day_number(compute_jd(year, month, day)) - day_number(compute_jd(year, 1, 1)) + 1
}

fn format_g(value: f64, precision: i32) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    let exponent = value.abs().log10().floor() as i32;
    let decimals = (precision - 1 - exponent).max(0) as usize;
    let mut text = format!("{:.*}", decimals, value);
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    text
}

fn strftime(args: &[Cell]) -> Cell {
    let Cell::Text(format) = &args[0]
    else {
        return Cell::Null;
    };
    let mut stamp = if args.len() >= 2 {
        match parse_value(&args[1]) {
            Some(value) => value,
            None => return Cell::Null,
        }
    }
    else {
        now_stamp()
    };
    let original = args.get(1).cloned().unwrap_or_else(|| Cell::Text("now".to_string()));
    let modifiers: &[Cell] = if args.len() >= 2 { &args[2..] } else { &[] };
    let Some(start) = apply_leading_input_type(&mut stamp, modifiers, &original)
    else {
        return Cell::Null;
    };
    if !stamp.valid() {
        return Cell::Null;
    }
    if !apply_modifiers(&mut stamp, &modifiers[start..]) {
        return Cell::Null;
    }
    if !stamp.valid() {
        return Cell::Null;
    }
    let (year, month, day) = stamp.output_ymd();
    let (hour, minute, second, millisecond) = stamp.output_hms();
    let julian_day = day_number(compute_jd(year, month, day));
    // 儒略日整数以星期一为 0；`%U` 用星期日为 0，`%u`/`%W` 用 ISO 编号。
    let monday = julian_day.rem_euclid(7);
    let sunday = (julian_day + 1).rem_euclid(7);
    let iso_weekday = monday + 1;
    let doy = doy_of(year, month, day);
    let doy0 = doy - 1;
    let (iso_year, iso_week_number) = iso_week(year, doy0, iso_weekday);
    let mut out = String::new();
    let mut characters = format.chars();
    while let Some(character) = characters.next() {
        if character != '%' {
            out.push(character);
            continue;
        }
        let Some(spec) = characters.next()
        else {
            return Cell::Null;
        };
        let piece = match spec {
            '%' => "%".to_string(),
            'd' => format!("{:02}", day),
            'e' => format!("{:2}", day),
            'f' => format!("{:02}.{:03}", second, millisecond),
            'F' => format!("{:04}-{:02}-{:02}", year, month, day),
            'g' => format!("{:02}", iso_year.rem_euclid(100)),
            'G' => format!("{:04}", iso_year),
            'H' => format!("{:02}", hour),
            'I' => format!("{:02}", clock12(hour)),
            'j' => format!("{:03}", doy),
            'J' => format_g(stamp.ijd as f64 / MS_PER_DAY, 16),
            'k' => format!("{:2}", hour),
            'l' => format!("{:2}", clock12(hour)),
            'm' => format!("{:02}", month),
            'M' => format!("{:02}", minute),
            'p' => {
                if hour < 12 {
                    "AM".to_string()
                }
                else {
                    "PM".to_string()
                }
            }
            'P' => {
                if hour < 12 {
                    "am".to_string()
                }
                else {
                    "pm".to_string()
                }
            }
            'R' => format!("{:02}:{:02}", hour, minute),
            's' => {
                let offset = stamp.ijd - UNIX_EPOCH_IJD;
                if stamp.subsec { format!("{:.3}", offset as f64 / 1000.0) } else { format!("{}", offset.div_euclid(1000)) }
            }
            'S' => format!("{:02}", second),
            'T' => format!("{:02}:{:02}:{:02}", hour, minute, second),
            'u' => format!("{}", iso_weekday),
            'U' => format!("{:02}", (doy0 + 7 - sunday) / 7),
            'V' => format!("{:02}", iso_week_number),
            'w' => format!("{}", sunday),
            'W' => format!("{:02}", (doy0 + 7 - monday) / 7),
            'Y' => format!("{:04}", year),
            _ => return Cell::Null,
        };
        out.push_str(&piece);
    }
    Cell::Text(out)
}

fn clock12(hour: i32) -> i32 {
    let value = hour % 12;
    if value == 0 { 12 } else { value }
}

/// 返回 ISO 周所在的年份与周号。`weekday` 是 ISO 编号（星期一为 1）。
fn iso_week(year: i32, doy0: i64, weekday: i64) -> (i32, i64) {
    let mut week = (doy0 - weekday + 10) / 7;
    let mut iso_year = year;
    if week < 1 {
        iso_year = year - 1;
        week = iso_weeks_in_year(iso_year);
    }
    else if week > iso_weeks_in_year(year) {
        iso_year = year + 1;
        week = 1;
    }
    (iso_year, week)
}

fn timediff(args: &[Cell]) -> Cell {
    let (Some(mut first), Some(mut second)) = (parse_value(&args[0]), parse_value(&args[1]))
    else {
        return Cell::Null;
    };
    if !first.valid() || !second.valid() {
        return Cell::Null;
    }
    let negative = first.ijd < second.ijd;
    if negative {
        std::mem::swap(&mut first, &mut second);
    }
    let (start_y, start_m, _) = second.output_ymd();
    let (end_y, end_m, _) = first.output_ymd();
    let hms = second.hms_ms();
    let mut total = i64::from(end_y - start_y) * 12 + i64::from(end_m - start_m);
    while total > 0 {
        let (y, m, d) = second.output_ymd();
        let (year, month) = shift_months(y, m, total as i32);
        let candidate = compute_jd(year, month, d) + hms;
        if candidate <= first.ijd {
            break;
        }
        total -= 1;
    }
    let (start_year, start_month, start_day) = second.output_ymd();
    let (year, month) = shift_months(start_year, start_month, total as i32);
    let candidate = compute_jd(year, month, start_day) + hms;
    let remainder = first.ijd - candidate;
    let days = remainder / 86_400_000;
    let milliseconds = remainder % 86_400_000;
    let hours = milliseconds / 3_600_000;
    let minutes = (milliseconds % 3_600_000) / 60_000;
    let seconds = (milliseconds % 60_000) / 1000;
    let millis = milliseconds % 1000;
    Cell::Text(format!(
        "{}{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
        if negative { '-' } else { '+' },
        total / 12,
        total % 12,
        days,
        hours,
        minutes,
        seconds,
        millis
    ))
}

/// 宿主时区转换：`utc` 把输入的本地时刻换成 UTC，`localtime` 反向。
#[cfg(any(unix, windows))]
fn to_utc(stamp: &mut Stamp) -> bool {
    let (year, month, day) = stamp.output_ymd();
    let (hour, minute, second, millisecond) = stamp.output_hms();
    match host_to_epoch(year, month, day, hour, minute, second) {
        Some(epoch) => {
            stamp.set_ijd(UNIX_EPOCH_IJD + epoch * 1000 + i64::from(millisecond));
            true
        }
        None => false,
    }
}

#[cfg(any(unix, windows))]
fn to_localtime(stamp: &mut Stamp) -> bool {
    let offset = stamp.ijd - UNIX_EPOCH_IJD;
    let seconds = offset.div_euclid(1000);
    let millisecond = offset - seconds * 1000;
    match host_from_epoch(seconds) {
        Some((year, month, day, hour, minute, second)) => {
            stamp.set_ijd(
                compute_jd(year, month, day)
                    + i64::from(hour) * 3_600_000
                    + i64::from(minute) * 60_000
                    + i64::from(second) * 1000
                    + millisecond,
            );
            true
        }
        None => false,
    }
}

/// MSVC 运行库的 `struct tm` 是 9 个 `int`。
#[cfg(windows)]
#[repr(C)]
struct CrtTm {
    tm_sec: i32,
    tm_min: i32,
    tm_hour: i32,
    tm_mday: i32,
    tm_mon: i32,
    tm_year: i32,
    tm_wday: i32,
    tm_yday: i32,
    tm_isdst: i32,
}

#[cfg(windows)]
unsafe extern "C" {
    fn _mktime64(tm: *mut CrtTm) -> i64;
    fn _localtime64_s(tm: *mut CrtTm, clock: *const i64) -> i32;
}

#[cfg(windows)]
fn host_to_epoch(year: i32, month: i32, day: i32, hour: i32, minute: i32, second: i32) -> Option<i64> {
    let mut tm = CrtTm {
        tm_sec: second,
        tm_min: minute,
        tm_hour: hour,
        tm_mday: day,
        tm_mon: month - 1,
        tm_year: year - 1900,
        tm_wday: 0,
        tm_yday: 0,
        tm_isdst: -1,
    };
    // SAFETY: `_mktime64` 只读写传入的 `struct tm`，指针来自本栈对象且布局与 MSVC 一致。
    Some(unsafe { _mktime64(&mut tm) })
}

#[cfg(windows)]
fn host_from_epoch(epoch: i64) -> Option<(i32, i32, i32, i32, i32, i32)> {
    let mut tm = CrtTm { tm_sec: 0, tm_min: 0, tm_hour: 0, tm_mday: 0, tm_mon: 0, tm_year: 0, tm_wday: 0, tm_yday: 0, tm_isdst: 0 };
    // SAFETY: `_localtime64_s` 写入本栈上的 `struct tm`，布局与 MSVC 一致，失败时返回非零。
    let status = unsafe { _localtime64_s(&mut tm, &epoch) };
    if status != 0 {
        return None;
    }
    Some((tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec))
}

/// glibc 的 `struct tm` 在前 9 个 `int` 之后还有 `tm_gmtoff` 与 `tm_zone`。
#[cfg(unix)]
#[repr(C)]
struct CrtTm {
    tm_sec: i32,
    tm_min: i32,
    tm_hour: i32,
    tm_mday: i32,
    tm_mon: i32,
    tm_year: i32,
    tm_wday: i32,
    tm_yday: i32,
    tm_isdst: i32,
    tm_gmtoff: i64,
    tm_zone: *const u8,
}

#[cfg(unix)]
unsafe extern "C" {
    fn mktime(tm: *mut CrtTm) -> i64;
    fn localtime_r(clock: *const i64, tm: *mut CrtTm) -> *mut CrtTm;
}

#[cfg(unix)]
fn host_to_epoch(year: i32, month: i32, day: i32, hour: i32, minute: i32, second: i32) -> Option<i64> {
    let mut tm = CrtTm {
        tm_sec: second,
        tm_min: minute,
        tm_hour: hour,
        tm_mday: day,
        tm_mon: month - 1,
        tm_year: year - 1900,
        tm_wday: 0,
        tm_yday: 0,
        tm_isdst: -1,
        tm_gmtoff: 0,
        tm_zone: std::ptr::null(),
    };
    // SAFETY: `mktime` 只读写传入的 `struct tm`，指针来自本栈对象且字段与 glibc 布局一致。
    Some(unsafe { mktime(&mut tm) })
}

#[cfg(unix)]
fn host_from_epoch(epoch: i64) -> Option<(i32, i32, i32, i32, i32, i32)> {
    let mut tm = CrtTm {
        tm_sec: 0,
        tm_min: 0,
        tm_hour: 0,
        tm_mday: 0,
        tm_mon: 0,
        tm_year: 0,
        tm_wday: 0,
        tm_yday: 0,
        tm_isdst: 0,
        tm_gmtoff: 0,
        tm_zone: std::ptr::null(),
    };
    // SAFETY: `localtime_r` 写入本栈上的 `struct tm`，字段与 glibc 布局一致。
    let result = unsafe { localtime_r(&epoch, &mut tm) };
    if result.is_null() {
        return None;
    }
    Some((tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec))
}
