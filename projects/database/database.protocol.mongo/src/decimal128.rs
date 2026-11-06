//! BSON Decimal128 (0x13) 的字节与十进制文本转换。
//!
//! 采用 IEEE 754-2008 decimal128 布局：符号位、14 位指数（偏置 6176）与 113
//! 位系数。转换只做表示层处理，不做舍入：有效数字超过 34 位或指数越界的输入
//! 直接拒绝，避免悄悄改变值。
//!
//! 文本格式与 MongoDB 的 `Decimal128.toString()` 一致：指数为正、科学计数
//! 指数不小于 34 或不大于 -7 时使用 `1.5E+3` 形式，其余使用普通小数形式。
use std::cmp::Ordering;

/// 指数偏置。
const EXPONENT_BIAS: i32 = 6176;
/// 指数上界。
const EXPONENT_MAX: i32 = 6111;
/// 指数下界。
const EXPONENT_MIN: i32 = -6176;
/// 最大有效数字位数。
const MAX_DIGITS: usize = 34;
/// 组合字段的无穷值。
const COMBINATION_INFINITY: u32 = 0x1e;
/// 组合字段的 NaN 值。
const COMBINATION_NAN: u32 = 0x1f;
/// 符号位。
const SIGN_MASK: u32 = 0x8000_0000;
/// 低 96 位掩码。
const LOW_MASK: u128 = (1_u128 << 96) - 1;

/// 一份 decimal128 值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal128 {
    bytes: [u8; 16],
}

/// 有限值的符号、系数与指数。
#[derive(Debug, Clone, Copy)]
struct Finite {
    negative: bool,
    coefficient: u128,
    exponent: i32,
}

/// 解码后的三种形态。
#[derive(Debug, Clone, Copy)]
enum Classification {
    /// NaN（符号无意义）。
    Nan,
    /// 正负无穷。
    Infinite(bool),
    /// 有限值。
    Finite(Finite),
}

impl Decimal128 {
    /// 从线上 16 字节构造。
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self { bytes }
    }

    /// 线上 16 字节。
    pub fn bytes(&self) -> [u8; 16] {
        self.bytes
    }

    /// 按 [`Decimal128::as_str`] 的反向解析；超出 34 位有效数字或指数范围时返回
    /// `None`，不做舍入。
    pub fn from_str(text: &str) -> Option<Self> {
        let (negative, rest) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text.strip_prefix('+').unwrap_or(text)),
        };
        let lowered = rest.to_ascii_lowercase();
        if lowered == "infinity" || lowered == "inf" {
            let mut bytes = [0_u8; 16];
            bytes[15] = if negative { 0xf8 } else { 0x78 };
            return Some(Self { bytes });
        }
        if lowered == "nan" {
            let mut bytes = [0_u8; 16];
            bytes[15] = 0x7c;
            return Some(Self { bytes });
        }
        let (mantissa, exponent_text) = match rest.find(['e', 'E']) {
            Some(index) => (&rest[..index], Some(&rest[index + 1..])),
            None => (rest, None),
        };
        let exponent = match exponent_text {
            Some(text) => text.trim_start_matches('+').parse::<i32>().ok()?,
            None => 0,
        };
        let mut digits = String::new();
        let mut fraction_digits = 0_i32;
        let mut seen_radix = false;
        for character in mantissa.chars() {
            match character {
                '0'..='9' => {
                    digits.push(character);
                    if seen_radix {
                        fraction_digits += 1;
                    }
                }
                '.' if !seen_radix => seen_radix = true,
                _ => return None,
            }
        }
        if digits.is_empty() {
            return None;
        }
        let significant = digits.trim_start_matches('0');
        if significant.len() > MAX_DIGITS {
            return None;
        }
        let coefficient = if significant.is_empty() { 0 } else { significant.parse::<u128>().ok()? };
        let exponent = exponent.checked_sub(fraction_digits)?;
        if !(EXPONENT_MIN..=EXPONENT_MAX).contains(&exponent) {
            return None;
        }
        let biased = (exponent + EXPONENT_BIAS) as u32;
        let msb = ((coefficient >> 110) & 0x7) as u32;
        let high = (if negative { SIGN_MASK } else { 0 }) | (biased << 17) | (msb << 14) | ((coefficient >> 96) & 0x3fff) as u32;
        let bits = ((high as u128) << 96) | (coefficient & LOW_MASK);
        Some(Self { bytes: bits.to_le_bytes() })
    }

    /// 与 MongoDB `Decimal128.toString()` 相同的文本表示。
    pub fn as_str(&self) -> String {
        let bits = u128::from_le_bytes(self.bytes);
        let high = (bits >> 96) as u32;
        let negative = high & SIGN_MASK != 0;
        let combination = (high >> 26) & 0x1f;
        let (coefficient, exponent) = if combination >> 3 == 3 {
            match combination {
                COMBINATION_INFINITY => {
                    return if negative { "-Infinity".to_string() } else { "Infinity".to_string() };
                }
                COMBINATION_NAN => return "NaN".to_string(),
                _ => {
                    let biased = (high >> 15) & 0x3fff;
                    let msb = 8 | ((high >> 14) & 1);
                    (coefficient_from(bits, high, msb), biased as i32 - EXPONENT_BIAS)
                }
            }
        }
        else {
            let biased = (high >> 17) & 0x3fff;
            let msb = (high >> 14) & 0x7;
            (coefficient_from(bits, high, msb), biased as i32 - EXPONENT_BIAS)
        };
        let digits = coefficient.to_string();
        let length = digits.len();
        let scientific = length as i32 - 1 + exponent;
        let mut out = String::new();
        if negative {
            out.push('-');
        }
        if length > MAX_DIGITS {
            out.push('0');
            out.push('E');
            if exponent > 0 {
                out.push('+');
            }
            out.push_str(&exponent.to_string());
        }
        else if scientific >= 34 || scientific <= -7 || exponent > 0 {
            out.push_str(&digits[..1]);
            if length > 1 {
                out.push('.');
                out.push_str(&digits[1..]);
            }
            out.push('E');
            if scientific > 0 {
                out.push('+');
            }
            out.push_str(&scientific.to_string());
        }
        else if exponent >= 0 {
            out.push_str(&digits);
            for _ in 0..exponent {
                out.push('0');
            }
        }
        else if scientific >= 0 {
            let point = (length as i32 + exponent) as usize;
            out.push_str(&digits[..point]);
            out.push('.');
            out.push_str(&digits[point..]);
        }
        else {
            out.push_str("0.");
            for _ in 0..(-scientific - 1) {
                out.push('0');
            }
            out.push_str(&digits);
        }
        out
    }

    /// 近似 `f64` 值，供跨数值类型的比较与过滤使用。
    pub fn as_f64(&self) -> f64 {
        match self.classify() {
            Classification::Nan => f64::NAN,
            Classification::Infinite(negative) => {
                if negative {
                    f64::NEG_INFINITY
                }
                else {
                    f64::INFINITY
                }
            }
            Classification::Finite(_) => self.as_str().parse::<f64>().unwrap_or(f64::NAN),
        }
    }

    /// 能精确表示成 `i64` 时返回该整数，供 `_id` 与数值过滤共用键空间。
    pub fn as_i64(&self) -> Option<i64> {
        let Classification::Finite(finite) = self.classify()
        else {
            return None;
        };
        let magnitude = if finite.exponent >= 0 {
            finite.coefficient.checked_mul(10_u128.checked_pow(finite.exponent as u32)?)?
        }
        else {
            let scale = 10_u128.checked_pow((-finite.exponent) as u32)?;
            finite.coefficient.checked_div(scale).filter(|_| finite.coefficient % scale == 0)?
        };
        if finite.negative {
            if magnitude == 0 {
                return Some(0);
            }
            i64::try_from(magnitude).ok().map(|value| -value).or_else(|| (magnitude == 1_u128 << 63).then_some(i64::MIN))
        }
        else {
            i64::try_from(magnitude).ok()
        }
    }

    /// 与另一份 decimal128 的数值比较；NaN 小于一切。
    pub fn compare(&self, other: &Decimal128) -> Ordering {
        let left = self.classify();
        let right = other.classify();
        match (left, right) {
            (Classification::Nan, Classification::Nan) => Ordering::Equal,
            (Classification::Nan, _) => Ordering::Less,
            (_, Classification::Nan) => Ordering::Greater,
            (Classification::Infinite(left), Classification::Infinite(right)) => match (left, right) {
                (true, true) | (false, false) => Ordering::Equal,
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
            },
            (Classification::Infinite(left), Classification::Finite(_)) => {
                if left {
                    Ordering::Less
                }
                else {
                    Ordering::Greater
                }
            }
            (Classification::Finite(_), Classification::Infinite(right)) => {
                if right {
                    Ordering::Greater
                }
                else {
                    Ordering::Less
                }
            }
            (Classification::Finite(left), Classification::Finite(right)) => {
                if left.negative != right.negative {
                    return if left.negative { Ordering::Less } else { Ordering::Greater };
                }
                let ordering = compare_magnitude(left, right);
                if left.negative { ordering.reverse() } else { ordering }
            }
        }
    }

    /// 归一化后的数值键：符号、指数与去掉末尾零的系数。
    ///
    /// `1.5` 与 `1.50`、`0.0` 与 `-0.00` 必须落到同一个键，才能与 MongoDB 的
    /// 数值相等语义一致。
    pub fn key_parts(&self) -> (u8, i32, u128) {
        match self.classify() {
            Classification::Nan => (0, 0, 0),
            Classification::Infinite(negative) => (if negative { 1 } else { 5 }, 0, 0),
            Classification::Finite(finite) => {
                if finite.coefficient == 0 {
                    return (2, 0, 0);
                }
                let mut coefficient = finite.coefficient;
                let mut exponent = finite.exponent;
                while coefficient % 10 == 0 {
                    coefficient /= 10;
                    exponent += 1;
                }
                let sign = if finite.negative { 1 } else { 5 };
                (sign, exponent, coefficient)
            }
        }
    }

    /// 解析符号位、系数与指数。
    fn classify(&self) -> Classification {
        let bits = u128::from_le_bytes(self.bytes);
        let high = (bits >> 96) as u32;
        let negative = high & SIGN_MASK != 0;
        let combination = (high >> 26) & 0x1f;
        if combination >> 3 == 3 {
            return match combination {
                COMBINATION_INFINITY => Classification::Infinite(negative),
                COMBINATION_NAN => Classification::Nan,
                _ => {
                    let biased = (high >> 15) & 0x3fff;
                    let msb = 8 | ((high >> 14) & 1);
                    Classification::Finite(Finite {
                        negative,
                        coefficient: coefficient_from(bits, high, msb),
                        exponent: biased as i32 - EXPONENT_BIAS,
                    })
                }
            };
        }
        let biased = (high >> 17) & 0x3fff;
        let msb = (high >> 14) & 0x7;
        Classification::Finite(Finite { negative, coefficient: coefficient_from(bits, high, msb), exponent: biased as i32 - EXPONENT_BIAS })
    }
}

/// 从 128 位线上字节还原 113 位系数。
fn coefficient_from(bits: u128, high: u32, msb: u32) -> u128 {
    let part = ((high & 0x3fff) | (msb << 14)) as u128;
    (part << 96) | (bits & LOW_MASK)
}

/// 比较两个同号有限值的绝对值。
fn compare_magnitude(left: Finite, right: Finite) -> Ordering {
    if left.coefficient == 0 || right.coefficient == 0 {
        return left.coefficient.cmp(&right.coefficient);
    }
    let left_digits = left.coefficient.to_string().len() as i64;
    let right_digits = right.coefficient.to_string().len() as i64;
    let left_magnitude = left_digits + left.exponent as i64;
    let right_magnitude = right_digits + right.exponent as i64;
    if left_magnitude != right_magnitude {
        return left_magnitude.cmp(&right_magnitude);
    }
    let target = left_digits.max(right_digits) as u32;
    let left_scaled = 10_u128.pow(target - left_digits as u32).checked_mul(left.coefficient);
    let right_scaled = 10_u128.pow(target - right_digits as u32).checked_mul(right.coefficient);
    match (left_scaled, right_scaled) {
        (Some(left), Some(right)) => left.cmp(&right),
        // 超出 u128 的编码不在合法 decimal128 范围内，退回浮点比较保持确定性。
        _ => left.coefficient.cmp(&right.coefficient),
    }
}
