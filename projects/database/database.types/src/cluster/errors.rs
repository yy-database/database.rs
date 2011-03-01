use std::{error, fmt, io, result};

use crate::cluster::fencing::{FenceToken, OwnerEntry};

/// 分布式 API 通用结果别名。
pub type Result<T> = result::Result<T, Error>;

/// 目录、分片与分布式路径产生的错误。
#[derive(Debug)]
pub enum Error {
    /// 底层文件系统或 I/O 失败。
    Io(io::Error),
    /// 分片或目录字节与期望布局不符。
    Corrupt(&'static str),
    /// 分片中不存在请求的键。
    NotFound {
        /// 缺失的键。
        key: String,
    },
    /// 已存目录 schema 版本与调用方期望不一致。
    SchemaConflict {
        /// 调用方期望的版本。
        expected: u32,
        /// 实际读到的版本。
        found: u32,
    },
    /// 路由世代未新于已发布目录世代。
    RoutingConflict {
        /// 要求严格新于的世代。
        expected: u64,
        /// 实际见到的世代。
        found: u64,
    },
    /// Schema 文档未通过 VOS 校验。
    Schema {
        /// 校验失败说明。
        message: String,
    },
    /// 产品面已声明但尚未实现的能力。
    Unsupported(&'static str),
    /// 存入或传入的十进制整数非法，或超出有符号 64 位。
    InvalidIntegerValue,
    /// 检查整数运算超出有符号 64 位。
    IntegerOverflow,
    /// 独立 compare-and-set 失败。
    CasConflict {
        /// 发生冲突的键。
        key: String,
    },
    /// VOS 编写的 UDF 无法落入分布式执行模型。
    Udf {
        /// UDF 名称。
        name: String,
        /// 失败说明。
        message: String,
    },
    /// 入站 fence 与目录为该分片发布的 owner 不一致。
    FencingRejected {
        /// 被拒绝的 fence。
        fence: FenceToken,
        /// 目录当前发布的 owner。
        current: OwnerEntry,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Corrupt(reason) => write!(f, "corrupt cluster data: {reason}"),
            Self::NotFound { key } => write!(f, "key not found: {key}"),
            Self::SchemaConflict { expected, found } => {
                write!(f, "catalog schema version conflict: expected {expected}, found {found}")
            }
            Self::RoutingConflict { expected, found } => {
                write!(f, "routing epoch conflict: expected newer than {expected}, found {found}")
            }
            Self::Schema { message } => write!(f, "VOS schema: {message}"),
            Self::Unsupported(feature) => write!(f, "unsupported: {feature}"),
            Self::InvalidIntegerValue => write!(f, "value is not an integer or out of range"),
            Self::IntegerOverflow => write!(f, "increment or decrement would overflow"),
            Self::CasConflict { key } => write!(f, "cas conflict on key: {key}"),
            Self::Udf { name, message } => write!(f, "UDF {name}: {message}"),
            Self::FencingRejected { fence, current } => {
                write!(
                    f,
                    "fencing rejected: fence token (epoch={} term={} leader={}) does not match catalog owner {}",
                    fence.epoch.0, fence.term.0, fence.leader.0, current
                )
            }
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
