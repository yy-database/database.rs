//! 适配器公共错误面。
//!
//! MySQL / PostgreSQL / Redis / SQLite 四个外存适配器共用同一错误类型：
//! wire 失败统一落到 `database-types` 的 [`database_types::Error`]，以免每个
//! 适配器各自定义一份同名枚举。本地连接器（`connector_local`）的错误绑定
//! 到 YYDB 引擎错误，仍留在其自身模块内。

/// 适配器错误。
#[derive(Debug)]
pub enum Error {
    /// YYDS 线协议客户端失败。
    Wire(database_types::Error),
    /// VOS schema 解析或语义问题。
    Vos(String),
    /// 策略 / adopt 阻断 / 未审阅的破坏性迁移。
    Policy(String),
    /// 线客户端 SQL 落地前不可用的能力。
    Unsupported(String),
    /// SQLite provider 失败（L3 接续内核未就绪时统一 fail-closed）。
    Sqlite(String),
    /// I/O 失败（journal 文件与目录操作）。
    Io(std::io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wire(e) => write!(f, "{e}"),
            Self::Vos(s) | Self::Policy(s) | Self::Unsupported(s) | Self::Sqlite(s) => write!(f, "{s}"),
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wire(e) => Some(e),
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// 适配器结果别名。
pub type Result<T> = std::result::Result<T, Error>;
