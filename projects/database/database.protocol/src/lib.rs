//! `database-protocol` 再导出各技术线协议编解码。
//!
//! 实现代码优先直接依赖具体的 `database-protocol-*` crate；
//! 门面则把它们嵌在 `database::protocol::*` 下。

#![deny(missing_docs)]

/// 共享 YY 线协议（嵌入式与分布式共用；仅 magic / URI 头不同）。
pub mod yy {
    pub use database_protocol_yy::*;
}

/// MySQL 线协议组帧。
pub mod mysql {
    pub use database_protocol_mysql::*;
}

/// PostgreSQL 线协议组帧。
pub mod pgsql {
    pub use database_protocol_pgsql::*;
}

/// Redis RESP 线协议组帧。
pub mod redis {
    pub use database_protocol_redis::*;
}

/// MongoDB BSON / OP_MSG 线协议组帧。
pub mod mongo {
    pub use database_protocol_mongo::*;
}

/// 伪装网关共用的协议种类元数据（端口 / 标识），不含品牌 crate 名。
pub mod gateway {
    /// 支持的伪装网关协议种类。
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum GatewayKind {
        /// Redis 线协议伪装。
        Redis,
        /// MySQL 线协议伪装。
        Mysql,
        /// PostgreSQL 线协议伪装。
        Pgsql,
        /// MongoDB 线协议伪装。
        Mongo,
    }

    /// 全部支持的网关种类，顺序稳定。
    pub const ALL_GATEWAY_KINDS: [GatewayKind; 4] = [GatewayKind::Redis, GatewayKind::Mysql, GatewayKind::Pgsql, GatewayKind::Mongo];

    impl GatewayKind {
        /// 日志与诊断用的稳定标识。
        pub const fn id(self) -> &'static str {
            match self {
                Self::Redis => "redis",
                Self::Mysql => "mysql",
                Self::Pgsql => "pgsql",
                Self::Mongo => "mongo",
            }
        }

        /// 该伪装协议的默认监听端口。
        pub const fn default_port(self) -> u16 {
            match self {
                Self::Redis => 6379,
                Self::Mysql => 3306,
                Self::Pgsql => 5432,
                Self::Mongo => 27017,
            }
        }
    }

    /// 网关监听配置。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct GatewayConfig {
        /// 协议种类。
        pub kind: GatewayKind,
        /// 监听端口。
        pub port: u16,
    }

    impl GatewayConfig {
        /// 使用协议默认端口构造。
        pub const fn new(kind: GatewayKind) -> Self {
            Self { kind, port: kind.default_port() }
        }

        /// 覆盖监听端口。
        pub const fn with_port(mut self, port: u16) -> Self {
            self.port = port;
            self
        }
    }

    /// 伪装服务启动横幅。
    pub fn startup_banner(config: &GatewayConfig) -> String {
        format!("{} disguise listening on :{}", config.kind.id(), config.port)
    }
}

pub use gateway::{ALL_GATEWAY_KINDS, GatewayConfig, GatewayKind, startup_banner};
