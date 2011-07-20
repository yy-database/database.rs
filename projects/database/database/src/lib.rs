//! `database` 是数据库家族的唯一公开门面。
//!
//! 只按能力再导出，不按产品品牌分 feature，也不实现业务逻辑。
//! 对象—数据库映射与代码生成已提级到独立的 `odm` 能力域。

#![deny(missing_docs)]

/// 共享值、标识与基础合同。
#[cfg(feature = "types")]
pub mod types {
    pub use database_types::*;
}

/// 作者面 / 查询语言 / 命令输入。
#[cfg(feature = "input")]
pub mod input {
    pub use database_input::*;
}

/// 绑定、计划验证与结果描述。
#[cfg(feature = "query")]
pub mod query {
    pub use database_query::*;
}

/// 连接、游标、算子、写入与事务协调。
#[cfg(feature = "execution")]
pub mod execution {
    pub use database_execution::*;
}

/// UDF 合同、注册表与调用运行时。
#[cfg(feature = "udf")]
pub mod udf {
    pub use database_udf::*;
}

/// 磁盘字节布局。
#[cfg(feature = "formats")]
pub mod formats {
    pub use database_formats::*;
}

/// 读写、锁、快照、恢复与对象存储。
#[cfg(feature = "storage")]
pub mod storage {
    pub use database_storage::*;
}

/// 分片、路由、复制与分布式编排。
#[cfg(feature = "cluster")]
pub mod cluster {
    pub use database_cluster::*;
}

/// 线协议编解码与握手（按技术协议分化，不按产品品牌）。
#[cfg(feature = "protocol")]
pub mod protocol {
    pub use database_protocol::*;

    /// 共享 YY 线协议（嵌入式与分布式同一套；仅 magic / URI 头不同）。
    #[cfg(feature = "protocol-yy")]
    pub mod yy {
        pub use database_protocol_yy::*;
    }

    /// MySQL 线协议。
    #[cfg(feature = "protocol-mysql")]
    pub mod mysql {
        pub use database_protocol_mysql::*;
    }

    /// PostgreSQL 线协议。
    #[cfg(feature = "protocol-pgsql")]
    pub mod pgsql {
        pub use database_protocol_pgsql::*;
    }

    /// Redis RESP 线协议。
    #[cfg(feature = "protocol-redis")]
    pub mod redis {
        pub use database_protocol_redis::*;
    }

    /// MongoDB BSON / OP_MSG 线协议。
    #[cfg(feature = "protocol-mongo")]
    pub mod mongo {
        pub use database_protocol_mongo::*;
    }
}

/// 远端访问客户端。
#[cfg(feature = "client")]
pub mod client {
    pub use database_client::*;
}

/// 服务会话与请求分派。
#[cfg(feature = "server")]
pub mod server {
    pub use database_server::*;
}
