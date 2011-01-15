/// Supported disguise gateway protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GatewayKind {
    /// Redis wire disguise.
    Redis,
    /// MySQL wire disguise.
    Mysql,
    /// PostgreSQL wire disguise.
    Pgsql,
    /// MongoDB wire disguise.
    Mongo,
}

/// All supported gateway kinds in stable order.
pub const ALL_GATEWAY_KINDS: [GatewayKind; 4] =
    [GatewayKind::Redis, GatewayKind::Mysql, GatewayKind::Pgsql, GatewayKind::Mongo];

impl GatewayKind {
    /// Stable gateway identifier used in logs and diagnostics.
    pub const fn id(self) -> &'static str {
        match self {
            Self::Redis => "redis",
            Self::Mysql => "mysql",
            Self::Pgsql => "pgsql",
            Self::Mongo => "mongo",
        }
    }

    /// Default listen port for the disguise protocol.
    pub const fn default_port(self) -> u16 {
        match self {
            Self::Redis => 6379,
            Self::Mysql => 3306,
            Self::Pgsql => 5432,
            Self::Mongo => 27017,
        }
    }

    /// Workspace crate name implementing the gateway surface.
    pub const fn crate_name(self) -> &'static str {
        match self {
            Self::Redis => "yyds-gateway-redis",
            Self::Mysql => "yyds-gateway-mysql",
            Self::Pgsql => "yyds-gateway-pgsql",
            Self::Mongo => "yyds-gateway-mongo",
        }
    }
}
