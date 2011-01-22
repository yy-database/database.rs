//! Iris 的有限 Redis 键空间 / 文档 / 缓存适配器。
//!
//! 经 `yyds-gateway-redis::client` 说 Redis 线协议（与 YYDS Redis 伪装服务同帧）。
//! 不依赖第三方 `redis` crate。属 L1 服务模拟。

#![deny(missing_docs)]

mod cache;
mod mapping;
mod ops;

use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Mutex,
    time::Duration,
};

use database_client::redis::Client;
use database_query::ir::{CommitToken, ConsistencyIntent, DEFAULT_COMMIT_SHARD, IrVersion, OutboxRecord, PhysicalOp, PhysicalPlan};
use odm_types::{AppliedWatermarkState, CacheWatermarkProbe, CapabilitySet, CompensationBudget, QueryCaps, WriteCaps};

pub use cache::{CacheEntry, IdentityCacheResult};
pub use mapping::{KeyEncoding, KeyspaceMapping, MappingManifest};

/// Adapter identifier.
pub const BACKEND_ID: &str = "redis";

/// Adapter crate version label.
pub const ADAPTER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Redis foreign datasource (keyspace adapter).
pub struct RedisSource {
    address: SocketAddr,
    timeout: Duration,
    /// One shared wire session; Redis commands are serialized through this mutex.
    client: Mutex<Client>,
    mapping: MappingManifest,
}

impl std::fmt::Debug for RedisSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisSource")
            .field("backend", &BACKEND_ID)
            .field("address", &self.address)
            .field("tables", &self.mapping.tables.len())
            .finish_non_exhaustive()
    }
}

impl RedisSource {
    /// Limited capabilities: PK get/put/delete only — no relational filter/sort.
    pub fn capabilities() -> CapabilitySet {
        CapabilitySet {
            backend_id: BACKEND_ID.into(),
            backend_version: ADAPTER_VERSION.into(),
            ir_version_max: IrVersion::PHASE1,
            query: QueryCaps::scan_only(),
            write: WriteCaps::full(),
            budget: CompensationBudget { max_rows: 1, max_round_trips: 8, max_assoc_fanout: 1, ..CompensationBudget::default() },
        }
    }

    /// Connect with an explicit mapping manifest (required — no auto catalog invent).
    pub fn connect(url: &str, mapping: MappingManifest) -> Result<Self> {
        if mapping.tables.is_empty() {
            return Err(Error::Policy("Redis adapter requires at least one explicit keyspace mapping".into()));
        }
        let (address, timeout) = parse_redis_url(url)?;
        let client = Client::connect(address, timeout).map_err(Error::Wire)?;
        Ok(Self { address, timeout, client: Mutex::new(client), mapping })
    }

    /// Ping a Redis URL without inventing a catalog (connectivity only).
    pub fn ping_url(url: &str) -> Result<()> {
        let (address, timeout) = parse_redis_url(url)?;
        let mut client = Client::connect(address, timeout).map_err(Error::Wire)?;
        client.ping().map_err(Error::Wire)
    }

    /// Draft a reviewable keyspace mapping from a VOS schema (does not SCAN Redis).
    pub fn draft_keyspace_mapping(vos_schema: &str) -> Result<MappingManifest> {
        let document = odm_types::parse_schema(vos_schema).map_err(Error::Policy)?;
        let mut tables = Vec::new();
        for table in document.types.iter().filter(|item| item.kind == vos::TypeContractKind::Table) {
            let Some(name) = table.canonical_path.last()
            else {
                continue;
            };
            let pk = table
                .fields
                .iter()
                .find(|f| f.attributes.iter().any(|a| a.name == "primary"))
                .ok_or_else(|| Error::Policy(format!("table `{name}` has no primary key — Redis keyspace mapping requires one")))?;
            tables.push(KeyspaceMapping {
                vos_table: name.clone(),
                key_prefix: format!("iris:{}:", name.to_ascii_lowercase()),
                primary_key_field: pk.canonical_name.clone(),
                encoding: KeyEncoding::JsonDocument,
                ttl_secs: None,
            });
        }
        if tables.is_empty() {
            return Err(Error::Policy("VOS schema has no tables to map into Redis keyspace".into()));
        }
        Ok(MappingManifest::with_tables(tables))
    }

    /// Borrow the mapping manifest.
    pub fn mapping(&self) -> &MappingManifest {
        &self.mapping
    }

    fn with_client<R>(&self, f: impl FnOnce(&mut Client) -> Result<R>) -> Result<R> {
        let mut guard = self.client.lock().map_err(|_| Error::Policy("redis session mutex poisoned".into()))?;
        f(&mut guard)
    }

    /// GET by primary key under the mapped prefix.
    pub fn get_primary(&self, table: &str, primary_key: &str) -> Result<Option<String>> {
        let map = self.table(table)?.clone();
        self.with_client(|conn| ops::get_primary(conn, &map, primary_key))
    }

    /// SET (optionally with TTL) by primary key.
    pub fn put_primary(&self, table: &str, primary_key: &str, value: &str) -> Result<()> {
        let map = self.table(table)?.clone();
        self.with_client(|conn| ops::put_primary(conn, &map, primary_key, value))
    }

    /// SET NX — atomic create-if-absent.
    pub fn put_primary_nx(&self, table: &str, primary_key: &str, value: &str) -> Result<bool> {
        let map = self.table(table)?.clone();
        self.with_client(|conn| ops::put_primary_nx(conn, &map, primary_key, value))
    }

    /// DELETE by primary key.
    pub fn delete_primary(&self, table: &str, primary_key: &str) -> Result<bool> {
        let map = self.table(table)?.clone();
        self.with_client(|conn| ops::delete_primary(conn, &map, primary_key))
    }

    /// TTL remaining in seconds (`-1` if no expiry, `-2` if missing).
    pub fn ttl_primary(&self, table: &str, primary_key: &str) -> Result<i64> {
        let map = self.table(table)?.clone();
        self.with_client(|conn| ops::ttl_primary(conn, &map, primary_key))
    }

    /// Read the cache [`AppliedWatermarkState`] for a shard (default shard if `None`).
    pub fn cache_watermark(&self, shard: Option<&str>) -> Result<Option<AppliedWatermarkState>> {
        let shard = shard.unwrap_or(DEFAULT_COMMIT_SHARD);
        self.with_client(|conn| cache::get_watermark(conn, shard))
    }

    /// Probe a Cache watermark without a keyspace mapping.
    pub fn probe_watermark_url(url: &str, shard: Option<&str>) -> Result<Option<AppliedWatermarkState>> {
        let (address, timeout) = parse_redis_url(url)?;
        let mut client = Client::connect(address, timeout).map_err(Error::Wire)?;
        cache::get_watermark(&mut client, shard.unwrap_or(DEFAULT_COMMIT_SHARD))
    }

    /// Overwrite the cache watermark (tests / rebuild).
    pub fn cache_set_watermark(&self, state: &AppliedWatermarkState) -> Result<()> {
        self.with_client(|conn| cache::set_watermark(conn, state))
    }

    /// Monotonically advance the cache watermark to at least `token.seq`.
    pub fn cache_advance_watermark(&self, token: &CommitToken, applied_unix_ms: u64) -> Result<AppliedWatermarkState> {
        self.with_client(|conn| cache::advance_watermark(conn, token, applied_unix_ms))
    }

    /// Get a versioned cache entry.
    pub fn cache_get_entry(&self, table: &str, primary_key: &str) -> Result<Option<CacheEntry>> {
        let map = self.table(table)?.clone();
        self.with_client(|conn| cache::get_entry(conn, &map.key_prefix, primary_key))
    }

    /// Put a versioned cache entry.
    pub fn cache_put_entry(&self, table: &str, primary_key: &str, entry: &CacheEntry) -> Result<()> {
        let map = self.table(table)?.clone();
        self.with_client(|conn| cache::put_entry(conn, &map.key_prefix, primary_key, entry, map.ttl_secs))
    }

    /// Invalidate a cache entry.
    pub fn cache_invalidate(&self, table: &str, primary_key: &str) -> Result<bool> {
        let map = self.table(table)?.clone();
        self.with_client(|conn| cache::invalidate_entry(conn, &map.key_prefix, primary_key))
    }

    /// Apply one authority outbox record into the cache.
    pub fn cache_apply_outbox(&self, record: &OutboxRecord, payload: Option<&str>, now_unix_ms: u64) -> Result<AppliedWatermarkState> {
        let map = self.table(&record.table)?.clone();
        self.with_client(|conn| cache::apply_outbox(conn, &map.key_prefix, map.ttl_secs, record, payload, now_unix_ms))
    }

    /// Identity read via cache under a consistency intent.
    pub fn cache_identity_read(
        &self,
        table: &str,
        primary_key: &str,
        intent: &ConsistencyIntent,
        session_fence: Option<&CommitToken>,
        now_unix_ms: u64,
        shard: Option<&str>,
    ) -> Result<IdentityCacheResult> {
        let map = self.table(table)?.clone();
        self.with_client(|conn| {
            cache::identity_cache_read(
                conn,
                &map.key_prefix,
                primary_key,
                intent,
                session_fence,
                now_unix_ms,
                shard.unwrap_or(DEFAULT_COMMIT_SHARD),
            )
        })
    }

    /// Fill cache from an authority-sourced value after miss/bypass.
    pub fn cache_fill_from_authority(
        &self,
        table: &str,
        primary_key: &str,
        entry: &CacheEntry,
        token: &CommitToken,
        now_unix_ms: u64,
    ) -> Result<AppliedWatermarkState> {
        let map = self.table(table)?.clone();
        self.with_client(|conn| cache::fill_from_authority(conn, &map.key_prefix, primary_key, entry, map.ttl_secs, token, now_unix_ms))
    }

    /// Reject relational physical plans that would require keyspace scans.
    pub fn execute_plan(&self, plan: &PhysicalPlan) -> Result<Vec<odm_types::Row>> {
        if plan.is_rejected() {
            return Err(Error::Unsupported(plan.rejection_note().unwrap_or("plan rejected").to_string()));
        }
        for node in &plan.nodes {
            match &node.op {
                PhysicalOp::Filter { .. }
                | PhysicalOp::Sort { .. }
                | PhysicalOp::Skip { .. }
                | PhysicalOp::Take { .. }
                | PhysicalOp::Project { .. } => {
                    return Err(Error::Unsupported(
                        "Redis adapter rejects filter/sort/page/project over the keyspace; \
                         use explicit primary-key get/put/delete"
                            .into(),
                    ));
                }
                PhysicalOp::Scan { .. } => {
                    return Err(Error::Unsupported("Redis adapter rejects full-table Scan; use get_primary with an explicit key".into()));
                }
                PhysicalOp::Insert { .. } | PhysicalOp::Patch { .. } | PhysicalOp::Delete { .. } => {
                    return Err(Error::Unsupported("Redis adapter rejects relational write physical plans; use explicit put/delete".into()));
                }
                PhysicalOp::Collect => {}
            }
        }
        Err(Error::Unsupported("Redis adapter does not execute relational physical plans".into()))
    }

    fn table<'a>(&'a self, name: &str) -> Result<&'a KeyspaceMapping> {
        self.mapping
            .tables
            .iter()
            .find(|t| t.vos_table == name)
            .ok_or_else(|| Error::Policy(format!("no keyspace mapping for VOS table `{name}` — Redis will not invent one")))
    }
}

/// 解析明文 Redis URL（拒绝 TLS 与 URL 内凭据）。
pub fn parse_redis_url(url: &str) -> Result<(SocketAddr, Duration)> {
    let timeout = Duration::from_secs(5);
    if url.starts_with("rediss://") {
        return Err(Error::Policy(
            "redis adapter does not implement TLS — `rediss://` is refused rather than silently connecting in plaintext; use `redis://` on a trusted network".into(),
        ));
    }
    let rest = url.strip_prefix("redis://").unwrap_or(url);
    let hostport = rest.split('/').next().unwrap_or(rest);
    if hostport.contains('@') {
        return Err(Error::Policy(
            "redis adapter does not support URL credentials — `AUTH` is not implemented; remove user:password from the URL".into(),
        ));
    }
    let defaulted = if hostport.contains(':') { hostport.to_owned() } else { format!("{hostport}:6379") };
    let address = defaulted
        .to_socket_addrs()
        .map_err(|e| Error::Wire(yyds_types::Error::Io(e)))?
        .next()
        .ok_or_else(|| Error::Policy(format!("redis URL host did not resolve: {url}")))?;
    let _ = timeout;
    Ok((address, timeout))
}

/// Adapter errors.
#[derive(Debug)]
pub enum Error {
    /// YYDS Redis wire client failure.
    Wire(yyds_types::Error),
    /// Capability / mapping policy.
    Policy(String),
    /// Operation not supported on Redis keyspace adapter.
    Unsupported(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wire(e) => write!(f, "{e}"),
            Self::Policy(s) | Self::Unsupported(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wire(e) => Some(e),
            _ => None,
        }
    }
}

/// Result alias.
pub type Result<T> = std::result::Result<T, Error>;

impl CacheWatermarkProbe for RedisSource {
    fn probe_watermark(&self, shard: Option<&str>) -> std::result::Result<Option<AppliedWatermarkState>, String> {
        self.cache_watermark(shard).map_err(|e| e.to_string())
    }
}

/// URL-only Cache watermark probe (no keyspace mapping required).
pub struct RedisWatermarkProbe {
    url: String,
}

impl RedisWatermarkProbe {
    /// Create a probe bound to a Redis URL (env-expanded by the caller).
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

impl CacheWatermarkProbe for RedisWatermarkProbe {
    fn probe_watermark(&self, shard: Option<&str>) -> std::result::Result<Option<AppliedWatermarkState>, String> {
        RedisSource::probe_watermark_url(&self.url, shard).map_err(|e| e.to_string())
    }
}
