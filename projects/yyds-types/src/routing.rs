//! YYDS-owned shard routing contracts.

use crate::ShardId;

/// Monotonic routing-map generation used to reject stale writers and readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardEpoch(pub u64);

/// A deterministic immutable map from logical keys to shard identities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMap {
    epoch: ShardEpoch,
    shards: Vec<ShardId>,
}

/// The result of routing one key under a specific map epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDecision {
    /// Routing-map generation used for the decision.
    pub epoch: ShardEpoch,
    /// Stable hash used to select the shard.
    pub hash: u64,
    /// Selected shard identity.
    pub shard: ShardId,
}

/// Failure to construct or use a shard routing map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingError {
    /// The routing epoch must be nonzero.
    InvalidEpoch,
    /// At least one shard is required.
    EmptyMap,
    /// A shard identity is empty or repeated.
    InvalidShardIdentity,
    /// The caller used a map epoch older than the published one.
    StaleEpoch { expected: ShardEpoch, found: ShardEpoch },
}

impl ShardMap {
    /// Creates an immutable map for one published routing epoch.
    pub fn new(epoch: ShardEpoch, shards: Vec<ShardId>) -> Result<Self, RoutingError> {
        if epoch.0 == 0 {
            return Err(RoutingError::InvalidEpoch);
        }
        if shards.is_empty() {
            return Err(RoutingError::EmptyMap);
        }
        let mut seen = std::collections::BTreeSet::new();
        for shard in &shards {
            if shard.0.is_empty() || !seen.insert(shard.0.as_str()) {
                return Err(RoutingError::InvalidShardIdentity);
            }
        }
        Ok(Self { epoch, shards })
    }

    /// Returns the published routing epoch.
    pub fn epoch(&self) -> ShardEpoch {
        self.epoch
    }

    /// Returns shards in their stable map order.
    pub fn shards(&self) -> &[ShardId] {
        &self.shards
    }

    /// Routes a tenant-scoped key using the current map epoch.
    pub fn route(&self, namespace: &str, key: &[u8]) -> RouteDecision {
        let hash = stable_key_hash(namespace, key);
        let index = (hash % self.shards.len() as u64) as usize;
        RouteDecision { epoch: self.epoch, hash, shard: self.shards[index].clone() }
    }

    /// Routes a key only when the caller has observed this exact map epoch.
    pub fn route_at_epoch(
        &self,
        expected_epoch: ShardEpoch,
        namespace: &str,
        key: &[u8],
    ) -> Result<RouteDecision, RoutingError> {
        if expected_epoch != self.epoch {
            return Err(RoutingError::StaleEpoch { expected: self.epoch, found: expected_epoch });
        }
        Ok(self.route(namespace, key))
    }
}

fn stable_key_hash(namespace: &str, key: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in (namespace.len() as u64)
        .to_le_bytes()
        .iter()
        .chain(namespace.as_bytes())
        .chain((key.len() as u64).to_le_bytes().iter())
        .chain(key)
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3_u64);
    }
    hash
}
