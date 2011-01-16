//! `.yyds` catalog bytes — thin validation wrapper over `database-formats`.

use database_formats::cluster::catalog_format;
use database_types::cluster::{CatalogSchema, OwnerEntry, Result, ShardEpoch, ShardId};

pub use database_formats::cluster::CATALOG_MAGIC as MAGIC;

/// Serialize catalog state to `.yyds` bytes after contract validation.
pub fn encode(
    schema: Option<&CatalogSchema>,
    routing_epoch: Option<ShardEpoch>,
    shards: &[ShardId],
    resolved_contract: Option<&vos::types::ResolvedContract>,
    owners: &[OwnerEntry],
) -> Result<Vec<u8>> {
    if let Some(contract) = resolved_contract {
        crate::catalog::catalog::validate_contract(schema, contract)?;
    }
    catalog_format::encode(schema, routing_epoch, shards, resolved_contract, owners)
}

/// Parse catalog bytes and validate any resolved contract.
pub fn decode(
    bytes: &[u8],
) -> Result<(Option<CatalogSchema>, Option<ShardEpoch>, Vec<ShardId>, Option<vos::types::ResolvedContract>, Vec<OwnerEntry>)> {
    let (schema, routing_epoch, shards, resolved_contract, owners) = catalog_format::decode(bytes)?;
    if let Some(contract) = &resolved_contract {
        crate::catalog::catalog::validate_contract(schema.as_ref(), contract)?;
    }
    Ok((schema, routing_epoch, shards, resolved_contract, owners))
}
