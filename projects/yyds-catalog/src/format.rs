//! On-disk `.yyds` catalog bytes (distinct from the embedded `.yydb` layout).

use std::io::{Cursor, Read, Write};

use yyds_types::{CatalogSchema, Error, LeaderTerm, OwnerEntry, ReplicaNodeId, Result, ShardEpoch, ShardId};

/// Catalog file magic (`YYDS` catalog plane, not `.yydb`).
pub const MAGIC: &[u8] = b"YYDS\x01";

/// Catalog on-disk format version. Fixed at 0 throughout 0.x releases; any
/// mismatching version is rejected outright. No compatibility shims.
const FORMAT_VERSION: u32 = 0;

/// Serialize catalog state to `.yyds` bytes.
pub fn encode(
    schema: Option<&CatalogSchema>,
    routing_epoch: Option<ShardEpoch>,
    shards: &[ShardId],
    resolved_contract: Option<&vos::types::ResolvedContract>,
    owners: &[OwnerEntry],
) -> Result<Vec<u8>> {
    if let Some(contract) = resolved_contract {
        crate::catalog::validate_contract(schema, contract)?;
    }
    let mut out = Vec::new();
    out.write_all(MAGIC)?;
    out.write_all(&FORMAT_VERSION.to_le_bytes())?;

    match schema {
        Some(schema) => {
            out.write_all(&1u32.to_le_bytes())?;
            out.write_all(&schema.version.to_le_bytes())?;
            let doc = schema.document.as_bytes();
            if doc.len() > u32::MAX as usize {
                return Err(Error::Unsupported("catalog schema document too large"));
            }
            out.write_all(&(doc.len() as u32).to_le_bytes())?;
            out.write_all(doc)?;
        }
        None => out.write_all(&0u32.to_le_bytes())?,
    }

    match routing_epoch {
        Some(epoch) => {
            if epoch.0 == 0 {
                return Err(Error::Unsupported("routing epoch must be nonzero"));
            }
            out.write_all(&1u32.to_le_bytes())?;
            out.write_all(&epoch.0.to_le_bytes())?;
        }
        None => out.write_all(&0u32.to_le_bytes())?,
    }

    if shards.len() > u32::MAX as usize {
        return Err(Error::Unsupported("too many shard registrations"));
    }
    out.write_all(&(shards.len() as u32).to_le_bytes())?;
    for shard in shards {
        let id = shard.0.as_bytes();
        if id.len() > u32::MAX as usize {
            return Err(Error::Unsupported("shard id too long"));
        }
        out.write_all(&(id.len() as u32).to_le_bytes())?;
        out.write_all(id)?;
    }

    match resolved_contract {
        Some(contract) => {
            let json = contract.to_json().map_err(|_| Error::Corrupt("resolved contract encode"))?;
            let bytes = json.as_bytes();
            if bytes.len() > u32::MAX as usize {
                return Err(Error::Unsupported("resolved contract too large"));
            }
            out.write_all(&1u32.to_le_bytes())?;
            out.write_all(&(bytes.len() as u32).to_le_bytes())?;
            out.write_all(bytes)?;
        }
        None => out.write_all(&0u32.to_le_bytes())?,
    }

    // Owners section.
    if owners.len() > u32::MAX as usize {
        return Err(Error::Unsupported("too many owner registrations"));
    }
    out.write_all(&(owners.len() as u32).to_le_bytes())?;
    for owner in owners {
        let shard_bytes = owner.shard_id.0.as_bytes();
        let node_bytes = owner.node_id.0.as_bytes();
        if shard_bytes.len() > u32::MAX as usize || node_bytes.len() > u32::MAX as usize {
            return Err(Error::Unsupported("owner identity too long"));
        }
        out.write_all(&(shard_bytes.len() as u32).to_le_bytes())?;
        out.write_all(shard_bytes)?;
        out.write_all(&(node_bytes.len() as u32).to_le_bytes())?;
        out.write_all(node_bytes)?;
        out.write_all(&owner.leader_term.0.to_le_bytes())?;
    }

    Ok(out)
}

/// Parse catalog bytes previously written by [`encode`].
///
/// Returns `(schema, routing_epoch, shards, resolved_contract, owners)`.
pub fn decode(
    bytes: &[u8],
) -> Result<(Option<CatalogSchema>, Option<ShardEpoch>, Vec<ShardId>, Option<vos::types::ResolvedContract>, Vec<OwnerEntry>)> {
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; MAGIC.len()];
    read_exact(&mut cursor, &mut magic)?;
    if magic.as_slice() != MAGIC {
        return Err(Error::Corrupt("unknown catalog header"));
    }

    let format_version = read_u32(&mut cursor)?;
    if format_version != FORMAT_VERSION {
        return Err(Error::Unsupported("catalog format version requires regeneration"));
    }

    let schema = match read_u32(&mut cursor)? {
        0 => None,
        1 => {
            let version = read_u32(&mut cursor)?;
            let len = read_u32(&mut cursor)? as usize;
            let mut doc = vec![0u8; len];
            read_exact(&mut cursor, &mut doc)?;
            let document = String::from_utf8(doc).map_err(|_| Error::Corrupt("catalog schema utf8"))?;
            Some(CatalogSchema { version, document })
        }
        _ => return Err(Error::Corrupt("invalid catalog schema presence flag")),
    };

    let routing_epoch = match read_u32(&mut cursor)? {
        0 => None,
        1 => {
            let epoch = read_u64(&mut cursor)?;
            if epoch == 0 {
                return Err(Error::Corrupt("invalid catalog routing epoch"));
            }
            Some(ShardEpoch(epoch))
        }
        _ => return Err(Error::Corrupt("invalid catalog routing presence flag")),
    };

    let shard_count = read_u32(&mut cursor)? as usize;
    let mut shards = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        let len = read_u32(&mut cursor)? as usize;
        let mut id = vec![0u8; len];
        read_exact(&mut cursor, &mut id)?;
        let id = String::from_utf8(id).map_err(|_| Error::Corrupt("catalog shard id utf8"))?;
        shards.push(ShardId(id));
    }

    let resolved_contract = match read_u32(&mut cursor)? {
        0 => None,
        1 => {
            let len = read_u32(&mut cursor)? as usize;
            let mut json = vec![0u8; len];
            read_exact(&mut cursor, &mut json)?;
            let json = String::from_utf8(json).map_err(|_| Error::Corrupt("resolved contract utf8"))?;
            Some(vos::types::ResolvedContract::from_json(&json).map_err(|_| Error::Corrupt("resolved contract decode"))?)
        }
        _ => return Err(Error::Corrupt("invalid resolved contract presence flag")),
    };

    // Owners section.
    let owner_count = read_u32(&mut cursor)? as usize;
    let mut owners = Vec::with_capacity(owner_count);
    for _ in 0..owner_count {
        let shard_len = read_u32(&mut cursor)? as usize;
        let mut shard_bytes = vec![0u8; shard_len];
        read_exact(&mut cursor, &mut shard_bytes)?;
        let shard_id = String::from_utf8(shard_bytes).map_err(|_| Error::Corrupt("catalog owner shard utf8"))?;

        let node_len = read_u32(&mut cursor)? as usize;
        let mut node_bytes = vec![0u8; node_len];
        read_exact(&mut cursor, &mut node_bytes)?;
        let node_id = String::from_utf8(node_bytes).map_err(|_| Error::Corrupt("catalog owner node utf8"))?;

        let term = read_u64(&mut cursor)?;
        owners.push(OwnerEntry { shard_id: ShardId(shard_id), node_id: ReplicaNodeId(node_id), leader_term: LeaderTerm(term) });
    }

    if cursor.position() != bytes.len() as u64 {
        return Err(Error::Corrupt("trailing catalog bytes"));
    }

    if let Some(contract) = &resolved_contract {
        crate::catalog::validate_contract(schema.as_ref(), contract)?;
    }

    Ok((schema, routing_epoch, shards, resolved_contract, owners))
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut buf = [0u8; 4];
    read_exact(cursor, &mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut buf = [0u8; 8];
    read_exact(cursor, &mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn read_exact(cursor: &mut Cursor<&[u8]>, buf: &mut [u8]) -> Result<()> {
    cursor.read_exact(buf).map_err(|err| {
        Error::Corrupt(match err.kind() {
            std::io::ErrorKind::UnexpectedEof => "unexpected catalog eof",
            _ => "catalog read failed",
        })
    })
}
