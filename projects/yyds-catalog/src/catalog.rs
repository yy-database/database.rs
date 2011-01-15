use std::path::{Path, PathBuf};

use yyds_types::{
    adopt_catalog_schema, CatalogSchema, Error, LeaderTerm, OwnerEntry, ReplicaNodeId, Result, ShardEpoch, ShardId, ShardMap,
    CATALOG_SUFFIX,
};

use crate::format;

/// In-memory or file-backed `.yyds` catalog state.
#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    path: Option<PathBuf>,
    schema: Option<CatalogSchema>,
    resolved_contract: Option<vos::types::ResolvedContract>,
    shards: Vec<ShardId>,
    routing_epoch: Option<ShardEpoch>,
    owners: Vec<OwnerEntry>,
}

impl Catalog {
    /// Creates an empty in-memory catalog.
    pub fn open_memory() -> Self {
        Self { path: None, schema: None, resolved_contract: None, shards: Vec::new(), routing_epoch: None, owners: Vec::new() }
    }

    /// Opens an existing catalog file or starts an empty catalog at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            let bytes = std::fs::read(&path)?;
            let (schema, routing_epoch, shards, mut resolved_contract, owners) = format::decode(&bytes)?;
            if resolved_contract.is_none() {
                if let Some(schema) = schema.as_ref() {
                    resolved_contract = Some(resolved_contract_for_schema(schema)?);
                }
            }
            Ok(Self { path: Some(path), schema, resolved_contract, shards, routing_epoch, owners })
        }
        else {
            Ok(Self {
                path: Some(path),
                schema: None,
                resolved_contract: None,
                shards: Vec::new(),
                routing_epoch: None,
                owners: Vec::new(),
            })
        }
    }

    /// Returns the backing path when this catalog is file-backed.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Current catalog schema, if any.
    pub fn schema(&self) -> Option<&CatalogSchema> {
        self.schema.as_ref()
    }

    /// Returns the strict resolved VOS contract published for this catalog.
    pub fn resolved_contract(&self) -> Option<&vos::types::ResolvedContract> {
        self.resolved_contract.as_ref()
    }

    /// Publishes a validated resolved VOS contract for downstream consumers.
    pub fn publish_resolved_contract(&mut self, contract: vos::types::ResolvedContract) -> Result<()> {
        validate_contract(self.schema.as_ref(), &contract)?;
        if let Some(current) = &self.resolved_contract {
            if current != &contract {
                return Err(Error::Unsupported("resolved contract evolution requires an explicit identity migration"));
            }
        }
        self.resolved_contract = Some(contract);
        Ok(())
    }

    /// Registered shard ids in catalog order.
    pub fn shards(&self) -> &[ShardId] {
        &self.shards
    }

    /// Returns the currently published routing map, if one exists.
    pub fn routing(&self) -> Result<Option<ShardMap>> {
        self.routing_epoch
            .map(|epoch| ShardMap::new(epoch, self.shards.clone()).map_err(|_| Error::Corrupt("invalid catalog routing map")))
            .transpose()
    }

    /// Current published owner records, one per shard.
    pub fn owners(&self) -> &[OwnerEntry] {
        &self.owners
    }

    /// Sets or replaces the owner for one shard.
    ///
    /// The catalog accepts any `LeaderTerm` and `ReplicaNodeId` tuple here;
    /// fencing validation against the runtime replica set happens in the
    /// execution layer (Track B).
    pub fn publish_owner(&mut self, shard_id: ShardId, node_id: ReplicaNodeId, leader_term: LeaderTerm) {
        if let Some(existing) = self.owners.iter_mut().find(|o| o.shard_id == shard_id) {
            existing.node_id = node_id;
            existing.leader_term = leader_term;
        }
        else {
            self.owners.push(OwnerEntry { shard_id, node_id, leader_term });
        }
    }

    /// Replaces the entire owner list atomically.
    pub fn publish_owners(&mut self, owners: Vec<OwnerEntry>) {
        self.owners = owners;
    }

    /// Stores catalog schema truth when empty and rejects mismatched versions thereafter.
    pub fn ensure_schema(&mut self, version: u32, document: &str) -> Result<()> {
        match &self.schema {
            Some(schema) if schema.version != version => {
                Err(Error::SchemaConflict { expected: version, found: schema.version })
            }
            Some(schema) if schema.document != document => {
                Err(Error::SchemaConflict { expected: version, found: schema.version })
            }
            Some(_) => {
                if self.resolved_contract.is_none() {
                    self.resolved_contract = Some(resolved_contract_for_schema(self.schema.as_ref().expect("schema exists"))?);
                }
                Ok(())
            }
            None => {
                let schema = adopt_catalog_schema(version, document)?;
                let contract = resolved_contract_for_schema(&schema)?;
                self.schema = Some(schema);
                self.resolved_contract = Some(contract);
                Ok(())
            }
        }
    }

    /// Registers a shard id exactly once.
    pub fn register_shard(&mut self, shard: ShardId) -> Result<()> {
        if self.routing_epoch.is_some() {
            return Err(Error::Unsupported("publish a newer routing epoch to change shard membership"));
        }
        if self.shards.iter().any(|existing| existing == &shard) {
            return Err(Error::Unsupported("shard already registered in catalog"));
        }
        self.shards.push(shard);
        Ok(())
    }

    /// Publishes a newer complete shard map and routing epoch atomically in catalog state.
    pub fn publish_routing(&mut self, epoch: ShardEpoch, shards: Vec<ShardId>) -> Result<()> {
        if let Some(current) = self.routing_epoch {
            if epoch <= current {
                return Err(Error::RoutingConflict { expected: current.0, found: epoch.0 });
            }
        }
        let map = ShardMap::new(epoch, shards).map_err(|_| Error::Unsupported("invalid routing map"))?;
        self.shards = map.shards().to_vec();
        self.routing_epoch = Some(epoch);
        Ok(())
    }

    /// Persists the catalog when file-backed.
    pub fn flush(&self) -> Result<()> {
        let Some(path) = &self.path
        else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let bytes = format::encode(
            self.schema.as_ref(),
            self.routing_epoch,
            &self.shards,
            self.resolved_contract.as_ref(),
            &self.owners,
        )?;
        std::fs::write(path, bytes)?;
        Ok(())
    }
}

fn resolved_contract_for_schema(schema: &CatalogSchema) -> Result<vos::types::ResolvedContract> {
    let projection = vos::parse_oak(&schema.document)
        .map_err(|message| Error::Schema { message })?
        .project_schema()
        .map_err(|diagnostics| Error::Schema { message: format!("VOS semantic projection failed: {diagnostics:?}") })?;
    let mut next_type_id = 1u64;
    let mut next_field_id = 1u64;
    let types = projection
        .types
        .iter()
        .map(|projected| {
            if projected.canonical_path.is_empty() {
                return Err(Error::Corrupt("VOS type has an empty canonical path"));
            }
            let type_id = next_type_id;
            next_type_id += 1;
            let fields = projected
                .fields
                .iter()
                .enumerate()
                .map(|(field_index, field)| {
                    let field_id = next_field_id;
                    next_field_id += 1;
                    Ok(vos::types::FieldIdentity {
                        canonical_name: field.canonical_name.clone(),
                        field_id,
                        virtual_field_index: Some(field_index as u32),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(vos::types::TypeIdentity {
                canonical_path: projected.canonical_path.clone(),
                type_id,
                kind: projected.kind,
                fields,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let manifest = vos::types::IdentityManifest { format_version: vos::types::IDENTITY_MANIFEST_VERSION.to_owned(), types };
    let contract = vos::resolve_contract(&projection, &manifest)
        .map_err(|diagnostics| Error::Schema { message: format!("VOS resolved contract failed: {diagnostics:?}") })?;
    validate_contract(Some(schema), &contract)?;
    Ok(contract)
}

pub(crate) fn validate_contract(schema: Option<&CatalogSchema>, contract: &vos::types::ResolvedContract) -> Result<()> {
    let artifact = contract.to_json().map_err(|_| Error::Corrupt("resolved contract encode"))?;
    vos::types::ResolvedContract::from_json(&artifact).map_err(|error| Error::Schema {
        message: format!("invalid resolved VOS contract {}: {}", error.code, error.message),
    })?;
    let schema = schema.ok_or(Error::Unsupported("publish schema before resolved contract"))?;
    let projection = vos::validate_schema(&schema.document).map_err(|message| Error::Schema { message })?;
    let manifest = vos::types::IdentityManifest {
        format_version: contract.identity_manifest_version.clone(),
        types: contract
            .types
            .iter()
            .map(|entry| vos::types::TypeIdentity {
                canonical_path: entry.canonical_path.clone(),
                type_id: entry.type_id,
                kind: entry.kind,
                fields: entry
                    .fields
                    .iter()
                    .map(|field| vos::types::FieldIdentity {
                        canonical_name: field.canonical_name.clone(),
                        field_id: field.field_id,
                        virtual_field_index: field.virtual_field_index,
                    })
                    .collect(),
            })
            .collect(),
    };
    let expected = vos::resolve_contract(&projection, &manifest).map_err(|diagnostics| Error::Schema {
        message: format!("resolved contract does not match schema: {diagnostics:?}"),
    })?;
    if &expected != contract {
        return Err(Error::Corrupt("resolved contract does not match schema"));
    }
    Ok(())
}

/// Returns true when `path` uses the `.yyds` catalog suffix.
pub fn is_catalog_path(path: &Path) -> bool {
    path.extension().is_some_and(|ext| ext == CATALOG_SUFFIX)
}

/// Builds a catalog path with the `.yyds` suffix.
pub fn catalog_path(base: impl AsRef<Path>) -> PathBuf {
    base.as_ref().with_extension(CATALOG_SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_path_uses_yyds_suffix() {
        let path = catalog_path("cluster-a");
        assert_eq!(path.extension().and_then(|ext| ext.to_str()), Some(CATALOG_SUFFIX));
    }

    #[test]
    fn empty_catalog_has_no_owners() {
        let catalog = Catalog::open_memory();
        assert!(catalog.owners().is_empty());
    }

    #[test]
    fn publish_owner_adds_or_updates() {
        let mut catalog = Catalog::open_memory();
        catalog.publish_owner(ShardId("shard-0".into()), ReplicaNodeId("node-a".into()), LeaderTerm(1));
        assert_eq!(catalog.owners().len(), 1);
        assert_eq!(catalog.owners()[0].leader_term, LeaderTerm(1));

        // Same shard_id, higher term — should update, not append.
        catalog.publish_owner(ShardId("shard-0".into()), ReplicaNodeId("node-a".into()), LeaderTerm(2));
        assert_eq!(catalog.owners().len(), 1);
        assert_eq!(catalog.owners()[0].leader_term, LeaderTerm(2));
    }

    #[test]
    fn publish_owners_roundtrip_via_flush_and_reopen() {
        let dir = boost_fs::temp::create_folder().expect("tempdir");
        let path = catalog_path(dir.path().join("owner-test"));

        {
            let mut catalog = Catalog::open(&path).expect("open");
            catalog.publish_owner(ShardId("shard-0".into()), ReplicaNodeId("node-a".into()), LeaderTerm(1));
            catalog.publish_owner(ShardId("shard-1".into()), ReplicaNodeId("node-b".into()), LeaderTerm(3));
            catalog.flush().expect("flush");
        }

        let reopened = Catalog::open(&path).expect("reopen");
        assert_eq!(reopened.owners().len(), 2);
        let shard0 = reopened.owners().iter().find(|o| o.shard_id == ShardId("shard-0".into())).expect("shard-0");
        assert_eq!(shard0.node_id, ReplicaNodeId("node-a".into()));
        assert_eq!(shard0.leader_term, LeaderTerm(1));

        let shard1 = reopened.owners().iter().find(|o| o.shard_id == ShardId("shard-1".into())).expect("shard-1");
        assert_eq!(shard1.node_id, ReplicaNodeId("node-b".into()));
        assert_eq!(shard1.leader_term, LeaderTerm(3));
    }
}
