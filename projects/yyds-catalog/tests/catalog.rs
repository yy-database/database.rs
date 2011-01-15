use std::fs;

use boost_fs::temp::create_folder;
use yyds_catalog::{catalog_path, Catalog};
use yyds_types::{ShardEpoch, ShardId};

const VOS: &str = "table User {\n    @@id: uuid,\n}\n";

#[test]
fn ensure_schema_and_register_shard_in_memory() {
    let mut catalog = Catalog::open_memory();
    catalog.ensure_schema(1, VOS).expect("ensure");
    catalog.register_shard(ShardId("shard-0".into())).expect("register");

    let schema = catalog.schema().expect("schema");
    assert_eq!(schema.version, 1);
    assert_eq!(schema.document, VOS);
    assert!(catalog.resolved_contract().is_some());
    assert_eq!(catalog.shards().len(), 1);
}

#[test]
fn file_catalog_round_trip() {
    let dir = create_folder().expect("tempdir");
    let path = catalog_path(dir.path().join("cluster-a"));

    {
        let mut catalog = Catalog::open(&path).expect("open");
        catalog.ensure_schema(1, VOS).expect("ensure");
        catalog.register_shard(ShardId("shard-a".into())).expect("register");
        catalog.flush().expect("flush");
    }

    let catalog = Catalog::open(&path).expect("reload");
    let schema = catalog.schema().expect("schema");
    assert_eq!(schema.version, 1);
    assert_eq!(schema.document, VOS);
    assert!(catalog.resolved_contract().is_some());
    assert_eq!(catalog.shards(), &[ShardId("shard-a".into())]);
    assert!(path.is_file());

    let bytes = fs::read(&path).expect("read");
    assert!(bytes.starts_with(yyds_catalog::MAGIC));
}

#[test]
fn ensure_schema_rejects_version_mismatch() {
    let mut catalog = Catalog::open_memory();
    catalog.ensure_schema(1, VOS).expect("ensure");
    let err = catalog.ensure_schema(2, VOS).expect_err("conflict");
    assert!(err.to_string().contains("schema version conflict"));
}

#[test]
fn routing_epoch_round_trips_and_requires_monotonic_publish() {
    let dir = create_folder().expect("tempdir");
    let path = catalog_path(dir.path().join("routing"));
    let mut catalog = Catalog::open(&path).expect("open");
    catalog.publish_routing(ShardEpoch(1), vec![ShardId("a".into()), ShardId("b".into())]).expect("publish");
    assert_eq!(catalog.routing().expect("routing").expect("map").epoch(), ShardEpoch(1));
    assert!(catalog.publish_routing(ShardEpoch(1), vec![ShardId("a".into())]).is_err());
    catalog.flush().expect("flush");

    let reopened = Catalog::open(&path).expect("reopen");
    let map = reopened.routing().expect("routing").expect("map");
    assert_eq!(map.epoch(), ShardEpoch(1));
    assert_eq!(map.shards(), &[ShardId("a".into()), ShardId("b".into())]);
}

#[test]
fn resolved_contract_round_trips_and_rejects_conflicting_publish() {
    let source = "class T { id: uuid }";
    let projection = vos::parse_oak(source).expect("Oak parses").project_schema().expect("projection");
    let manifest = vos::types::IdentityManifest {
        format_version: vos::types::IDENTITY_MANIFEST_VERSION.into(),
        types: vec![vos::types::TypeIdentity {
            canonical_path: vec!["T".into()],
            type_id: 1,
            kind: vos::types::TypeContractKind::Class,
            fields: vec![vos::types::FieldIdentity { canonical_name: "id".into(), field_id: 1, virtual_field_index: Some(0) }],
        }],
    };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    let dir = create_folder().expect("tempdir");
    let path = catalog_path(dir.path().join("contract"));
    let mut catalog = Catalog::open(&path).expect("open");
    assert!(catalog.publish_resolved_contract(contract.clone()).is_err());
    catalog.ensure_schema(1, source).expect("schema");
    catalog.publish_resolved_contract(contract.clone()).expect("publish");
    catalog.flush().expect("flush");

    let reopened = Catalog::open(&path).expect("reload");
    assert_eq!(reopened.resolved_contract(), Some(&contract));

    let mut conflicting = contract.clone();
    conflicting.schema_fingerprint = "0".repeat(64);
    assert!(reopened.clone().publish_resolved_contract(conflicting).is_err());
}

#[test]
fn older_catalog_bytes_are_rejected_instead_of_silently_reinterpreted() {
    let dir = create_folder().expect("tempdir");
    let path = catalog_path(dir.path().join("legacy"));
    let mut bytes = yyds_catalog::MAGIC.to_vec();
    bytes.extend_from_slice(&7u32.to_le_bytes());
    fs::write(&path, &bytes).expect("legacy file");
    let error = Catalog::open(&path).expect_err("legacy catalog must not load");
    assert!(error.to_string().contains("requires regeneration"), "{error}");
}

#[test]
fn failed_schema_resolution_does_not_publish_partial_catalog() {
    let mut catalog = Catalog::open_memory();
    let before = catalog.clone();
    assert!(catalog.ensure_schema(1, "table User { @@id: uuid, value: Missing }").is_err());
    assert_eq!(catalog, before);
    catalog.ensure_schema(1, VOS).expect("retry with valid schema");
    let contract = catalog.resolved_contract().unwrap();
    assert_eq!(contract.types[0].type_id, 1);
    assert_eq!(contract.types[0].fields[0].field_id, 1);
}
