use yyds_types::{adopt_catalog_schema, validate_document, Error};

#[test]
fn validate_document_rejects_nul_bytes() {
    let err = validate_document("table T {\0}").expect_err("nul");
    assert!(matches!(err, Error::Schema { .. }));
}

#[test]
fn adopt_catalog_schema_accepts_vos_text() {
    let source = "table User {\n    @@id: uuid,\n}\n";
    let schema = adopt_catalog_schema(1, source).expect("adopt");
    assert_eq!(schema.version, 1);
    assert_eq!(schema.document, source);
}

#[test]
fn catalog_rejects_invalid_oak_structure_and_vos_semantics() {
    for source in ["table User { @@id: uuid } ]", "table User { id: i64 }"] {
        assert!(matches!(adopt_catalog_schema(1, source), Err(Error::Schema { .. })));
    }
}
