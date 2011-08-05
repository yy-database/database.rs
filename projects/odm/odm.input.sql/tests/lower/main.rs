//! SQL DDL → MappingModel 冒烟。

use odm_input_sql::lower_source;

#[test]
fn lowers_simple_create_table() {
    let model = lower_source("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);").expect("parse");
    assert_eq!(model.entities.len(), 1);
    assert_eq!(model.entities[0].name, "users");
    assert_eq!(model.entities[0].fields.len(), 3);
    assert!(model.entities[0].fields[0].primary_key);
    assert!(!model.entities[0].fields[1].nullable);
    assert!(model.entities[0].fields[2].unique);
}
