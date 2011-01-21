//! GraphQL SDL → MappingModel 冒烟。

use odm_input_graphql::lower_source;

#[test]
fn lowers_simple_type() {
    let model = lower_source(
        r#"
            type User {
              id: ID!
              name: String!
              email: String
            }
            "#,
    )
    .expect("parse");
    assert_eq!(model.entities.len(), 1);
    assert_eq!(model.entities[0].name, "User");
    assert_eq!(model.entities[0].fields.len(), 3);
    assert!(model.entities[0].fields[0].primary_key);
    assert!(!model.entities[0].fields[1].nullable);
    assert!(model.entities[0].fields[2].nullable);
}
