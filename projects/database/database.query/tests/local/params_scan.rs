//! 从 `src/params.rs` 迁出的测试。

use database_query::local::{Dialect, prepare, scan_and_rewrite};

#[test]
fn prepare_extracts_dollar_named_parameters_in_order() {
    let source = r#"User.all().take($limit).collect()"#;
    let (_, rewritten) = scan_and_rewrite(source, Dialect::Vos).expect("scan");
    assert!(vos::parse_program(&rewritten).is_ok(), "rewritten must parse: {}", rewritten);
    let stmt = prepare(source).expect("prepare");
    assert_eq!(stmt.parameters.len(), 1);
    assert_eq!(stmt.parameters[0].name, "limit");
    assert_eq!(stmt.parameters[0].index, 0);
}

// ─── new lexer-aware tests ─────────────────────────────────────────────

#[test]
fn param_inside_double_string_is_not_extracted() {
    // `$limit` inside a string literal must not be treated as a parameter.
    let (params, _) = scan_and_rewrite("User.all().filter(|u| u.name == \"$limit\").collect()", Dialect::Vos).unwrap();
    assert!(params.is_empty(), "string literal `$limit` should not be a param");
}

#[test]
fn param_inside_line_comment_is_not_extracted() {
    let (params, _) = scan_and_rewrite("User.all().take($x).collect() // $y should not count", Dialect::Vos).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0].name, "x");
}

#[test]
fn param_inside_block_comment_is_not_extracted() {
    let (params, _) = scan_and_rewrite("User.all().take($x) /* $y is inside */ .collect()", Dialect::Vos).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0].name, "x");
}

#[test]
fn double_string_with_escape_is_handled() {
    let (params, _) = scan_and_rewrite("User.all().take($x).filter(|u| u.n == \"has\\\"$y\"\").collect()", Dialect::Vos).unwrap();
    assert_eq!(params.len(), 1, "only `$x` is a param");
    assert_eq!(params[0].name, "x");
}
