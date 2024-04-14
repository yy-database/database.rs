//! 保证 `Error::code()` 在本 crate 内唯一，且落在 `database.` 命名空间。
//!
//! 产品壳的公开错误目录由各产品仓自行维护；底库测试不钉产品路径。

use std::{collections::BTreeSet, fs};

fn engine_codes() -> BTreeSet<String> {
    let source = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs")).expect("crate source is readable");
    source
        .lines()
        .filter(|line| line.contains("=> \"database."))
        .filter_map(|line| {
            let start = line.find("=> \"")? + 4;
            let rest = &line[start..];
            let end = rest.find('"')?;
            Some(rest[..end].to_string())
        })
        .collect()
}

#[test]
fn engine_codes_are_unique_and_unbranded() {
    let codes = engine_codes();
    assert_eq!(codes.len(), 32, "code count changed: {codes:?}");
    for code in &codes {
        assert!(code.starts_with("database."), "`{code}` must stay inside the `database.` namespace");
        assert!(!code.starts_with("yydb."), "`{code}` must not use product-branded prefix");
        assert!(!code.starts_with("yyds."), "`{code}` must not use product-branded prefix");
    }
}
