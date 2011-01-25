# yydb

嵌入式单文件数据库的 **Rust 门面**。打开 `.yydb`，用 [VOS](https://github.com/voml/vos-language) 声明 schema，同一 `Connection` 上做 KV、事务与 serve。

产品壳的 Node / Python / wasm 绑定只应依赖本 crate，不要直连 `yydb-engine` 等内部层。

## 用法

```rust
use yydb::{Connection, OpenFlags, Result};

fn main() -> Result<()> {
    let db = Connection::open_with_flags("app.yydb", OpenFlags::wal())?;
    db.ensure_schema("table Project { @@id: uuid, title: utf8 }")?;
    db.put("project/title", b"Spark")?;
    assert_eq!(db.get("project/title")?.as_deref(), Some(b"Spark".as_slice()));
    Ok(())
}
```

常用入口：`connection` / `udf` / `catalog` / `protocol`；非 `wasm32` 另有 `client` / `server`。

## 约束

- 壳工程**只依赖**本 crate
- **无 SQL 产品合同**；方言 SQL 续跑归 `sqlite` 产品 L3
- `wasm32` 目标不含 `server` / `client`

## 相关

| 需求 | 去向 |
|------|------|
| format-3 文件 | 门面 `sqlite` |
| 分布式 / 方言伪装 | 门面 `yyds` |
| ORM | 门面 `iris` |
| Node / Python 壳 | [yy-database/yydb](https://github.com/yy-database/yydb) |
| 仓库总览 | [根 readme](../../../readme.md) |
