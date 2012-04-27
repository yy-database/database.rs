# sqlite

SQLite **产品门面**：format-3 **L2** 只读检查与可写快照。不捆绑官方 C 库，不依赖 YYDB 引擎。

| 档 | 能力 | 入口 |
|----|------|------|
| L2 | 只读打开既有 `.sqlite` | `SqliteDatabase` |
| L2 | 写出新快照 / 夹具 | `SqliteWriter` |
| L3 | SQL 执行（fail-closed 子集） | `sql::Engine` |

## 用法

```rust
use sqlite::{SchemaObjectKind, SqliteDatabase, TableLimits};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = SqliteDatabase::open("app.sqlite")?;
    let tables = db.schema(TableLimits::default())?
        .into_iter()
        .filter(|o| o.kind == SchemaObjectKind::Table);
    let _rows = db.table("samples", TableLimits::default())?;
    let _ = tables.count();
    Ok(())
}
```

## 约束

- L2 不做 WAL 恢复、不做 SQL 执行，SQL 走 L3 `sql::Engine`
- 禁止用 `rusqlite` / 官方引擎冒充 L3
- Iris `iris-adapter-sqlite` 随本产品 L3 就绪后再挂执行面

## 相关

[根 readme](../../../readme.md) · 产品壳 `SQLite` 仓
