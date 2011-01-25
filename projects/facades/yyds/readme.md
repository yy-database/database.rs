# yyds

YYDS 分布式数据库的 **唯一 Rust 门面**。再导出控制面、存储、执行与方言网关。

目标档是 **L1 服务模拟**：线协议像 MySQL / PostgreSQL / Redis / Mongo，磁盘仍是 YYDS 自有格式。

## 用法

```rust
use std::time::Duration;
use yyds::gateway::mysql::client::Session;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _session = Session::connect("mysql://127.0.0.1:3306", Duration::from_secs(3))?;
    Ok(())
}
```

模块面：`types` / `kv` / `catalog` / `control` / `execution` / `gateway::{mysql,pgsql,redis,mongo}`。更完整的宿主示例在 `yyds` 产品壳的 napi / pyo3。

## 约束

- 壳工程只依赖本 crate，禁止直连 `yyds-kv` / `yyds-gateway-*`
- 不引入第三方 `mysql` / `postgres` / `redis` 客户端顶替自有 wire
- 网关不因 SQLite L3 升档，停在 L1

## 相关

[根 readme](../../../readme.md) · 产品壳 [yy-database/yyds](https://github.com/yy-database/yyds)
