# mongo

MongoDB **产品门面**：文档存储、游标、`local.oplog.rs` 条目与命令语义，全部只写一份，
存储底座由注入的后端提供。

| 模块 | 职责 |
|------|------|
| `storage` | 存储 trait `MongoStorage`：命令层唯一的数据入口 |
| `store` | `.yyds` catalog + `.yykv` shard 后端（`storage` feature） |
| `cursor` | 调用方自有的游标表，惰性回收 |
| `oplog` | `local.oplog.rs` 条目形态，按 `mongod 8.0.9` 实测 |
| `import` | `mongodump` 形态 `.bson` 流导入 |

## 边界

- 本 crate 只提供库原语：不导出 bin、不实现 server 主循环、不做 CLI。
- napi / pyo3 / wasm 绑定只做类型转换与错误映射，命令语义只能落在本 crate。
- 存储后端有两个注入方向：产品仓注入 `store::Store`（`.yykv`），`yyds` 注入
  `KeyValueExecutor` 适配器。命令层对两者只认 `MongoStorage`。

## 相关

[根 readme](../../../readme.md) · 产品壳 `MongoDB` 仓
