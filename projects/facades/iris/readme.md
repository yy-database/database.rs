# iris

Iris ORM 的 **Rust 产品门面**。产品壳只依赖本 crate；host / 生成器 / wasm / migrate 用 feature 打开。

## Feature

| Feature | 用途 |
|---------|------|
| `host`（默认） | 会话核心（napi / pyo3 共用） |
| `generator` / `generator-aot` | 代码生成与模板 |
| `wasm-bridge` | 浏览器 / 边缘辅助 |
| `migrate` | 外存托管推送 |
| `napi` / `pyo3` | 对应宿主组合面 |

## 用法

```rust
use iris::host;

fn main() {
    // 宿主绑定经 host 会话核心翻译；稳定诊断码可在绑定层断言。
    assert_eq!(host::CODE_NOT_FOUND, "IRIS-NOT-FOUND");
}
```

生成与迁移示例在 `iris-orm` 产品壳。

## 约束

- 壳工程禁止直连 `iris-host` / `iris-generator` / `iris-adapter-*` / `iris-connector-*`
- 外存 MySQL / PG / Redis 适配属 **L1**；SQLite 适配随产品 `sqlite` 的 **L3**，禁止 `rusqlite`

## 相关

[根 readme](../../../readme.md) · 产品壳 [voml/iris-orm](https://github.com/voml/iris-orm)
