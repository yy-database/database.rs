# Database Agents

本仓是 YY 数据库家族的 **聚合 Rust 内核**。产品壳（`yydb` / `yyds` / `iris-orm` / `SQLite`）只依赖对应门面 crate，不直接钉内部角色
crate。

工作区根目录另有全局索引；本文件只写本仓实现约定。用户向产品故事见 [`readme.md`](./readme.md)；目录角色与维护者命令见 [`scripts/README.md`](./scripts/README.md)。

## 语言

- **文档、模块说明、公开 API 注释默认用中文。**
- 标识符、协议字段名、错误码、测试名保持英文（与 wire / serde 合同一致）。
- 改到英文遗留注释时，顺手改成中文，不要留双语复读。

## `unsafe`

- **禁止** `#![forbid(unsafe_code)]` / `#![deny(unsafe_code)]`。
- 高性能路径（页编解码、零拷贝窗口、锁原语、SIMD、与 OS/FFI 边界）允许、也预期会出现 `unsafe`。
- 每个 `unsafe` 块必须有 **中文**安全注释：不变量是什么、谁保证、越界/别名/生命周期如何成立。
- 能用 safe 封装把危险面收窄的，优先封装；不要为了“看起来安全”把热路径藏进无注释的 `unsafe`。

## 文档 lint

- 库 crate 使用 `#![deny(missing_docs)]`（可与其它 `deny`/`warn` 组合）。
- **不要**停在 `#![warn(missing_docs)]`：警告会被忽略，缺口会堆积。
- **禁止**模板硬补（「某某字段」「TODO docs」「The X.」这类空话）。
- 补文档前先读调用点与不变量：写清 **职责、单位、非法值、与相邻字段的关系**。
- 元组变体 / 结构体字段缺 docs 同样按 `deny` 处理，字段说明要落到语义，不只复述类型名。

## 门面与依赖

- 下游产品壳只依赖门面：`yydb` / `yyds` / `iris` / `sqlite`。
- git 依赖钉完整 40 位 `rev`；本地覆盖只 patch **门面** path，写在 gitignore 的 `.cargo/config.toml`。
- 不要再引入官方 SQLite / `rusqlite` / 第三方 MySQL·PG·Redis 客户端做“对照服务”或顶替 L3。

## 兼容档位（实现口径）

| 档 | 名称     | 默认目标                       |
|----|----------|--------------------------------|
| L1 | 服务模拟 | 多数 gateway / foreign adapter |
| L2 | 只读迁移 | format 解析 / 快照             |
| L3 | 接续运行 | **仅 SQLite 产品**主线         |

YYDB **不再**把 SQL 当作产品合同；方言 SQL 续跑归 SQLite L3 计划。

## 提交

- subject 用 gitmoji，标识符用反引号，禁止 ASCII `;.` / 中文 `；。`。
- 一个可独立说明、验证、回滚的逻辑块 → 一次提交 → 立即 `git push origin dev`。
