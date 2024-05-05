# Database Agents

本仓是 YY 数据库家族的 **聚合 Rust 内核**。产品壳（`yydb` / `yyds` / `iris-orm` / `SQLite`）只依赖对应门面 crate，不直接钉内部角色
crate。

工作区根目录另有全局索引；本文件只写本仓实现约定。用户向产品故事见 [`readme.md`](./readme.md)；目录角色与维护者命令见 `scripts/`。

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

## 序列化命名边界

- **R1 核心类型只说 Rust 的话**：`*.types`、`database.query` 的 IR/local 类型、集群类型的字段一律 `snake_case`，枚举判别值一律 `snake_case`（`i64`、`null` 这类单词值保持小写）。**禁止**在核心类型上出现面向单一语言的 `serde(rename_all)`。
- **R2 翻译只在绑定层发生**：napi 优先用 `#[napi(object)]` 自动转 camelCase；需要返回 JSON 字符串或自定义拼写时在绑定 crate 内建专用 DTO，**不得直接序列化核心类型**。
- **R3 pyo3 只说 Python 的话**：Python 侧参数名与返回键一律 `snake_case`，与 Rust 字段同名；**禁止**向 Python 返回 camelCase 键。协议原语（RESP/BSON/PG 报文）不受此限。
- **R4 协议优先，可读性让位**：字段拼写以协议/宿主编解码器为准（Mongo Extended JSON、PG/MySQL/Redis 报文、VOS operation artifact、生成客户端 `OperationRequest`、`$database` tagged JSON）。**禁止**为了"好读"改协议字段，包括 `algo`、`mediaType`、`utcMicros`。
- **R5 落盘契约带版本**：`node.json`、`node.status.json`、SQL schema document 属数据面。改动必须同时升版本号、保留读旧格式、并有迁移测试；不得按语言习惯顺手改。落盘键名由核心内私有 DTO（如 `StoredIdentity`）或绑定层 DTO 冻结，核心类型本身不携带。
- **R6 wasm 跟随 JS**：wasm 绑定输出 camelCase，转换在 `*-wasm` 适配层完成，不回流核心类型。
- **R7 枚举值统一**：同一 crate 内判别值统一 `snake_case`；仅协议规定处或影子迁移期与既有台账词汇逐字节对齐处（如 `database.harness` 的 `BaselineProblem`）使用 kebab-case，且必须在代码注释里钉明理由。每个被外部消费的值必须在测试里钉死。
- **R8 全称优先**：内部标识符能写全称就写全称（`slot_type`、`leader_node_id`、`algorithm`）；已发布配置键（`generate.out`）与协议字段只补文档、不改名。
- **R9 新增字段的默认答案**：新增 serde 结构体先问"它属于哪一层"。答不上来就不要加 `rename_all`。

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
