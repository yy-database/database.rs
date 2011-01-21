# database.rs

Aggregated Rust core for the YY database family.

实现约定（`unsafe`、文档 lint、中文注释、门面依赖）见 [`AGENTS.md`](./AGENTS.md)。

Product repos (`yydb`, `yyds`, `iris-orm`, and dialect surfaces) stay as **distribution shells**: language bindings, packaging, and public branding. Shared formats, execution, gateways, and Iris adapters live here so they are not forked three ways.

## Storage vs dialect compatibility

| Surface | Role | On-disk format |
|---------|------|----------------|
| YYDB + `sqlite` facade | Embedded engines | `yydb-formats` / `sqlite-formats` (parse & own storage) |
| YYDS | Distributed storage | `yyds-formats` (`.yyds` / `.yykv` / `YYRL`) via `yyds-kv` |
| MySQL / PostgreSQL / Redis / Mongo gateways | **Service-level** wire compatibility (+ migration assist) | **YYDS storage only** — no foreign data directories |

Dialect gateways and Iris foreign adapters speak our own `yyds-gateway-*` wire clients/servers. Third-party `mysql` / `postgres` / `redis` / `r2d2` crates are not workspace dependencies.

## Workspace layout

Crates are grouped by **function / role**, not by product.

| Role | Path | Notes |
|------|------|-------|
| Types / IR | `projects/types/*` | Shared contracts |
| Formats | `projects/formats/*` | `yydb-formats`, `sqlite-formats`, `yyds-formats` |
| Storage | `projects/storage/*` | IO / shard lifecycle |
| Execution | `projects/execution/*` | Runtime kernels (`yydb-engine`, `*-execution`, `yydb-udf`) |
| Planning | `projects/planning/*` | `yydb-query`, `yyds-query` (scaffold) |
| Facades | `projects/facades/*` | Thin product assembly (`yydb`, `yyds`, `sqlite`, `iris`, hosts) |
| Control | `projects/control/*` | YYDS catalog / control |
| Cluster | `projects/cluster/*` | Coordinator / exchange / replication (scaffold) |
| Protocol | `projects/protocol/*` | Native YY wire codecs |
| Gateways | `projects/gateways/*` | Disguise wire **server + client** |
| Adapters | `projects/adapters/*` | Iris dialect adapters (gateway clients; SQLite embedded) |
| Connectors | `projects/connectors/*` | Iris → YYDB / YYDS |
| Services | `projects/services/*` | Native wire client/server |
| Tools | `projects/tools/*` | Bench / generator |
| Conformance | `projects/conformance/*` | Shared behavioral cases |

```shell
cargo check --workspace
```

Pinned toolchain: `rust-toolchain.toml` (`nightly-2026-07-20`).

Local sibling path overrides belong in `.cargo/config.toml` (gitignored). CI resolves the pinned git `rev`s in the root `Cargo.toml`.

## Commit style

| Emoji | Meaning |
|-------|---------|
| 🎂 | Project initialized |
| 🚧 | Scaffold / migration in progress |
| ✨ | User-visible capability |
| 🐛 | Bug fix |
| 🧪 | Tests |
| 📝 | Documentation |
| ♻️ | Refactor |
| 🔥 | Delete obsolete code |
| 📦 | Packaging / publish |
