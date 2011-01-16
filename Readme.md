# database.rs

Aggregated Rust core for the YY database family.

Product repos (`yydb`, `yyds`, `iris-orm`, and dialect surfaces) stay as **distribution shells**: language bindings, packaging, and public branding. Shared engines, formats, gateways, and Iris adapters live here so they are not forked three ways.

## Workspace layout

| Area | Crates |
|------|--------|
| YYDB | `yydb`, `yydb-*`, `sqlite`, `sqlite-*` |
| YYDS | `yyds-*`, `yyds-gateways/*` (MySQL / PostgreSQL / Redis / Mongo) |
| Iris | `iris`, `iris-*` adapters and connectors |

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
