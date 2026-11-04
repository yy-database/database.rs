# yyds-catalog

Catalog control plane for **YYDS**: versioned VOS schema documents and shard registration in `.yyds` files.

This crate is **not** the embedded `.yydb` engine. It owns cluster catalog truth that `yyds-kv` shards execute under.
