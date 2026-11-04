# yyds

Sole Rust dependency for YYDS distribution shells.

Re-exports control, storage, execution, and dialect gateways. Shells must not
depend on `yyds-kv` / `yyds-gateway-*` directly.
