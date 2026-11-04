# yyds-types

Shared identity and error types for **YYDS**, the distributed VOS database.

YYDS is the network-facing sibling of embedded [YYDB](https://github.com/yy-database/yydb.rs). Like YYDB it uses **[VOS](https://github.com/voml/vos-language)** for schema, DDL, and query. There is no private SQL dialect and no parallel schema language.

Storage differs from YYDB: catalog truth lives in versioned `.yyds` files (VOS documents) and shard data in `.yykv` files (`yyds-kv`).
