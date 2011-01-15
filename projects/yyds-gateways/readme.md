# yyds-gateways

Protocol disguise gateways that make YYDS look like familiar database products on the wire.

| Crate | Role |
|-------|------|
| `yyds-gateway` | Shared config, banners, and legacy-surface guards |
| `yyds-gateway-redis` | Redis wire disguise |
| `yyds-gateway-mysql` | MySQL wire disguise |
| `yyds-gateway-pgsql` | PostgreSQL wire disguise |

These gateways are **not** SQL translation layers for Iris and **not** legacy `yyds-gateway/src/sql`. They expose a minimal health and metadata surface backed by YYDS.
