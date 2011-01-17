# gateways

Protocol disguise gateways that make YYDS look like familiar database products on the wire.

| Crate | Role |
|-------|------|
| `yyds-gateway` | Shared config, banners, SQL bind/plan (extract target: `yyds-query`), legacy-surface guards |
| `yyds-gateway-redis` | Redis wire disguise |
| `yyds-gateway-mysql` | MySQL wire disguise |
| `yyds-gateway-pgsql` | PostgreSQL wire disguise |
| `yyds-gateway-mongo` | Mongo wire disguise |

Shared behavioral cases live under `../conformance/gateway-conformance`.

These gateways are **not** SQL translation layers for Iris. They expose a dialect wire surface backed by YYDS. Native YY wire belongs in `yyds-protocol` / `yyds-server`, not here.
