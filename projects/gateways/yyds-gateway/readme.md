# yyds-gateway

Shared core for YYDS disguise gateways. Protocol-specific crates (`yyds-gateway-redis`, `yyds-gateway-mysql`, `yyds-gateway-pgsql`) implement wire surfaces on top of this crate.

This is **not** the legacy SQL gateway (`yyds-gateway/src/sql`) and **not** an Iris VOS IR executor path.
