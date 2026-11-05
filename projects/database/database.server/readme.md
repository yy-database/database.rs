# `database-server`

服务会话、认证入口、预处理语句生命周期与请求分派；按来源分 `yydb`（嵌入式内核宿主 / 本地服务）与 `yyds`（分布式运行时与 MySQL / PostgreSQL / Redis / Mongo 方言网关）。
