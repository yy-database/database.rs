# `database-protocol`

线协议门面：再导出 `database-protocol-{yy,mysql,pgsql,redis,mongo}`，并提供伪装网关元数据。

实现优先直接依赖具体 protocol 子 crate；门面供 `database::protocol` 嵌套导出。
