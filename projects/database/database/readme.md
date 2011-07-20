# `database`

数据库家族的唯一公开门面：按能力再导出 types / input / query / execution / udf / formats / storage / cluster / protocol / client / server，不实现业务逻辑。

产品壳只依赖本门面；对象—数据库映射见独立的 `odm` 能力域。
