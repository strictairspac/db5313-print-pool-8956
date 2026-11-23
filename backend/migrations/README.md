# 初始结构

`src/models/tables.py` 定义初始关系结构。API 启动时显式创建演示库的表；此版本只包含初始 schema。需要在已有部署升级表结构时，应单独引入版本化迁移。

## 交接表

`handover_lanes` 为每个网点保存已分配的交接序号，签署事务按网点行锁递增 `last_order`。
`handover_batches` 保存逐网点的交接批次：`number` 由凭证与网点确定、唯一，`order_seq`
是网点内的签署生效顺序，`state` 取 `pending`（待交接）、`dispatched`（已派发）或
`received`（已回执）。已有部署升级时需为这两张表补建结构；既有证据不会自动补交接记录。
