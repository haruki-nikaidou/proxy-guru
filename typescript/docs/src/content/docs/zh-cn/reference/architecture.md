---
title: 架构
description: 各 crate 的职责、Processor 抽象，以及每个模块都遵循的分层规则。
---

## Crate 一览

| Crate | 职责 |
|---|---|
| `bin/guru-master` | 控制平面。单个二进制，四种模式（`--mode`）：`dashboard_grpc`（运维 API）、`workers_grpc`（Worker API + 配置视图轮询器）、`consumer`（AMQP 钩子 —— 配置派生钩子以及所有周期任务）、`cron`（时钟：为每个到期的周期任务发布一条执行信号）。 |
| `bin/guru-worker` | 数据平面。终结监听器并转发流量。既可以基于 TOML 文件独立运行（收到 `SIGHUP` 时重新加载），也可以以 agent 模式运行，从 master 流式获取配置。 |
| `bin/manage-tool` | 管理 CLI：`create-admin` 初始化引导、`orchestration export-config`。 |
| `lib/guru_worker_config` | Worker 配置模型，由两个平面共享。 |
| `lib/rpguru_sdk` | 由 `proto/` 生成的 gRPC/protobuf 类型（Rust）。 |
| `lib/newtype_record_id` | 用于生成带类型的 SurrealDB record id 的 `table_record!` 宏。 |
| `modules/auth` | 账号、会话、API key、RBAC。 |
| `modules/orchestration` | 画布、服务器、节点、连线；拓扑校验、配置派生、Worker 发布。 |
| `modules/notify` | 通知模块 —— 已按 `base` 搭好骨架，尚未实现。 |
| `modules/base` | 共享基础设施，以及所有模块共同遵循的目录布局。 |

## 一切皆 Processor

`kanau::processor::Processor` 就是**状态加上一个异步函数**：

```text
Processor = State + async fn(Input) -> Result<Output, Error>
```

`Processor` 是一个可 `Clone` 的结构体，它持有自己的依赖，并为每个操作实现一次
`Processor<Input>`。每一条数据库查询、每一个业务操作、每一个队列消费者都被建模为「一个输入结构体
加一个 `Processor` 实现」—— 不存在第二套需要额外学习的抽象。

## 模块结构

每个模块 crate 都与 `modules/base` 保持一致：

```text
src/
├── lib.rs        # 声明下面各个模块；设置 crate 级别的 lint
├── config.rs     # 带类型的配置（对应一行 `app_config`，JSON 文档）
├── utils/        # 小巧、几乎无依赖的辅助函数
├── entities/     # 持久化层
│   ├── surreal/  # SurrealDB 行类型 + SurrealProcessor 查询
│   └── redis/    # Redis 键值类型（rkyv 编码）
├── services/     # 业务逻辑（有状态的 Processor）
├── events/       # AMQP 消息载荷 + 路由
├── hooks/        # 后台反应器（事件消费者、周期信号消费者、日志记录器）
└── rpc/          # gRPC 服务实现（传输边界）
```

| 你正在写…… | 就放到…… |
|---|---|
| 一条 SurrealDB 查询或一个表行类型 | `entities/surreal` |
| 一个 Redis 缓存值或临时令牌 | `entities/redis` |
| 一个组合了查询与规则的用例 | `services` |
| 一条其他模块会响应的消息 | `events` |
| 对某个事件的响应 / 一个周期任务 / 一条审计日志 | `hooks` |
| 一个 gRPC 端点实现 | `rpc` |
| 一项运维人员可以修改的带类型设置 | `config` |
| 一个不依赖运行时的纯辅助函数 | `utils` |

## 分层规则

- **依赖方向：** `rpc → services → entities/events/config`。功能模块可以依赖 `base`，但 `base`
  不得依赖任何功能模块；模块之间也不得直接触碰彼此的内部实现 —— 它们通过 gRPC 或 AMQP 事件通信。
- **`entities/surreal`：** 每个表或聚合对应一个子模块；行结构体派生 `SurrealValue`；record id 由
  `table_record!(NameId, "table")` 包装。每条查询在
  `wakuwaku::surreal::SurrealProcessor` 上实现一次 `Processor`，`Error = surrealdb::Error`。
  查询是在运行时校验的，因此必须由针对 `mem://` 的集成测试覆盖。
- **`services`：** 持有自身依赖的 `Clone` 结构体，每个操作一个 `Processor` 实现，返回领域类型 ——
  绝不返回 protobuf 类型。
- **`events`：** 消息载荷加上 `AmqpRouting`（`EXCHANGE`、`EXCHANGE_TYPE`、`ROUTING_KEY`）与
  `AmqpMessageSend`。这是模块之间唯一被认可的异步通道 —— 周期性工作也走这条通道：一个到期的任务是
  一条形如 `sweep_liveness` 的执行信号，而不是一次调用。
- **`hooks`：** AMQP 消费者实现 `AmqpMessageProcessor<E>`，并带有一个持久化的 `QUEUE` 名称。周期任务
  就是其中之一：`cron` 在任务到期时发布信号，钩子在开始工作前先抢占这次运行（写入一行
  `orchestration_job_run`，采用 compare-and-set），因此投递可以重复、消费者可以多副本部署，而同一轮
  处理不会被执行两次。调度器不打开任何数据库连接，也不读取任何配置；决定*做什么*的逻辑全部位于消费者
  一侧。由此带来的结果是：broker 在全部四种模式下都是必需的 —— RabbitMQ 停机期间，配置派生扫描、存活
  扫描和证书续期都不会发生，直到它恢复为止。
- **`rpc`：** 只做薄适配 —— 解码请求、调用服务、编码回复。不含业务逻辑。
- **错误：** 在 service/hook 边界使用 `wakuwaku::Error`；`entities/surreal` 内部使用
  `surrealdb::Error`，并在服务层通过 `?` 转换。
- **Lint：** crate 级别 `deny(clippy::unwrap_used)`、`expect_used` 与 `panic`。请求路径上不允许
  panic。
- **Tracing：** 使用 `#[tracing::instrument(skip_all, err)]` 并显式指定 span `name` —— entities 用
  `Query:<Input>`（或 `Query-Transaction:<Input>`），services 用 `Service:<Input>`，hook processor 用
  `Hook:<Input>`。gRPC handler 不需要指定；trait 方法名本身就标识了 span。

## SurrealDB 注意事项

- 使用 `type::record(tb, id)`；旧的 `type::thing` 已在 3.x 中移除。
- 绝不要绑定名为 `token` 的变量 —— 它是保留字。
- 如果某个字段断言会读取某一行，请把这一行写在触发该断言的 `CREATE` *之后*；部分服务端版本无法通过
  `record::exists()` 看到同一事务内的 `UPDATE`。
