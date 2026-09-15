---
title: 本地开发
description: 在同一台机器上启动 SurrealDB、RabbitMQ、Redis、控制平面与控制台。
---

控制平面需要一个 SurrealDB 实例、一个 AMQP 消息代理和一个 Redis 服务端。其余部分全部从工作区直接运行。

:::caution[仓库根目录的 `.env` 不是开发配置]
根目录的 `.env` 可能保存着**生产环境**凭据（`SURREALDB_HOST`、`SURREALDB_USER`、
`SURREALDB_PASSWORD`、`SURREALDB_NAMESPACE`、`SURREALDB_NAME`、`AMQP_URI`、`REDIS_URL`），而你启动的
每个进程都会继承它。请显式传入数据库相关参数——或者覆盖这些变量——这样本地运行才不会意外连上远程数据库。
:::

## 1. 依赖组件

```sh
docker run -d --name guru-surreal -p 8000:8000 \
  surrealdb/surrealdb:latest start --user root --pass root

docker run -d --name guru-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-alpine

docker run -d --name guru-redis -p 6379:6379 redis:7-alpine \
  redis-server --save '' --appendonly no
```

请使用 **3.2 或更高版本**的 SurrealDB 服务端。更旧的 3.0 版本二进制与本工作区链接的客户端不兼容，
并且会错误处理那些读取同一事务中先前写入行的断言。

消息代理 URI 的写法很关键：默认 vhost 请使用 `amqp://guest:guest@127.0.0.1:5672/`。Redis 的 URL 是
`redis://127.0.0.1:6379/`；它只做 pub/sub——运维 API `Watch*` 流的实时事件都经由它，因此这里关掉了
持久化，重启它也不会丢掉任何需要保留的东西。

## 2. Schema

Schema 位于 `database/schema/*.surql`（每个模块一个文件），由
[surrealkit](https://surrealdb.com/) 负责管理：

```sh
surrealkit sync --host ws://127.0.0.1:8000 --ns guru --db guru
```

随后把各模块的默认配置写入 `app_config` 表。该操作是幂等的，且绝不会覆盖你手动修改过的值，
因此每次 sync 之后都可以重新执行：

```sh
cargo run -p manage-tool -- \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  config seed
```

`config list`、`config get <key>` 和 `config set <key> <json>` 可以查看和修改这些值；
master 进程会在重启时读取它们。参见
[配置 → 模块配置](/zh-cn/reference/configuration/#模块配置)。

## 3. 初始化管理员账号

```sh
cargo run -p manage-tool -- \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  create-admin --email admin@example.com --password 'change-me'
```

## 4. 控制平面

所有会访问数据库的运行模式都使用 `GURU_MASTER_KEY` 解密机密数据，而它没有默认值：
缺少它时进程会在启动阶段中止，并报出
`Error: "master key: GURU_MASTER_KEY is not set"`。只需生成一次——该子命令不需要数据库——
然后在启动 master 的那个 shell 中保留它：

```sh
cargo run -p manage-tool -- generate-master-key
# j7ILadgGjBy+jYMIJuiPBl5eai65t7G8G4XimNcyLpU=

export GURU_MASTER_KEY='<the printed value>'
```

每个模式都是一个独立进程。控制台所访问的运维 API 是 `dashboard_grpc`：

```sh
cargo run -p guru-master -- \
  --mode dashboard_grpc \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  --amqp-uri 'amqp://guest:guest@127.0.0.1:5672/' \
  --redis-url 'redis://127.0.0.1:6379/'
```

在 `consumer` 运行之前不会有任何画布被推导，因此请在第二个 shell 中启动一个——它既是编辑钩子，
也是所有周期任务的执行者（过期画布清扫、存活性清扫、健康数据保留、ACME 以及中继叶证书轮换）：

```sh
cargo run -p guru-master -- \
  --mode consumer \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  --amqp-uri 'amqp://guest:guest@127.0.0.1:5672/' \
  --redis-url 'redis://127.0.0.1:6379/'
```

第三个 shell 用来跑时钟。`cron` 只为每个到期任务发布一个执行信号，除此之外什么都不做：
它不会打开数据库连接，从不读取 `GURU_MASTER_KEY`，也不接受任何数据库参数，
因此消息代理 URI 就是它的全部配置：

```sh
env -u GURU_MASTER_KEY cargo run -p guru-master -- \
  --mode cron \
  --amqp-uri 'amqp://guest:guest@127.0.0.1:5672/'
```

`workers_grpc` 是第四种模式——即 Worker API 加上配置视图轮询器——它接受的参数与
`dashboard_grpc` 完全一致。所有模式都依赖消息代理：RabbitMQ 停机时什么都启动不了；
而 `cron` 或 `consumer` 停机时，任何周期任务都不会发生。除 `cron` 之外的每种模式还需要 Redis：
`REDIS_URL` 缺失或服务端连不上时它们会在启动阶段中止。它是运维 API `Watch*` 流背后的实时总线：
Redis 停机时，已打开的流会收不到消息，而编辑与派生一切照旧。控制台目前还不消费这些流，所以无论
它是否运行，浏览器里都看不出任何差别。

如果想让某个周期任务立即执行而不必等待它的时间间隔，删除它的声明行即可——对应的表是
`orchestration_job_run`，每个任务一行、以任务名作为键，所以执行
`DELETE orchestration_job_run:sweep_liveness` 就能让下一个 `sweep_liveness` 信号真正跑起来。

## 5. 控制台

```sh
bun install
GURU_GRPC_URL=127.0.0.1:50051 bun run dev
```

在工作区根目录执行 `bun run dev` 会代理到 `guru-frontend` 包。

## 6. 文档站点

这份文档本身也是工作区中的一个包：

```sh
bun run --filter guru-docs dev     # or: bun run docs:dev
bun run --filter guru-docs build   # static output in typescript/docs/dist
```

## 重新生成 API 客户端

`proto/` 是唯一的事实来源。修改之后需要重新生成 TypeScript 客户端（Rust 侧由
`rpguru_sdk` 的 `build.rs` 生成）：

```sh
bun run generate:proto
```

## 测试

模块集成测试运行在内存版 SurrealDB（`mem://`）上，并会应用模块自身的 schema 文件，
因此无需启动任何服务端：

```sh
cargo test
```
