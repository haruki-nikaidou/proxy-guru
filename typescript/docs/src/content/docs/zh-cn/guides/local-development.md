---
title: 本地开发
description: 在同一台机器上启动 PostgreSQL、RabbitMQ、Redis、控制平面与控制台。
---

控制平面需要一个 PostgreSQL 数据库、一个 AMQP 消息代理和一个 Redis 服务端。其余部分全部从工作区直接运行。

:::caution[仓库根目录的 `.env` 不是开发配置]
根目录的 `.env` 可能保存着**生产环境**凭据（`GURU_DATABASE_URL`、`AMQP_URI`、`REDIS_URL`），
而你启动的每个进程都会继承它。请显式传入 `--database-url`——或者覆盖这个变量——这样本地运行才不会
意外连上远程数据库。
:::

## 1. 依赖组件

```sh
docker run -d --name guru-postgres -p 15432:5432 \
  -e POSTGRES_USER=guru -e POSTGRES_PASSWORD=guru -e POSTGRES_DB=guru \
  postgres:18.6-alpine

docker run -d --name guru-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-alpine

docker run -d --name guru-redis -p 6379:6379 redis:7-alpine \
  redis-server --save '' --appendonly no
```

请使用 **16 或更高版本**的 PostgreSQL。

消息代理 URI 的写法很关键：默认 vhost 请使用 `amqp://guest:guest@127.0.0.1:5672/`。Redis 的 URL 是
`redis://127.0.0.1:6379/`；它只做 pub/sub——运维 API `Watch*` 流的实时事件都经由它，因此这里关掉了
持久化，重启它也不会丢掉任何需要保留的东西。如果本机 `6379` 已被别的程序占用，在
`docker compose up redis` 之前于 `.env` 中设置 `REDIS_PORT`（比如 `16379`）把宿主端口挪开，并在
`REDIS_URL` 里写同一个端口。

## 2. Schema

Schema 是 `migrations/` 下的一组 sqlx migration，它们被嵌入到二进制文件里。
`guru-master` 启动时会应用所有尚未应用的 migration；如果要手动执行：

```sh
export GURU_DATABASE_URL=postgres://guru:guru@127.0.0.1:15432/guru
cargo run -p manage-tool -- db migrate
```

随后把各模块的默认配置写入 `app_config` 表。该操作是幂等的，且绝不会覆盖你手动修改过的值，
因此每次 migration 之后都可以重新执行：

```sh
cargo run -p manage-tool -- config seed
```

`config list`、`config get <key>` 和 `config set <key> <json>` 可以查看和修改这些值；
master 进程会在重启时读取它们。参见
[配置 → 模块配置](/zh-cn/reference/configuration/#模块配置)。

## 3. 初始化管理员账号

```sh
cargo run -p manage-tool -- --database-url "$GURU_DATABASE_URL" \
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
  --database-url "$GURU_DATABASE_URL" \
  --amqp-uri 'amqp://guest:guest@127.0.0.1:5672/' \
  --redis-url 'redis://127.0.0.1:6379/'
```

在 `consumer` 运行之前不会有任何画布被推导，因此请在第二个 shell 中启动一个——它既是编辑钩子，
也是所有周期任务的执行者（过期画布清扫、存活性清扫、健康数据保留、ACME 以及中继叶证书轮换）：

```sh
cargo run -p guru-master -- \
  --mode consumer \
  --database-url "$GURU_DATABASE_URL" \
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

通知由它自己的模式投递，而这种模式恰好只有一个实例。它既不需要 master key 也不需要 Redis，而且只有
在某个画布或某个账号已经有通知设置之后才会真正做事（见[通知](/zh-cn/features/notifications/)）：

```sh
env -u GURU_MASTER_KEY GURU_TELEGRAM_BOT_TOKEN=... cargo run -p guru-master -- \
  --mode notifier \
  --database-url "$GURU_DATABASE_URL" \
  --amqp-uri 'amqp://guest:guest@127.0.0.1:5672/'
```

第二个 `notifier` 会拒绝启动 —— `another notifier already holds the advisory lock; run exactly one`
—— 这是那把锁在起作用，不是 bug。要收邮件，就把 `notify` 的 `smtp_host` 指向一个本地接收端
（`docker run -d -p 1025:1025 -p 8025:8025 axllent/mailpit`，然后
`manage-tool config set notify '{"smtp_host":"127.0.0.1","smtp_port":1025,"smtp_starttls":false}'`），
再到 `http://127.0.0.1:8025` 查看收到了什么。

`workers_grpc` 是第五种模式——即 Worker API 加上配置视图轮询器——它接受的参数与
`dashboard_grpc` 完全一致。所有模式都依赖消息代理：RabbitMQ 停机时什么都启动不了；
而 `cron` 或 `consumer` 停机时，任何周期任务都不会发生。`--redis-url`（或 `REDIS_URL`）是上面那三种
提供服务与派生的模式所必需的，`cron` 和 `notifier` 都不使用它；缺失或服务端连不上时它们会在启动阶段
中止。它是运维 API `Watch*` 流背后的实时总线，
控制台的画布编辑器和健康状况页面跟随的正是这些流：Redis 停机时，已打开的页面会停止更新（它的
*实时* 徽章仍保持绿色——那个徽章表示的是浏览器自己的连接），而编辑与派生一切照旧；Redis 恢复后，
页面会自行追上最新状态。

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

## 修改查询之后

所有语句都走 sqlx 的 `query!` 宏族，因此 SQL 在 crate 编译期就会按真实 schema 校验。普通构建不需要
数据库：它读取提交在 `.sqlx/` 里的离线数据。新增或修改语句（或 migration）之后，请对一个已迁移的数据库
重新生成这份数据并提交，否则没有 `DATABASE_URL` 的构建仍然只能看到旧的查询集合：

```sh
export DATABASE_URL=postgres://guru:guru@127.0.0.1:15432/guru_dev
# manage-tool 读的是 GURU_DATABASE_URL，不是 DATABASE_URL —— 显式传进去
cargo run -p manage-tool -- --database-url "$DATABASE_URL" db migrate
cargo sqlx prepare --workspace -- --all-targets   # 重写 .sqlx/
```

`cargo sqlx prepare` 需要 `sqlx-cli`（`cargo install sqlx-cli --no-default-features --features
postgres,rustls`）。`-- --all-targets` 不能省：少了它，测试自身的语句就不会写进 `.sqlx/`，离线的
`cargo test` 会在那里失败。一旦 export 了 `DATABASE_URL`，宏就会直接连那个数据库，`.sqlx/` 会被忽略
—— 这也正是重新生成的数据必须单独提交的原因。

## 测试

模块集成测试运行在一个真实的 PostgreSQL 服务端上：`#[sqlx::test]` 会基于 `DATABASE_URL`
为每个测试创建一个用完即弃的数据库，并对它应用全部 migration。请把这个变量指向一个**测试**
数据库，绝不要指向某个 master 正在使用的库——测试运行器会在它旁边不断创建和删除数据库：

```sh
export DATABASE_URL=postgres://guru:guru@127.0.0.1:15432/guru_test
SQLX_OFFLINE=true cargo test
```

用 `createdb`（或 `CREATE DATABASE guru_test;`）创建它一次即可；对应的角色需要 `CREATEDB` 权限。
运行器会为每个测试在它旁边新建一个库并对其应用 migration，所以 `DATABASE_URL` 指向的那个库本身并不需要
schema —— 但 `query!` 宏会试着拿它来校验每条语句，并因为表不存在而失败。`SQLX_OFFLINE=true` 让宏改去读
`.sqlx/`，这就把 `DATABASE_URL` 的两种身份分开了。（给那个库跑一次 migration 也可以。）
