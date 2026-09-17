---
title: 使用 Docker 部署
description: 从 GHCR 镜像运行控制平面，应用 Schema，搭建 PostgreSQL、RabbitMQ 与 Redis，并从 GitHub release 分发 worker 二进制文件。
---

本指南带你把单机生产部署从一台空机器一路做到可用的控制台。它假设你熟悉 Linux、Docker 和反向代理，
但从未接触过这个项目 —— 也从未见过类似架构。

有意不在范围内的内容：在数据平面节点上安装 worker 并把它注册到控制平面。本指南只负责把二进制文件放到一台
你可以从中分发它的机器上。

## 1. 你要部署的是什么

四个进程，全部来自**同一个**镜像，再加上控制台：

| 组件 | 运行模式 | 通信对象 |
|---|---|---|
| 运维 API | `dashboard_grpc` | PostgreSQL、RabbitMQ、Redis |
| Worker API | `workers_grpc` | PostgreSQL、RabbitMQ、Redis |
| 周期任务 + 派生钩子 | `consumer` | PostgreSQL、RabbitMQ、Redis |
| 调度器 | `cron` | RabbitMQ |
| 控制台 | — | 运维 API（gRPC） |

:::note
由于 TCP 反向代理服务器的特性，将 Worker 部署在 Docker 容器内并不是良好实践。
因此，我们不提供 Worker 节点的 Docker 镜像。
:::

状态只存在两个地方：**PostgreSQL**（画布、服务器、Pod 图、账号、配置视图）和 **RabbitMQ**
（一个持久队列承载"这个画布变了"的提示，外加每个周期任务一个队列）。容器文件系统上不保存任何东西，
所以每个容器都是可丢弃的。**Redis** 也是必需的，但它不持有状态：它在单一 pub/sub 频道上，把运维 API
的实时事件在各个 master 副本之间传递，并且不配置任何持久化，重启它最多丢掉正在路上的那几个事件。

这两种钩子模式的分工，是你在做容量规划之前就应该理解的。`cron` 是一只时钟：它为每个到期任务发布一条执行
信号，完全不打开数据库连接。`consumer` 才真正干活 —— 派生钩子*以及*所有周期任务 —— 因此清扫、存活检测和
证书续期的扩缩容与故障转移方式，与一次画布编辑完全一致。

端口，以及谁有权访问它们：

| 端口 | 进程 | 暴露范围 |
|---|---|---|
| `50051` | `dashboard_grpc` | **私有。** 仅供控制台访问；明文 h2c，无 TLS，传输层无认证。 |
| `50052` | `workers_grpc` | 数据平面节点可访问（VPN、私有网络，或一个终结 TLS 的 gRPC 代理）。 |
| `3000` | 控制台 | 放在你的 HTTPS 反向代理之后；绝不要直接对外暴露。 |
| `5432` | PostgreSQL | **私有。** 一个角色、一个数据库、一个密码。 |
| `5672` | RabbitMQ | **私有。** |
| `6379` | Redis | **私有。** 只承载 pub/sub；其中没有任何持久状态。 |

:::caution[两个 gRPC 端口都是明文的]
两种 master 模式都以明文 HTTP/2 提供服务，而控制台用 `ChannelCredentials.createInsecure()` 打开通道。
请把 `50051` 留在私有网络里（Docker 网络、loopback 绑定或 VPN），并且把 `50052` 视为一条在跨越公网时
需要自备传输层安全的链路。
:::

## 2. 前置条件

先完成 **[前置条件](/zh-cn/guides/prerequisites/)**，再来看本指南。对于镜像部署，你需要那一页中的：
Docker Engine 与 Compose 插件、在运维机器上检出一份本仓库（用于取得 Compose 文件）、`openssl`、
一个带 TLS 证书的 DNS 名称 —— 以及 PostgreSQL、RabbitMQ 和 Redis 本身，那一页会用
`/srv/guru/docker-compose.yml` 把它们拉起来，凭据放在 `/srv/guru/.env` 中。

`manage-tool` CLI 同样不在镜像里，但它不必自己构建：它和 `guru-master` 一样，会作为原始二进制文件随每个
`master-v*` release 发布（第 10 节）。因此 Rust 工具链、`protobuf-compiler`、C 工具链和 `cmake` 只有在你
**选择自行构建**它时才需要 —— `manage-tool` 会引入证书相关的依赖栈，其 crate 需要编译内置的 C 源码。
Schema 随二进制一起分发，因此应用它不需要从检出目录里复制任何东西。
Bun 在这里可以跳过 —— 控制台以镜像形式发布。`perl` 只在构建 `guru-worker` 的地方才需要，而那不是这里。

## 3. 选定版本

只有推送 tag 才会触发发布；镜像标签就是 git tag 去掉其组件前缀后的部分。

| Git tag | 发布产物 |
|---|---|
| `master-v0.1.0[-alpha]` | `ghcr.io/haruki-nikaidou/guru-master:v0.1.0[-alpha]`，外加一个携带 `guru-master` 与 `manage-tool` 原始二进制文件（均为 `x86_64-unknown-linux-gnu`）的 GitHub release |
| `frontend-v0.1.0[-alpha]` | `ghcr.io/haruki-nikaidou/guru-frontend:v0.1.0[-alpha]` |
| `worker-v0.1.0[-alpha]` | 携带两个 `linux/x86_64` 原始 `guru-worker` 二进制文件的 GitHub release：一个链接 glibc，一个静态链接 musl |

`latest` 只会随正式的 `vX.Y.Z` 移动，绝不会指向预发布版本。无论如何都请在 Compose 文件里
**固定一个明确的标签**：`latest` 让你无从得知正在运行的是哪个修订版本，而 master 与 Schema 是一起演进的。

两个镜像都是公开的，所以拉取时不需要 `docker login ghcr.io`。

## 4. 安排好密钥

[前置条件](/zh-cn/guides/prerequisites/)已经在 Compose 文件旁创建了 `/srv/guru/.env`，其中包含数据存储
凭据（`POSTGRES_USER`、`POSTGRES_PASSWORD`、`POSTGRES_DB`、`RABBIT_USER`、`RABBIT_PASSWORD`）。
把你在第 3 节中固定的镜像标签追加进去：

```sh
# /srv/guru/.env  (append)
# The master and the frontend are tagged and released independently; pin each one.
MASTER_VERSION=v0.3.0-beta
FRONTEND_VERSION=v0.2.0-beta
```

`GURU_MASTER_KEY` 会在第 7 节加入同一个文件，那时 `manage-tool` 已经可以打印一个出来。

:::caution[仓库里的 `.env` 是另一个文件]
仓库根目录下可能有一个 `.env`，里面是*另一套*环境的凭据（`GURU_DATABASE_URL`、`AMQP_URI`），而从该
目录启动的每个进程都会继承它。在检出目录里运行 `manage-tool` 时，请显式传入 `--database-url`。
一个被遗漏的参数，就是"本地"命令最终改写生产库的原因。
:::

## 5. PostgreSQL、RabbitMQ 与 Redis

三个数据存储、它们的 Compose 服务以及背后的要求（PostgreSQL ≥ 16，带持久化存储；
RabbitMQ 使用默认 vhost，URI 以斜杠结尾；Redis 7.x，只做 pub/sub，不需要任何持久化）都在
**[前置条件 → PostgreSQL、RabbitMQ 与 Redis](/zh-cn/guides/prerequisites/#4-postgresqlrabbitmq-与-redis)** 中。
它们必须在任何 master 启动之前就位：

```sh
cd /srv/guru
docker compose ps          # postgres healthy, rabbitmq healthy, redis up
```

有三个后果值得在这里重复一遍，因为它们塑造了整个部署形态：在**全部四种** master 模式下消息中间件都是
必需的 —— 周期性工作就是一条消息，所以 broker 中断会让派生、存活检测和证书续期一起停摆；master
用 `/srv/guru/.env` 里的角色连接数据库，第 7 节中的 `x-master` 锚点会把它拼成一个 URL；
而 Redis 在会打开数据库连接的那三种模式下必填（`cron` 不用它），丢掉它的代价也小得多。
中断只会让已打开的 `Watch*` 流停止投递，除此之外别无影响 —— 编辑照样生效，画布照样派生，Worker 照样
拿到自己的配置 —— 而且订阅端会自行重连，之后让每个 watcher 重新读一遍数据库。控制台跟随这些流，
因此 Redis 停机期间，已打开的画布或健康状况页面会停止更新，并在它恢复后自行追上最新状态；这些页面上的
*实时* 徽章表示的是浏览器自己的连接，此期间会一直保持绿色。

## 6. 应用 Schema

每个 master 启动时都会应用尚未应用的 migration，所以首次安装时这一节是可选的 —— 但先执行一遍，
可以让 Schema 上的问题在这里暴露，而不是在四个不断重启的容器里暴露。
按 **[配置数据库 Schema](/zh-cn/guides/setup-database-schema/)** 操作：

```sh
read -rs GURU_DATABASE_URL           # paste the URL, it is not echoed
export GURU_DATABASE_URL
./manage-tool db migrate
```

如果数据库在服务器上只监听 loopback，请建立隧道：
`ssh -N -L 5432:127.0.0.1:5432 guru-host`。

## 7. 运行控制平面

`guru-master` 的*部署*类设置来自环境变量：`GURU_WORKER_MODE` 选择运行模式，而
`GURU_DATABASE_URL`、`AMQP_URI`、`REDIS_URL` 和 `GURU_MASTER_KEY` **没有默认值**。
所有由运维按安装实例调优的项 —— 健康阈值与保留期、默认的 ACME 目录、续期窗口、各个周期任务的运行频率 ——
都改为存放在数据库里（第 8 节），因此副本之间不需要保持环境变量一致。

master key 只生成一次，并与数据库凭据一起保管 —— 它在静态存储层面加密每一个 DNS provider token 和证书
私钥，没有它就无法恢复这些数据。三种需要读取密钥的模式都必须有它；`cron` 从不读取，而下面的锚点只是把
同一份环境变量交给全部四个服务。`manage-tool` 可以从某个 `master-v*` release 下载（第 10 节），也可以
从你的检出目录构建（第 8 节）；这个子命令不需要数据库：

```sh
./target/release/manage-tool generate-master-key
```

扩展同一个 `docker-compose.yml`：`x-master` 锚点放在 `services:` 上方，四个服务放在其中，与 `postgres`、
`rabbitmq` 和 `redis` 并列：

```yaml
x-master: &master
  image: ghcr.io/haruki-nikaidou/guru-master:${MASTER_VERSION}
  restart: unless-stopped
  environment: &master-env
    GURU_DATABASE_URL: postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
    AMQP_URI: amqp://${RABBIT_USER}:${RABBIT_PASSWORD}@rabbitmq:5672/
    REDIS_URL: redis://redis:6379/
    GURU_MASTER_KEY: ${GURU_MASTER_KEY}
    GURU_LOG_LEVEL: info
  depends_on:
    postgres:
      condition: service_healthy
    rabbitmq:
      condition: service_healthy
    redis:
      condition: service_started

services:
  # ... postgres, rabbitmq and redis from section 5 ...

  master-dashboard:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: dashboard_grpc
    # No `ports`: only the dashboard container reaches :50051, over this network.

  master-workers:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: workers_grpc
    # Loopback only: nginx terminates TLS for the worker API on :443 (section 9).
    ports:
      - "127.0.0.1:50052:50052"

  master-consumer:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: consumer

  master-cron:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: cron
```

每种模式的用途，以及它如何扩缩容：

- **`dashboard_grpc`** —— 运维 API（`Auth` + `Orchestration`），监听 `GURU_DASHBOARD_GRPC_ADDR`
  （`0.0.0.0:50051`）。无状态；可以在一个支持 gRPC 的负载均衡器后面任意复制。
- **`workers_grpc`** —— worker API（`WorkerAgent`），监听 `GURU_WORKERS_GRPC_ADDR`
  （`0.0.0.0:50052`），外加唤醒 worker 流的配置视图轮询器
  （`GURU_WATCH_POLL_MS`，默认 `1000`）。可以复制，但每个 worker 会话都固定在持有其流的那个实例上，
  所以前面要放一个纯 TCP/gRPC 负载均衡器，绝不能用 HTTP/1 代理。
- **`consumer`** —— 所有钩子都在这里：收到 `CanvasDirty` 消息时重新派生一个画布（prefetch 为 8），
  并在六个周期任务的执行信号到达时运行它们 —— 过期画布清扫、健康存活检测清扫、健康数据保留清理、
  ACME 签发/续期、relay 叶子证书轮换，以及新服务器地址的国家查询。这也是需要出站 HTTPS 访问 ACME
  目录、DNS provider API 和 `country_lookup_url`、以及需要 DNS 访问公共解析器的模式。为了吞吐和故障
  转移可以复制它：派生由画布代数计数器守护，而每个周期任务在开始工作前都会在一行
  `orchestration_job_run` 中认领本次运行，因此一条被投递两次、或被投递给两个副本的信号只会执行一次。
  两个 ACME 任务也不会争抢同一个 DNS-01 挑战 —— 每次证书尝试都按行认领。
- **`cron`** —— 只是时钟，仅此而已。它每 5 秒扫描一次，为每个到期任务发布一条执行信号：
  `derive_stale_canvases` 与 `sweep_liveness` 每 30 秒，`renew_certificates` 与
  `resolve_server_countries` 每 60 秒，`trim_health_history` 每 5 分钟，`rotate_relay_certificates`
  每小时。它不打开数据库连接，从不读取 `GURU_MASTER_KEY`，也不保存任何本地状态，因此它持有的唯一机密
  就是 `AMQP_URI` 里的 broker 凭据 —— 那也是它唯一离不开的东西。这里没有什么需要扩容的：一个副本就够了，
  多一个也无害，因为 consumer 的运行认领会丢弃重复的那一条。一个任务实际允许多久运行一次是存储在
  数据库里的设置，不是启动参数 —— 见第 8 节。

基于 TLS 或 QUIC 的 relay 链路在其 pod 派生之前需要内部 CA。请在运维机器上执行一次下面的命令，
使用与 master 相同的 `GURU_MASTER_KEY`：

```sh
GURU_MASTER_KEY='<the key>' ./target/release/manage-tool \
  --database-url "$GURU_DATABASE_URL" orchestration init-ca
```

它会打印 CA 证书，并把每个包含 TLS/QUIC relay 的画布标记为需要重新派生。它拒绝被执行第二次。

从代码中可以直接得出的三条运维注意事项：

- `consumer` 和 `cron` 模式在 **AMQP 连接断开时会以非零码退出**（客户端不会重连，而一个静默死掉的
  consumer、或者一个把消息发到虚无处的时钟，比重启更糟）：
  `the AMQP connection was lost: restart once the broker at AMQP_URI is reachable again`。
  `restart: unless-stopped` 正是让它自愈的机制 —— 不要移除它。
- **Redis 里没有任何需要保护的状态。** 它的 Compose 服务以 `--save "" --appendonly no` 运行，因为经过
  它的只有实时事件；重启它最多让已打开的 `Watch*` 流错过几次投递，订阅端每次重连都会打印一行
  `live bus connected`，随后让每个 watcher 重新读一遍数据库，所以不会有内容停留在旧状态。但它在启动时
  必须可达：`REDIS_URL` 未设置或服务端连不上时，三种打开数据库连接的模式都会立即退出。
- 镜像是 distroless 的：没有 shell，没有 `curl`。依赖调用 shell 的 Compose `healthcheck` 无法工作。
  请改从外部监控（TCP 连接 `50051`/`50052`，或采集日志）。

启动它们：

```sh
docker compose up -d
docker compose logs master-dashboard master-workers master-consumer master-cron
```

健康的启动看起来是这样。consumer 为它绑定的每个队列打印一行，调度器打印一行列出它将使用的所有节奏：

```text
master-dashboard-1  | INFO guru_master: serving operator API addr=0.0.0.0:50051
master-workers-1    | INFO guru_master: serving worker API addr=0.0.0.0:50052
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_canvas_dirty" key="canvas_dirty"
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_derive_stale_canvases" key="derive_stale_canvases"
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_rotate_relay_certificates" key="rotate_relay_certificates"
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_sweep_liveness" key="sweep_liveness"
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_trim_health_history" key="trim_health_history"
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_renew_certificates" key="renew_certificates"
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_resolve_server_countries" key="resolve_server_countries"
master-cron-1       | INFO guru_master: scheduling periodic execution signals scan_interval_secs=5 derive_stale_canvases_secs=30 rotate_relay_certificates_secs=3600 sweep_liveness_secs=30 trim_health_history_secs=300 renew_certificates_secs=60 resolve_server_countries_secs=60
```

此后调度器是安静的：每次发布都记在 `DEBUG` 级别
（`published an execution signal job="sweep_liveness"`），而发布失败会是 `ERROR`，并且不会终止进程。
所以在 cron 容器上设置 `GURU_LOG_LEVEL=debug` 就是确认时钟在走的方法，而一条
`ERROR ... publishing an execution signal failed` 则是 broker 问题在连接被判定为丢失之前的表现形式。

## 8. 创建第一个管理员

这里没有自助注册：第一个账号是用 `manage-tool` 直接对着数据库创建的，它故意绕过了 RBAC，
因为此时还不存在任何管理员。它不在镜像里，所以请从某个 `master-v*` release 下载它（第 10 节），
或者从你的检出目录构建：

```sh
cd ~/proxy-guru
cargo build --release -p manage-tool

./target/release/manage-tool --database-url "$GURU_DATABASE_URL" \
  create-admin --email admin@example.com --password '<strong password>'
# Created admin account uz0ih3b30nrekqzs1h1y
```

请显式传入 `--database-url` —— `manage-tool` 同样会从环境变量读取 `GURU_DATABASE_URL`，所以一个多余的
`.env` 会悄悄把命令重定向到别处。

### 初始化模块配置

由运维调优的设置存放在 `app_config` 表中，每个键一行，而初始化这一步属于
[配置数据库 Schema → 初始化模块配置](/zh-cn/guides/setup-database-schema/#3-初始化模块配置) ——
如果你在那里跳过了，现在就执行：

```sh
./manage-tool --database-url "$GURU_DATABASE_URL" config seed
# seeded auth
# seeded orchestration
```

`config list` 打印所有已存储的文档，`config set <key> <json>` 替换某一个键 —— 例如把 ACME 续期窗口
设为两周：

```sh
./target/release/manage-tool ... config set orchestration '{"acme_renew_before_secs":1209600}'
```

一次 `set` 在写入前会按该配置的类型做校验，并且会替换整个文档，未指定的字段取各自的默认值。master 只在
启动时读取一次这些键，所以要让改动生效必须重启它们。参见
[配置 → 模块配置](/zh-cn/reference/configuration#模块配置)。

周期任务的节奏也在同一个键上：`sweep_interval_secs`（30）、`liveness_interval_secs`（30）、
`health_retention_interval_secs`（300）、`acme_interval_secs`（60）、
`country_lookup_interval_secs`（60）和 `relay_rotation_interval_secs`（3600）。调度器以固定节奏发布，
因为它不读取任何配置；每个 consumer 在每个*配置的*间隔内最多认领一次运行，所以一个小于或等于信号
节奏的值意味着"每条信号都执行"，而更大的值会让该任务在整个集群范围内变慢：

```sh
./target/release/manage-tool ... config set orchestration '{"acme_interval_secs":300}'
```

同一个二进制文件还有 `orchestration export-config --server <key>`，它会打印画布当前为某台服务器派生出的
worker TOML。当某个节点的行为与画布看起来不一致时，就该拿出这个工具。

## 9. 运行控制台

控制台是一个跑在 Node adapter 上的 SvelteKit 应用。它监听 `:3000`，并通过 `GURU_GRPC_URL` 访问控制平面。
在同一个文件里再加一个服务：

```yaml
  frontend:
    image: ghcr.io/haruki-nikaidou/guru-frontend:${FRONTEND_VERSION}
    restart: unless-stopped
    environment:
      GURU_GRPC_URL: master-dashboard:50051
      PROTOCOL_HEADER: x-forwarded-proto
      HOST_HEADER: x-forwarded-host
    ports:
      - "127.0.0.1:3000:3000"
    depends_on:
      - master-dashboard
```

:::danger[用 HTTPS 提供服务，并转发协议头]
这是导致"控制台能打开但无法登录"最常见的原因。

这个应用从不信任它所监听的套接字。对每个请求，它都从请求头重建自己的 origin，并且在 `PROTOCOL_HEADER`
未设置时**把协议默认为 `https`**。只要浏览器的 `Origin` 头与重建出的 origin 不一致，它的登录
（一个 SvelteKit *remote function*，即一次 POST）就会被以
`403 {"message":"Cross-site remote requests are forbidden"}` 拒绝。因此：

- **位于保留 `Host` 的 HTTPS 代理之后：** 无需额外配置即可工作 —— 协议默认为 `https`，主机来自 `Host`。
- **其他任何情况（纯 HTTP、不同的公开主机或端口）：** 设置
  `PROTOCOL_HEADER=x-forwarded-proto` 和 `HOST_HEADER=x-forwarded-host`，并让代理同时发送这两个头。
  如果公开 URL 使用了非默认端口，`X-Forwarded-Host` **必须包含端口** ——
  nginx 的 `$host` 会丢掉它，请用 `$http_host`。
- `ORIGIN` 不起任何作用。这个构建中的 Node adapter 在构建期就把 `kit.paths.origin` 的值烧了进去；
  运行时变量会被忽略。

无论哪种情况，HTTPS 都不是可选项：会话 cookie（`guru_session`，一份面向控制平面的完整 bearer 凭据）
是带 `Secure` 签发的，所以除 `localhost` 之外，浏览器在纯 HTTP 下都会丢弃它。
:::

下面这个 server 块把控制台、worker API 和 agent 下载目录放在同一个主机名下。gRPC 没有自己的路径前缀：
每次 worker 调用都是 `POST /guru.orchestration.agent.WorkerAgent/<Method>`，所以一个匹配该前缀的
`location` 把这些请求交给 `:50052`，其余一切交给控制台。`/agent/` 这个 location 提供的是
`manage-tool agent publish` 写出的内容（[安装与更新 Agent](/zh-cn/guides/agent-install/)）；
请在第一次 publish 之前先创建该目录（`sudo mkdir -p /srv/guru/agent`）。控制台和 worker API 也可以
分别放在两个主机名下 —— 两个 server 块，各自持有自己名字的证书 —— 只要 `agent_public_base_url`
指向 worker 相关 location 所在的那一个即可。

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}

server {
    listen 443 ssl so_keepalive=60s:15s:4;
    listen [::]:443 ssl so_keepalive=60s:15s:4;
    http2 on;                 # nginx ≥ 1.25.1; older builds: `listen 443 ssl http2;`
    server_name guru.example.com;

    ssl_certificate     /etc/letsencrypt/live/guru.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/guru.example.com/privkey.pem;

    # ---- worker agent API: TLS terminated here, plaintext h2c to master-workers.
    location ^~ /guru.orchestration.agent.WorkerAgent/ {
        grpc_pass grpc://127.0.0.1:50052;
        grpc_connect_timeout 5s;
        grpc_read_timeout 7d;     # WatchConfig may be silent for hours
        grpc_send_timeout 7d;
        grpc_socket_keepalive on;
        client_max_body_size 0;   # ReportHealth is one body that grows for the session's life
        client_body_timeout 60s;  # four missed health reports
        grpc_set_header x-api-key     $http_x_api_key;
        grpc_set_header x-refresh-key $http_x_refresh_key;
        grpc_set_header X-Real-IP     $remote_addr;
    }

    # ---- agent binaries, installer, unit and start guard.
    location ^~ /agent/ {
        alias /srv/guru/agent/;
        autoindex off;
        default_type application/octet-stream;
        add_header Cache-Control "public, max-age=300";
    }

    # ---- dashboard, with the two headers the app needs.
    location ^~ / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-Host  $http_host;   # $host drops the port
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header Upgrade           $http_upgrade;
        proxy_set_header Connection        $connection_upgrade;
        proxy_buffering off;      # SvelteKit streams responses
        proxy_read_timeout 3600s;
    }
}
```

worker location 里的每一项设置都在
[原生部署 → nginx](/zh-cn/guides/deploy-natively/#7-nginx把控制台和-worker-api-放在同一个主机名下)
中逐条解释；按 nginx 默认的 60 秒超时，每个 worker 每分钟都会重新注册一次。另外两个有用的变量：
如果你想拿到真实客户端 IP，可以设置 `ADDRESS_HEADER=x-forwarded-for`；如果你会导入非常大的画布，
可以调整 `BODY_SIZE_LIMIT`（默认 `512K`）。

现在打开 `https://guru.example.com/`，它会重定向到 `/auth`，用第 8 节创建的账号登录。
你应该会看到画布列表，侧边栏里显示你的邮箱。

## 10. 从 GitHub release 获取 master 二进制文件

除了 GHCR 镜像之外，`master-v*` tag 还会发布一个名为 `guru-master <version>` 的 GitHub release，
其中携带两个原始二进制文件 —— 控制平面本身，以及管理 CLI：

| 产物 | 是什么 |
|---|---|
| `guru-master-<version>-x86_64-unknown-linux-gnu` | 与镜像里同一个控制平面二进制，供不走 Docker 的部署使用 |
| `manage-tool-<version>-x86_64-unknown-linux-gnu` | 管理 CLI：`generate-master-key`、`create-admin`、`config`、`orchestration` |

`<version>` 是 tag 去掉 `master-` 前缀后的部分 —— 所以 tag `master-v0.3.0` 发布的是
`guru-master-v0.3.0-x86_64-unknown-linux-gnu` 和 `manage-tool-v0.3.0-x86_64-unknown-linux-gnu`。
两个产物都只有 `x86_64-unknown-linux-gnu` 一种形态：它们链接 glibc，与 `distroless/cc` 镜像的 ABI 一致。

:::caution[只有在工作流之后推送的 tag 才有产物]
*Release Master* 工作流比最早的 master tag 更晚出现：在它之前的 `master-v*` tag（撰写本文时是
`master-v0.0.1-alpha`、`master-v0.1.0-alpha`、`master-v0.2.0-beta`）只有镜像，没有 release 产物。
如果你固定的那个 tag 没有产物，请改用更新的 tag，或者从检出目录构建 `manage-tool`（第 8 节）；
有没有产物可以用下一节的 `jq` 片段确认，只要把 `startswith("worker-")` 换成 `startswith("master-")`。
:::

```sh
VERSION=v0.3.0                       # 要挑一个确实列出了产物的 release
gh release download "master-${VERSION}" \
  --repo haruki-nikaidou/proxy-guru \
  --pattern "manage-tool-*-x86_64-unknown-linux-gnu" \
  --output manage-tool
chmod +x manage-tool
./manage-tool --help
```

:::note[下载来的 `manage-tool` 与自行构建的那一个完全等价]
本页中每一条 `./target/release/manage-tool …` 命令都可以原样换成下载来的二进制（例如 `./manage-tool …`）：
参数、环境变量和输出都一样。请让它的版本与你正在运行的 master 标签一致 —— CLI 与 master 共用同一套
Schema 与配置类型。
:::

Schema 不需要单独的产物：migration 已经编译进两个二进制文件里，所以用下载来的二进制执行
`manage-tool db migrate`，应用的内容与相同版本的 master 完全一致。

## 11. 从 GitHub release 获取 worker 二进制文件

数据平面以原始二进制文件而不是镜像的形式发布，并且只发布 `linux/x86_64`。每个 `worker-<version>`
release 携带两个产物，其中 `<version>` 是 tag 去掉 `worker-` 前缀后的部分：

| 产物 | 适用的主机 |
|---|---|
| `guru-worker-<version>-x86_64-unknown-linux-gnu` | 使用 glibc 的发行版（Debian/Ubuntu/RHEL）；动态链接宿主的 glibc |
| `guru-worker-<version>-x86_64-unknown-linux-musl` | Alpine 以及任何其他 musl 发行版；静态链接 musl libc（static-pie），运行时不依赖宿主上的 libc |

所以 tag `worker-v0.1.0` 发布的是 `guru-worker-v0.1.0-x86_64-unknown-linux-gnu` 和
`guru-worker-v0.1.0-x86_64-unknown-linux-musl`。

怎么选：目标主机用 glibc 就取 `-gnu`，用 musl（Alpine 及其同类）就取 `-musl`。musl 那一份把 musl libc 静态
链接进了可执行文件，运行时不需要宿主安装任何 libc —— 在一个连 glibc 都没有装的 `alpine:3` 容器里也能直接
跑起来，所以它在两类主机上都能用；如果你不确定，或者想用同一个产物覆盖混合机群，就选它。手上拿到的是
哪一个，`ldd` 会告诉你：`-gnu` 会列出 `libc.so.6`，`-musl` 会回答 `not a dynamic executable`。

请挑一个确实列出了这些产物的 release，在围绕它写任何脚本之前先检查一下：

```sh
curl -fsSL https://api.github.com/repos/haruki-nikaidou/proxy-guru/releases \
  | jq -r '.[] | select(.tag_name | startswith("worker-"))
           | .tag_name + " -> " + ((.assets | map(.name)) | join(", "))'
```

:::caution[并非每个 tag 都有二进制文件]
如果某个 `worker-v*` tag 的 release 没有列出任何产物，说明它是在发布工作流存在之前打的（撰写本文时，
`worker-v0.0.1-alpha` 正处于这种状态：`assets: []`）。这样的 tag 没有任何可下载的东西 ——
请改用更新的 release，或者推送一个新的 `worker-v*` tag，让 *Release Worker* 工作流构建并附加二进制文件。
同样地，只列出一个 `-gnu` 产物的 release 早于 musl 构建被加入发布工作流；musl 那一份只能从更新的 tag 获得。
:::

使用 GitHub CLI。请用一个 `TARGET` 变量把产物钉死：一个同时匹配两者的 `--pattern` 会把它们一起下载，
于是 `--output` 会变得没有意义。

```sh
VERSION=v0.1.0
TARGET=x86_64-unknown-linux-gnu        # Alpine/musl 主机：x86_64-unknown-linux-musl
gh release download "worker-${VERSION}" \
  --repo haruki-nikaidou/proxy-guru \
  --pattern "guru-worker-*-${TARGET}" \
  --output guru-worker
```

或者用纯 `curl` —— 通过 API 解析产物地址，这样你就不用把 URL 写死。这里按产物名完整相等来选，
而不是用后缀匹配，因为这样无论 release 里有多少个目标，命中的都只会是一个：

```sh
VERSION=v0.1.0
TARGET=x86_64-unknown-linux-musl       # glibc 主机：x86_64-unknown-linux-gnu
url=$(curl -fsSL \
  "https://api.github.com/repos/haruki-nikaidou/proxy-guru/releases/tags/worker-${VERSION}" \
  | jq -r --arg name "guru-worker-${VERSION}-${TARGET}" \
      '.assets[] | select(.name == $name) | .browser_download_url')
curl -fsSL "$url" -o guru-worker
```

然后赋予可执行权限并确认它能运行 —— 这个二进制会打印自己的版本号，那也正是 `manage-tool agent publish`
所记录的版本：

```sh
chmod +x guru-worker
./guru-worker --version
# guru-worker 0.4.0-beta
```

请把这些二进制文件按版本存放在你自己的产物库中（内部 HTTP 服务器、apt/OCI registry，或你的配置管理系统）。
既没有 `latest` 别名，也没有发布校验和文件，所以请把版本号和目标三元组 —— 最好还有你自己算的
`sha256sum` —— 与你分发的副本一起记录下来。

两个产物都在 GitHub runner（`ubuntu-latest`）上构建，功能完全相同，差别只在 libc：`-gnu` 那一份链接
宿主的 glibc，因此需要一台 glibc 足够新的主机（当前的 Debian/Ubuntu/RHEL 都可以）；`-musl` 那一份把 libc
静态链接进了自身，运行时不依赖宿主的任何共享库，因此发行版是什么都无所谓。

在这套控制平面上安装并注册一个 worker 节点是另一篇文档的内容；以上全部内容止于"二进制文件已可获取、
可分发"。如果某个节点要在完全没有控制平面的情况下运行，那是另一篇指南：
[独立 Worker 部署](/zh-cn/guides/independent-worker/)。

## 12. 验证部署

按顺序逐项完成 —— 每一项都会独立地、显式地失败：

```sh
# 1. 数据存储
docker compose ps                     # postgres、rabbitmq 健康，redis 已启动

# 2. Schema
./manage-tool db migrate              # 来自第 6 节
#   → schema is up to date

# 3. 控制平面：每种模式一条启动横幅，且没有重启循环
docker compose logs --tail=20 master-dashboard master-workers master-consumer master-cron

# 4. Worker API through nginx: HTTP/2 200 with a grpc-status header means the
#    master answered (13 = "Missing request message", which an empty probe earns)
curl --http2 -sS -D - -o /dev/null -X POST -H 'content-type: application/grpc' \
  --data-binary '' https://guru.example.com/guru.orchestration.agent.WorkerAgent/Register

# 5. 经反向代理访问控制台（303 跳到 /auth）
curl -s -o /dev/null -w '%{http_code}\n' https://guru.example.com/

# 6. 实时总线：每个 `dashboard_grpc` 副本一行，在启动时以及每次 Redis 重连后打印。运维 API 的
#    `Watch*` 流 —— 因而也包括控制台上实时更新的画布和健康状况页面 —— 都是由它来提供的。
docker compose logs master-dashboard | grep 'live bus connected'

# 7. 用管理员账号登录 —— 这是唯一能端到端走通
#    控制台 → 运维 API → 数据库的检查。
```

如果第 1–5 步都通过，而第 7 步以 `Forbidden` 失败，请重读第 9 节里的代理警告。

## 13. 升级、备份、回滚

**升级。** 在 `.env` 中提升 `MASTER_VERSION`（如果控制台也有新标签，同时提升 `FRONTEND_VERSION`），
然后执行 `docker compose pull && docker compose up -d`。新的 master 会在启动时自行应用所有尚未应用的
migration。

回滚意味着把版本变量固定回上一批标签 —— 但这并不会撤销已经执行过的 migration，所以如果某个版本的
migration 删除或重命名了旧二进制仍在读取的东西，就无法用这种方式回滚。遇到这种情况时，release notes
会写明。

**备份。** PostgreSQL 是唯一无法重建的状态：

```sh
docker compose exec -T postgres pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" \
  > guru-$(date +%F).sql
```

如果你想要一条快速恢复路径，也请给 `postgres-data` 卷做快照。RabbitMQ 不需要备份：它的队列里装的是编辑
提示和执行信号，两者都会被调度器重新发布，而代数计数器保证了幂等 —— 但 broker 必须是*运行中*的，
因为它不在的时候不会有任何周期任务发生。
Redis 也不需要备份，而且理由更硬：它按不带 AOF、不带 RDB 配置运行，里面根本没有可保存的东西。
换掉容器，各个 master 会重新订阅。

**日志。** 所有输出都是 stdout 上结构化的 `tracing` 日志，`GURU_LOG_LEVEL` 接受完整的 `EnvFilter`
字符串（`info`、`warn`、`guru_master=debug,orchestration=debug`，……）。用你惯用的 Docker 日志驱动
把它送出去。

## 14. 故障排查

| 症状 | 原因 |
|---|---|
| 控制台登录返回 `Forbidden` / `Cross-site remote requests are forbidden` | 重建出的 origin ≠ 浏览器的 `Origin`。请用 HTTPS 提供服务，或设置 `PROTOCOL_HEADER`/`HOST_HEADER` 并转发 `X-Forwarded-Proto` 和 `X-Forwarded-Host`（带端口）。`ORIGIN` 没有任何效果。 |
| 登录成功，但下一个请求又跳回 `/auth` | 带 `Secure` 的会话 cookie 被丢弃了 —— 浏览器是通过纯 HTTP 访问控制台的。 |
| master 以 `this mode opens the database: set GURU_DATABASE_URL` 退出 | URL 未设置或为空；它没有默认值。`cron` 是唯一不需要它的模式。 |
| master 以 `master key: GURU_MASTER_KEY is not set`（或 `must be 32 bytes`）退出 | `dashboard_grpc`、`workers_grpc` 和 `consumer` 都需要这个密钥（`cron` 不读取它）。用 `manage-tool generate-master-key` 生成一个；它只从环境变量读取。 |
| master 以 `stored config for key ... does not match its type` 退出 | 存储的文档损坏，或早于某次字段重命名。用 `manage-tool config get <key>` 检查它，并用 `config set` 重写。 |
| 某个 TLS 客户端 Pod 一直停在 `invalid_pods`，提示 `certificate for … is pending` / `failed: …` | ACME 任务还没签发它，或上一次尝试失败了（`ListCertificates` 会显示 `last_error`）。它运行在 `consumer` 中，由 `renew_certificates` 信号触发：确认有 `consumer` 在运行、DNS provider token 与 `domain_id`（Cloudflare zone id / Vercel domain）正确，并且 consumer 能访问 ACME 目录。`RetryCertificate` 可以强制重试。 |
| 某个 relay pod 一直停在 `invalid_pods`，提示 `internal CA not initialised` | 执行一次 `manage-tool orchestration init-ca`。 |
| master 立即以 AMQP 错误退出 | `AMQP_URI` 未设置或不可达。四种模式都需要 broker。检查 URI 结尾的 `/`。 |
| master 立即以 Redis 错误退出 | `REDIS_URL` 未设置，或服务端不可达。`dashboard_grpc`、`workers_grpc` 和 `consumer` 都需要它；`cron` 不需要。 |
| 已打开的画布或健康状况页面停止更新（刷新页面却能看到那次变更），或某个 `Watch*` 流不再投递快照 | Redis 挂了，或者为该流服务的那个 `dashboard_grpc` 副本访问不到它。在它的日志里找 `live bus connected`。编辑照样生效、照样派生，停掉的只有实时投递，重连之后就会恢复。 |
| `consumer` 或 `cron` 周期性重启 | broker 丢失时属预期行为：客户端不重连，所以进程退出，再由重启策略把它拉起来。该排查的是 broker，不是 master。 |
| 全新安装后立刻出现 `relation "…" does not exist` | migration 从未运行：`GURU_DATABASE_URL` 中的角色可能对该数据库没有 `CREATE` 权限。执行 `manage-tool db migrate` 并阅读它的报错。 |
| `manage-tool` 写到了错误的数据库 | 工作目录下的某个 `.env` 提供了 `GURU_DATABASE_URL`。请始终显式传入 `--database-url`。 |
| 画布编辑永远到不了 worker | `consumer` 挂了：编辑钩子和过期画布清扫都由它运行，没有它什么都不会派生。如果 `consumer` 是正常的，就检查 `cron` —— 没有时钟，清扫永远不会触发，只有带活跃 `CanvasDirty` 的编辑才会派生。 |
| 周期任务不再发生（没有节点变 `Offline`，没有续期） | RabbitMQ 挂了，或者 `cron` 挂了。两者都是必需的：时钟发布信号，consumer 执行它们。 |

关于所有参数和变量，参见[配置](/zh-cn/reference/configuration/)；关于"派生"到底做了什么，参见
[发布模型](/zh-cn/reference/rollout/)。
