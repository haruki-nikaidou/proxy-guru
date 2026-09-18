---
title: 配置
description: guru-master、guru-worker、manage-tool 和控制台的全部参数与环境变量。
---

每个二进制程序的同一项配置既可以通过命令行参数给出，也可以通过环境变量给出；命令行参数优先。

## `guru-master`

| 参数 | 环境变量 | 默认值 |
|---|---|---|
| `--mode` | `GURU_WORKER_MODE` | `dashboard_grpc` |
| `--dashboard-addr` | `GURU_DASHBOARD_GRPC_ADDR` | `0.0.0.0:50051` |
| `--workers-addr` | `GURU_WORKERS_GRPC_ADDR` | `0.0.0.0:50052` |
| `--database-url` | `GURU_DATABASE_URL` | *除 `cron` 外所有模式均必填* |
| `--db-pool-size` | `GURU_DB_POOL_SIZE` | `10`（必须 ≥ 1） |
| `--db-statement-timeout-ms` | `GURU_DB_STATEMENT_TIMEOUT_MS` | `5000`（必须 ≥ 1） |
| `--amqp-uri` | `AMQP_URI` | *所有模式下均必填* |
| `--redis-url` | `REDIS_URL` | *在 `dashboard_grpc`、`workers_grpc` 和 `consumer` 模式下必填* |
| `--watch-poll-ms` | `GURU_WATCH_POLL_MS` | `1000`（必须 ≥ 1） |
| `--log-level` | `GURU_LOG_LEVEL` | `info` |
| — | `GURU_MASTER_KEY` | *在 `dashboard_grpc`、`workers_grpc` 和 `consumer` 模式下必填*（仅支持环境变量；32 字节随机数据的 base64 编码 —— `manage-tool generate-master-key`） |
| — | `GURU_SMTP_PASSWORD` | *可选，仅 `notifier`*（仅支持环境变量；未设置时以不认证的方式发送） |
| — | `GURU_TELEGRAM_BOT_TOKEN` | *可选，仅 `notifier`*（仅支持环境变量；未设置时禁用 Telegram） |

其余所有可由运维人员调整的项——健康阈值与保留期、默认 ACME 目录、续期窗口、各周期任务的运行频率——都存放在数据库中，而不是环境变量里。
参见[模块配置](#模块配置)。

`--mode` 接受 `dashboard_grpc`、`workers_grpc`、`consumer`、`notifier` 和 `cron`。消息代理 URI 形如
`amqp://guru:guru@127.0.0.1:5672/`，末尾的 `/` 表示选用默认 vhost。**所有**模式都需要消息代理，
`cron` 也不例外：周期性工作是以消息形式发布的，因此代理中断会让派生、存活检测和证书续期一直停滞，
直到代理恢复。

`notifier` 是[通知](/zh-cn/features/notifications/)的投递侧：恰好一个实例，由一把 PostgreSQL
advisory lock 强制保证。它在启动时读取 `notify` 配置，并且只需要数据库和消息代理 —— 既不需要
`GURU_MASTER_KEY`（它不解密任何东西），也不需要 `REDIS_URL`（它不发布任何实时事件）。它的两个渠道
机密就是上面那两个环境变量。

在提供服务与派生的那三种模式下 Redis 同样必填，理由也属于同一类：它是运维 API `Watch*` 流背后的
实时总线。URL 形如 `redis://127.0.0.1:6379/`。每一次变更都会在频道 `guru:orchestration:live` 上发布
一个事件，而每个 `dashboard_grpc` 副本只订阅它一次，因此在某个副本上做出的编辑也会到达由其他副本
提供的那些流。这里什么都不会被存下来：整个集群不需要该服务器提供任何持久化，既不需要 AOF，也不需要
RDB。Redis 中断只会让已打开的 `Watch*` 流收不到投递——订阅端会带退避地重连，并在每次重连后让每个
watcher 重新读取数据库，因此它一恢复就不会有任何内容停留在旧状态——但它绝不会影响派生，也不会影响
Worker 实际运行的内容。

控制台跟随这些流：画布编辑器、健康状况页面及其 Pod 事件都会就地更新，每个页面通过一条
server-sent-events 连接完成，由控制台把它桥接到 gRPC 流上。这些页面上的 *实时* 徽章显示的是这条浏览器
连接；而检查总线本身靠的是日志行 `live bus connected`。

`GURU_MASTER_KEY` 用于加密所有静态存储的密钥材料——DNS 提供商 API 令牌、ACME 账户密钥、证书和 CA
私钥——凡是需要读取密钥材料的三种模式都必须提供：`dashboard_grpc`、`workers_grpc` 和 `consumer`。
`cron` 从不接触密钥材料，也不会读取该密钥，`notifier` 也不读取。它被有意设计成不提供命令行参数：
argv 在进程列表中是可见的。丢失该密钥意味着必须重新录入每个 DNS 提供商令牌并重新签发所有证书；
也不支持原地更换。

`GURU_DATABASE_URL` 只在会建立连接的那四种模式下必填。`cron` 会忽略它，甚至完全不需要数据库 URL
即可启动：一个没有数据库 URL 就拒绝启动的时钟，等于背上了一个用不到的数据库依赖。在会打开连接的那些
模式下，连接池最多持有 `GURU_DB_POOL_SIZE` 个连接，每条语句都由服务端按 `GURU_DB_STATEMENT_TIMEOUT_MS`
限时——超时的语句会被 PostgreSQL 取消，边界层会把它报成 `UNAVAILABLE`，而不是一次故障。此外，每个
提供服务的 master 都会在启动时应用所有尚未应用的 migration，过程持有一把 advisory lock，因此一批同时
启动的 master 只会把每个 migration 恰好应用一次。

### 调度与执行

`cron` 只是一个时钟，别无其他。它每 5 秒扫描一次，为每个到期的任务发布一个执行信号，并且不打开任何
数据库连接。`consumer` 为每种信号绑定一个持久队列并执行对应的任务，与 `CanvasDirty` 派生钩子并列
——于是周期性工作在扩展、重试和故障转移上的表现与一次编辑完全一致，而某个卡住的任务也无法让时钟停摆。

| 路由键 | 队列 | 发布周期 | 最短实际执行周期 | 任务内容 |
|---|---|---|---|---|
| `derive_stale_canvases` | `guru_orchestration_derive_stale_canvases` | 30 s | `sweep_interval_secs`（30） | 重新派生所有 `generation` 超前于 `derived_generation` 的画布 |
| `sweep_liveness` | `guru_orchestration_sweep_liveness` | 30 s | `liveness_interval_secs`（30） | 当某台服务器在 `health_report_interval_secs × health_offline_after_intervals` 内没有上报时，将其标记为 `Offline` |
| `trim_health_history` | `guru_orchestration_trim_health_history` | 300 s | `health_retention_interval_secs`（300） | 删除早于 `server_health_ttl_secs` / `pod_health_ttl_secs` 的 `server_health_record` / `pod_health_record` 记录 |
| `renew_certificates` | `guru_orchestration_renew_certificates` | 60 s | `acme_interval_secs`（60） | ACME 签发与续期：在到期前 `acme_renew_before_secs` 续期，失败后经过 `acme_retry_after_secs` 重试 |
| `rotate_relay_certificates` | `guru_orchestration_rotate_relay_certificates` | 3600 s | `relay_rotation_interval_secs`（3600） | 重新签发距到期不足 `relay_cert_renew_before_secs` 的中继叶证书，并重新派生其所属画布 |
| `resolve_server_countries` | `guru_orchestration_resolve_server_countries` | 60 s | `country_lookup_interval_secs`（60） | 通过 `country_lookup_url` 查询还不知道国家的服务器 IPv4 地址：新地址或变化了的地址立即查询，查询失败的地址经过 `country_lookup_retry_after_secs` 后再查 |

这是两层机制，而且两个数字并不相同。调度器按中间那一列的固定节奏发布信号，因为它不读取任何配置；
消费者在真正动手之前会先声明这一次运行——每个任务一行 `orchestration_job_run`，通过 compare-and-set
完成，在整个集群范围内每个配置间隔最多运行一次。间隔是按信号所携带的*调度时刻*计算的，而不是按消费者
腾出手来处理它们的时刻，因此繁忙的消费者不会悄悄拉长节奏。于是，小于或等于信号发布节奏的间隔就等于
“每个信号都执行”，更大的间隔会让该任务在整个集群范围内变慢，而重复投递或重放的消息会被拒绝而不是执行
两次——因为已经运行过的时刻永远无法再次被声明。这些间隔是存储在 `orchestration` 键上的值，因此与该键
上的其他所有设置一样，修改会在消费者重启后生效。

当一台服务器落后于其 `desired` 修订版本的时间超过 `degraded_grace_secs`，或最近一次确认的修订版本在
任一 pod 上失败时，该服务器处于 `Degraded` 状态。上面提到的这些名称都是存储的 `orchestration` 配置的
键，而不是命令行参数。

## `guru-worker`

| 参数 | 环境变量 | 默认值 |
|---|---|---|
| `-c`, `--config` | `GURU_WORKER_CONFIG` | —（独立模式；收到 `SIGHUP` 时重新加载） |
| `--master` | `GURU_MASTER` | —（agent 模式；需要同时指定 `--server`；`http://host:50052` 是明文 h2c，`https://host` 是 TLS 并按系统根证书校验） |
| `--server` | `GURU_SERVER_ID` | —（`orchestration_server` 记录键） |
| `--api-key-file` | `GURU_API_KEY_FILE` | —（`GURU_API_KEY` 的替代方式） |
| `--state-dir` | `GURU_STATE_DIR` | `/var/lib/guru-worker` |
| `--health-interval` | `GURU_HEALTH_INTERVAL_SECS` | `15`（两次健康上报之间的秒数；agent 模式；必须 ≥ 1） |
| `--public-ipv4-urls` | `GURU_PUBLIC_IPV4_URLS` | `https://checkip.amazonaws.com,https://api.ipify.org,https://ipv4.icanhazip.com`（agent 模式；以逗号分隔的服务方列表，它们以纯文本返回调用方的 IPv4 地址，从一个轮转的起点开始依次尝试，每个 3 秒；每 60 秒重新检查一次，发生变化时上报；留空则禁用该查询，网卡地址仍会照常上报） |
| `--public-ipv6-urls` | `GURU_PUBLIC_IPV6_URLS` | `https://ipv6.icanhazip.com,https://api6.ipify.org,https://v6.ipinfo.io/ip`（IPv6 同上） |
| `--log-level` | `GURU_LOG_LEVEL` | `info` |

`--config` 和 `--master` 互斥；两者都不给出时，Worker 以独立模式运行，并使用默认路径
`/etc/guru-worker/config.toml`。`--log-level`/`GURU_LOG_LEVEL` 只配置 agent 模式，
而且只管到应用第一份配置为止（启动时是保存的 last-good 配置，之后是 master 下发的每一份）：从那以后生效的是仪表盘里
这台服务器的日志级别，切换无需重启。独立模式改为从配置文件读取 `log.level`（它完全不看这个参数）。agent 模式从 `GURU_API_KEY` 读取运维 API 密钥，或从
`--api-key-file` 指定的文件读取（尾部空白会被去掉）；该密钥在每个会话中只使用一次，用于向 master 注册。

## Worker 配置文件

上面的参数决定进程本身，而这里决定流量。该文件是 TOML 格式，由 `lib/guru_worker_config` 建模，并且在
两种模式下都是*同一个*模型：`guru-master` 从画布派生出它并以流推送，独立模式的 Worker 则从磁盘加载它。
因此，独立 Worker 能够接受的文件，也恰好就是控制平面会下发的内容。

顶层：

| 键 | 默认值 | 取值 |
|---|---|---|
| `ipv6_resolve` | `"tolerated"` | `required`、`preferred`、`tolerated`、`forbidden` —— 目的地为域名时的地址族策略 |
| `log.level` | `"info"` | 一条 `tracing` 的 `EnvFilter` 指令 —— `info`、`debug`，或更有针对性的写法如 `guru_worker=debug,warn`。启动时应用，每次重新加载时再次应用，无需重启；master 派生的配置里是 `trace`、`debug`、`info`、`warn`、`error` 之一 |
| `[keepalive]` | 见下文 | 数据面每条连接的存活探测 |
| `[quic]` | 见下文 | 本 Worker 在每条 QUIC 中继链路上的一侧：拥塞控制、速率、窗口 |
| `[[forwarding]]` | `[]` | 每一项对应一个监听器；不含任何条目的文件也是合法的，只是什么都不做 |

独立模式下，进程日志由 `log.level` 配置：`--log-level`/`GURU_LOG_LEVEL` 只适用于 agent 模式，并会被应用的第一份配置里的 `log.level` 取代。

`ipv6_resolve` 是全局设置，并会被固化进每个编译后的目标中：`required`/`forbidden` 会把另一个地址族
视为解析失败，`preferred`/`tolerated` 在两个地址族都能解析时选出优先者，否则回退到另一个。

### `[keepalive]`

TCP 分不清"对端安静"和"对端消失"：客户端没有发 FIN 或 RST 就从网络上掉了（手机断网、NAT 表项过期、
机器掉电），它的连接就会在 Worker 上永远挂着，连带后面整条管道一起。因此 Worker 会在每个 accept 到的和
拨出去的 TCP socket 上开启 `SO_KEEPALIVE`，由内核探测空闲连接，连续几次探测无应答后判定失败。QUIC
中继跳的需求正相反：不发 ping 的话，安静的流底下的 QUIC 连接会空闲超时，把一条只是暂时没话说的长连接切断。
所有值都是整秒（或次数），且至少为 1。

| 键 | 默认值 | 取值 |
|---|---|---|
| `tcp_idle_secs` | `60` | 空闲多久后开始第一次探测（`TCP_KEEPIDLE`） |
| `tcp_interval_secs` | `10` | 开始探测后，两次探测之间的间隔（`TCP_KEEPINTVL`） |
| `tcp_retries` | `3` | 连续几次探测无应答后判定连接失败（`TCP_KEEPCNT`） |
| `quic_ping_secs` | `15` | 空闲 QUIC 中继连接上的 ping 周期，两端都发 |
| `quic_idle_secs` | `60` | 多久收不到任何包就判定 QUIC 中继连接丢失；必须大于 `quic_ping_secs` |

这不是空闲上限：只要对端回应探测，连接想开多久就开多久。探测只在连接静默满空闲时长后才开始，每次是一个
空的 TCP 段，由对端内核自动应答——所有客户端都支持，顺带还能刷新沿途的 NAT 表项。

该段只有在与默认值不同时才会写出，而 `guru-master` 下发的就是默认值：早于该段存在的 Worker 会拒绝未知
键，所以在独立模式文件里调整了它的运维人员必须运行认识这一段的 Worker。

### `[quic]`

本 Worker 在它监听或拨出的每条 QUIC 中继链路上的一侧。quinn 的默认值是给网页客户端准备的，不是给中继的：
Cubic 一丢包就退避，而固定 1.25 MB 的单流接收窗口把一条流封在 `窗口 / RTT` 以内，发送端再怎么努力也没用。
告诉链路它的速率之后，它会得到 hysteria 的 *brutal* 发送端，以及按该速率算出的窗口。

| 键 | 默认值 | 取值 |
|---|---|---|
| `congestion` | `"cubic"` | `cubic` 探测带宽；`brutal` 不管路径怎样都按 `send_mbps` 发送，并按观测到的丢包率多发（最多四分之一） |
| `send_mbps` | `0` | 朝对端的速率，Mbit/s：brutal 的固定速率，也是发送窗口的依据。`0` 保持 quinn 默认 |
| `receive_mbps` | `0` | 对端朝本侧发送的速率，Mbit/s；接收窗口按它的半秒数据量计算。`0` 保持 quinn 每流 1.25 MB |
| `max_streams` | `0` | 监听方允许对端在一条连接上同时打开的流数；`0` 表示 4096 |
| `stream_receive_window` | `0` | 单流接收窗口字节数，代替由 `receive_mbps` 推导的值 |
| `receive_window` | `0` | 整条连接的接收窗口字节数；`0` 表示不限 |
| `send_window` | `0` | 本侧每条连接允许的未确认字节数，代替由 `send_mbps` 推导的值 |

一条链路上所有被代理的连接共用一条 QUIC 连接（每个连接一条流），所以速率是整条链路的总量，而不是每个
连接的配额：填这个方向上路径实际能跑的值，填多了只会制造丢包。`quic` 中继监听器上的 `[forwarding.quic]`
表，或 `quic` 跳上的 `[forwarding.to.quic]` 表，会替代该链路上的这一段。`guru-master` 就是这样配对链路两端的：
每台服务器把自己的数字放在 `[quic]` 里，当对端的*下行*低于本侧的*上行*（或反过来）时，forwarding 里写的是
较低的那个，谁都不会发得比对端声称能收的更快。没有 `send_mbps` 的 `brutal` 会被拒绝，把这两张表放在非 QUIC
中继上也会被拒绝。和 `[keepalive]` 一样，该段在默认值时不写出。

### `[[forwarding]]`

| 键 | 必填 | 取值 |
|---|---|---|
| `tag` | 是 | 自由格式的名称；它就是出现在日志、lint 输出和应用错误中的标识 |
| `listen` | 是 | `ip:port` —— 必须是字面地址，绝不能是主机名（`0.0.0.0:443`、`[::]:443`） |
| `receive_proxy_protocol` | 否 | `"v1"` 或 `"v2"` —— 表示在客户端载荷之前会有一个 PROXY 头 |
| `listen_as` | 是 | 如何接受连接：`"raw"`，或一个 `tls` / `relay` 表（见下文） |
| `quic` | 否 | 只作用于这个监听器的 `[quic]` 表；仅限 `quic` 中继监听器 |
| `to` | 是 | 流量去向：一个 `[forwarding.to]` 表（见下文） |

监听器以 `(listen, transport)` 为键，而只有 `quic` 中继监听器的 transport 才是 QUIC。因此，当一个条目
是 QUIC（UDP）而另一个是 TCP 时，两个条目可以共用同一个 `ip:port` —— 其他任何情况都会得到
`duplicate listener` 错误。

`receive_proxy_protocol` 是一个开关，而不是严格的版本校验：头部是自动识别的，所以配成 `"v1"` 的条目
同样接受 v2 头部。它真正改变的是*是否期待存在头部*，并因此决定 Worker 归属于该连接的客户端地址
（用于日志、`ip_hash`，以及它继续向后写出的头部）是真实客户端还是你的上游代理。它只对 `raw` 和 `tls`
监听器有意义 —— `relay` 监听器总是会读取一个头部（见下文）。

### `listen_as`

```toml
listen_as = "raw"                     # 纯 TCP，载荷原样转发
```

```toml
[forwarding.listen_as.tls]            # 在此终止 TLS，向后转发明文
key = "/etc/guru-worker/tls/key.pem"
full_chain = "/etc/guru-worker/tls/fullchain.pem"
```

```toml
[forwarding.listen_as.relay]          # 来自另一个 guru worker 的入向流量
relay_type = "tcp"                    # "tcp" | "tls" | "quic"
# relay_type = "tls" 和 "quic" 还额外需要：
# key = "/etc/guru-worker/tls/key.pem"
# full_chain = "/etc/guru-worker/tls/fullchain.pem"
```

`key` 和 `full_chain` 是 PEM 文件路径，在配置被应用时解析，这正是证书续期只需重新加载而不必重启的原因。
独立模式下没有任何组件会为你准备这些文件。agent 模式下，master 会随每个修订版本一起下发它们
（`ConfigRevision.files`），路径相对于 `--state-dir` —— TLS 客户端 Pod 对应
`certs/acme/<certificate>/{full_chain,key}.pem`，`tls`/`quic` 中继监听器对应
`certs/relay/<pod>/{full_chain,key}.pem`，内部 CA 对应 `certs/ca.pem` —— Worker 会在应用之前写入它们
（密钥文件权限 `0600`，每个目录都是原子替换）。文件中的相对路径按 `--state-dir` 解析。

顶层的 `relay_ca = "certs/ca.pem"` 指定 `tls`/`quic` 中继*拨号方*用于校验对端的 CA 证书；未设置则使用
系统根证书。只要内部 CA 存在（`manage-tool orchestration init-ca`），master 就会设置它；中继叶证书在
两端都携带 SNI `<pod-key>.relay.guru.internal`。

`relay` 监听器是 `to.type = "relay"` 这一跳的接收端，而不是对公网的入口：它**总是**从解码后的流中读取
一个 PROXY 头部（上游那一跳正是这样把真实客户端地址传递下来的），因此 `receive_proxy_protocol` 在那里
无关紧要。

### `[forwarding.to]`

```toml
[forwarding.to]
type = "exit"                         # 最后一跳：连接真实后端
destination = "10.0.0.5:8080"         # ip:port 或 domain:port
send_proxy_protocol = "v2"            # 可选："v1" | "v2"
```

```toml
[forwarding.to]
type = "relay"                        # 下一个 guru 跳
protocol = "tcp"                      # "tcp" | "tls" | "quic"
destination = "hop.example.com:9443"
sni = "hop.example.com"               # "tls" 和 "quic" 必填
```

```toml
[forwarding.to]
type = "load_balance"
strategy = "round_robin"              # "round_robin" | "random" | "ip_hash" | "fallback"

[[forwarding.to.members]]             # 成员本身也是 `to` 节点……
type = "exit"
destination = "10.0.0.6:8080"

[[forwarding.to.members]]             # ……所以分组可以嵌套：再加一层 `.members`
type = "load_balance"
strategy = "fallback"

[[forwarding.to.members.members]]
type = "exit"
destination = "backend.internal:8080"
```

两种形式下 `destination` 都是 `host:port`；能解析为字面 socket 地址时就按字面地址处理（IPv6 用方括号：
`[2001:db8::1]:8080`），否则按域名处理，此时会在每次连接时按 `ipv6_resolve` 解析。`fallback` 按顺序
尝试各个成员，直到有一个连接成功；其他策略则从中挑选一个。中继跳总是会向下一个 Worker 写出 PROXY v2
头部 —— 只有 `exit` 才有可选的 `send_proxy_protocol`，因为只有在那里对端才是别人的后端。

### 路由表

`to` 也可以不写内联树，而是写这个 forwarding 自己的某个 **分组（group）** 或 **上游（upstream）** 的 id。
每个下一跳都是一个上游，每个在下一跳之间的选择都是一个分组，分组按 id 列出成员 —— 所以在负载均衡之上做
故障转移，或者在故障转移之上做负载均衡，都只是成员为分组的分组。master 发给报告了 `route_table` 能力的
Worker 的就是这种形式；内联树仍是所有 Worker 都能读的形式。

```toml
[[forwarding]]
tag = "web"
listen = "[::]:443"
listen_as = "raw"
to = "g"                                # 流量开始进入的分组或上游

[[forwarding.group]]
id = "g"
failover = ["g.0", "u:backup"]          # 按顺序使用第一个存活的成员

[[forwarding.group]]
id = "g.0"
balance = [{ to = "u:hk-1", weight = 2 }, { to = "u:hk-2" }]   # weight 默认为 1
sticky = "client_ip"                    # 可选，仅限 balance

[[forwarding.upstream]]
id = "u:hk-1"
relay = { protocol = "quic", destination = "203.0.113.1:40000", sni = "hk-1.relay.guru.internal", confirm = true }

[[forwarding.upstream]]
id = "u:hk-2"
relay = { protocol = "tcp", destination = "203.0.113.2:40000" }

[[forwarding.upstream]]
id = "u:backup"
exit = { destination = "backend.internal:8080", send_proxy_protocol = "v2" }
```

balance 按权重比例把连接分给存活的成员 —— 平滑轮询；设置 `sticky = "client_ip"` 时则对客户端地址做加权
一致性（rendezvous）哈希，只要成员存活，客户端就一直落在同一个成员上。failover 使用第一个存活的成员。
两者都会跳过所有跳都已失效的成员，一次失败的尝试会在同一个客户端连接内转向下一个选择。连续失败三次的
上游被视为失效；十秒后每次只允许一个连接重新尝试它。

中继上游上的 `confirm = true` 要求中继在它 *自己的* 下一跳连通后再应答，于是存活中继背后失效的出口会在
拨号端被判定为这一跳失败，选择随之转向下一个。只应对报告了 `relay_confirm` 的 Worker 设置它；master
会自动这样做。中继上游上的 `quic` 表与树形 `relay` 中的一样，是这一跳这一端的链路设置。

### 哪些会被拒绝，哪些只是警告

出现下列任一情况时加载失败 —— 配置文件绝不会被部分应用：

| 错误 | 原因 |
|---|---|
| `parse toml: TOML parse error at line N …` | 每个表都拒绝未知键；拼错是错误，而不是静默取默认值 |
| `duplicate listener <addr> (<tag>)` | 两个条目在同一 transport 上声明了相同的 `ip:port` |
| `forwarding <tag> relay to tls/quic requires sni` | 任意嵌套层级上出现了没有 `sni` 的 `tls`/`quic` 中继跳 |
| `forwarding <tag> has an empty load-balance group` | 任意嵌套层级上出现 `members = []` |
| `invalid remote '…'` / `invalid port in remote '…'` | `destination` 不是 `host:port` 形式 |
| `forwarding <tag> refers to route id <id>, which it does not define` | `to` 或分组成员引用的 id 不是这个 forwarding 中任何分组或上游的 id |
| `forwarding <tag> defines route id <id> more than once` | 两个分组或上游使用了同一个 id |
| `forwarding <tag> has an empty group <id>` / `gives <id> a weight of zero` | 没有成员的分组、权重为 0 |
| `forwarding <tag> has a group cycle through <id>` | 互相包含的分组 |
| `forwarding <tag> has groups or upstreams but an inline to tree` | 同一个 forwarding 混用了两种形式 |

下面这些只会记录为警告，进程继续运行：

- 只有一个成员的负载均衡分组（这个分组毫无意义）；
- 在未设置 `receive_proxy_protocol` 的监听器之下任意位置使用了 `ip_hash`。只有对 `raw` 和 `tls`
  监听器才需要照字面理解这条警告：此时每个连接哈希的都是 Worker 看到的地址 —— 也就是你的上游代理的
  地址 —— “均衡”便塌缩到单个成员上。`relay` 监听器无论如何都会读取它必需的中继头部，因此它哈希的是
  真实客户端，这条警告在那里是误报；lint 并不区分这两种情况。

即使配置解析通过，应用时仍可能逐条失败 —— 缺少证书文件、地址已被其他进程占用 —— 报告形式为
`<tag>: <reason>`。这属于应用错误而非配置错误，而且是*按 pod* 计的：其他每个 `[[forwarding]]` 都会
照常提交，失败的那个保留它此前的监听器（或者什么都没有）。独立模式下启动失败会直接终止进程，而
`SIGHUP` 重新加载失败时会记录失败的条目并保留它们原有的监听器；agent 模式下每个 pod 的结果都会确认给
master，由 master 在服务器上记录失败的 pod，并为每个失败的 pod 写一条 `Failed` 健康事件。

## `manage-tool`

只有一个全局参数：`--database-url`（`GURU_DATABASE_URL`），和 `guru-master` 读取的是同一个 URL。

| 子命令 | 用途 |
|---|---|
| `create-admin --email <email> --password <password>` | 初始化第一个管理员账户 |
| `generate-master-key` | 打印一个新的 `GURU_MASTER_KEY`（无需数据库） |
| `db migrate` | 应用所有尚未应用的 Schema migration；`guru-master` 启动时做的是同一件事 |
| `config seed` | 为每个尚无取值的键写入默认值；已修改过的键保持不变 |
| `config list` | 打印每个键及其存储的文档（若没有存储值则打印默认值） |
| `config get <key>` | 原样打印某个键存储的文档，不做解码 —— 即使文档已损坏也能读出 |
| `config set <key> <json>` | 替换某个键存储的 JSON（写入前先校验） |
| `orchestration export-config --server <key>` | 打印为某台服务器派生出的 `guru-worker` TOML |
| `orchestration init-ca` | 为中继 TLS/QUIC 链路创建内部 CA 并打印其证书；若已存在则拒绝替换（需要 `GURU_MASTER_KEY`） |

## 控制台

| 环境变量 | 默认值 | 用途 |
|---|---|---|
| `GURU_GRPC_URL` | `127.0.0.1:50051` | `guru-master --mode dashboard_grpc` 的控制台 gRPC 端点 |
| `PROTOCOL_HEADER` | — | 携带对外协议的请求头，例如 `x-forwarded-proto`。**未设置意味着应用假定为 `https`** |
| `HOST_HEADER` | — | 携带对外主机名的请求头，例如 `x-forwarded-host`（必须包含非默认端口） |
| `ADDRESS_HEADER` | — | 携带客户端 IP 的请求头，例如 `x-forwarded-for` |
| `BODY_SIZE_LIMIT` | `512K` | 请求体的最大大小 |

服务器监听 `:3000`。每个请求自身的 origin 都由上述请求头重建，浏览器 `Origin` 与之不匹配的 POST 请求会
被以 `403 Cross-site remote requests are forbidden` 拒绝 —— 因此在纯 HTTP 或改动了端口的代理之后，
`PROTOCOL_HEADER` 和 `HOST_HEADER` 都是必需的。`ORIGIN` 在运行时**不会**被读取：Node adapter 会在构建
时把 `kit.paths.origin` 固化进产物。会话 cookie 带有 `Secure`，因此控制台必须通过 HTTPS 提供服务
（`localhost` 除外）。

## 模块配置

可由运维人员调整的设置存放在数据库中：每个键一行 `app_config`，以 JSON 文档形式保存整份配置。数据库是
唯一的事实来源 —— 没有缓存，也没有第二份副本 —— 因此集群中的每个 `guru-master` 都运行完全相同的设置，
无需对齐环境变量；修改设置不需要重新部署，只需要重启。目前存在三个键：

| 键 | 结构体 | 内容 |
|---|---|---|
| `auth` | `auth::config::AuthConfig` | `session_idle_ttl_secs` |
| `notify` | `notify::config::NotifyConfig` | `smtp_host`（默认为空，即禁用邮件）、`smtp_port`（587）、`smtp_starttls`（`true`；`false` 走明文 SMTP，仅适用于 localhost 上的中继或测试接收端）、`smtp_username`（为空则以不认证的方式发送）、`smtp_from`（`guru <noreply@example.com>`）、`telegram_api_base`（`https://api.telegram.org`）、`delivery_attempts`（3）、`delivery_retry_delay_secs`（5）、`default_language`（`en`；取 `en`、`ja`、`zh_cn` 之一 —— 从未指定过语言的设置行会以它渲染）。SMTP 密码和机器人令牌**不在**这里：它们是 `notifier` 上的 `GURU_SMTP_PASSWORD` 和 `GURU_TELEGRAM_BOT_TOKEN`，因为这份文档对每个 Admin 都可读 |
| `orchestration` | `orchestration::config::OrchestrationConfig` | `health_report_interval_secs`、`health_offline_after_intervals`、`degraded_grace_secs`、`server_health_ttl_secs`、`pod_health_ttl_secs`、`default_acme_directory`、`acme_renew_before_secs`、`acme_retry_after_secs`、`relay_cert_valid_secs`、`relay_cert_renew_before_secs`、`sweep_interval_secs`、`liveness_interval_secs`、`health_retention_interval_secs`、`acme_interval_secs`、`relay_rotation_interval_secs`、`stream_keepalive_secs`（默认 `15`：一条空闲的 `Watch*` 流多久发送一次空的保活消息，并重新校验开启它的那个会话；请让它小于 `:50051` 前面任何代理的空闲超时）、`trust_proxy_address_headers`（默认 `true`：Worker API 会把 `x-real-ip` / `x-forwarded-for` 的第一跳记录为注册请求的来源地址；如果 `:50052` 在没有前述代理的情况下也可达，请关闭它，否则 Worker 可以伪造该地址）、`country_lookup_url`（默认 `https://api.country.is/{ip}`：为服务器的国旗查询其 IPv4 地址所在国家的地址，`{ip}` 会替换成该地址；返回值可以是带两字母 `country` 字段的 JSON 对象，也可以只是两个字母，所以 `https://get.geojs.io/v1/ip/country/{ip}` 也能用；留空则不查询）、`country_lookup_interval_secs`（默认 60）、`country_lookup_retry_after_secs`（默认 3600：查询失败后，要隔多久才再次查询同一个地址） |

在 `manage-tool db migrate` 之后运行 `manage-tool config seed` 写入默认值，再用 `manage-tool config list`
查看已存储的内容。`list` 和 `get` 会原样打印该行 —— 它们不做解码，因此即便某份文档会让 master 启动时
读取失败，你仍然可以检视它。`set` 会替换整份文档，但在写入之前会先解码成该配置对应的类型，因此不完整的
载荷会用默认值补齐，而形状错误的载荷会在到达数据行之前就被拒绝：

```sh
manage-tool config set orchestration '{"acme_renew_before_secs":1209600}'
```

那六个 `*_interval_secs` 字段规定了周期任务实际允许运行的频率
（见[调度与执行](#调度与执行)）。它们放在这里而不是环境变量里，是因为整个集群必须就它们达成一致：
强制执行间隔的那次声明就是所有消费者共享的一行数据库记录。

```sh
manage-tool config set orchestration '{"acme_interval_secs":300,"relay_rotation_interval_secs":7200}'
```

这三份文档同样可以在控制台中读取和替换：**Admin**（且仅有 Admin —— 其他角色都不具备该权限，API 密钥
则从来没有）能看到该行的存储原文、seed 会写入的载荷，以及一个用于替换整份文档的表单。它的校验与
`manage-tool config set` 完全一致，因此形状错误的载荷会被拒绝，该行保留原有内容。从控制台保存同样不是
热重载：新值要生效必须**重启 `guru-master`**。

依赖数据库的那四种模式 —— `dashboard_grpc`、`workers_grpc`、`consumer` 和 `notifier` —— 只在启动期间
读取它们各自需要的键一次，然后把取值交给各自的服务；不存在热重载。`cron` 不打开数据库连接，因此这些
键一个都不读：它只发布执行信号，而收到信号的消费者会按存储的间隔执行。未执行过 seed 的安装运行的是
默认值。无法反序列化的数据行
会导致启动失败并指出对应的键 —— 因为用默认值替代损坏的文档会悄悄换掉运维人员的整份配置，比如把 ACME
从 staging 挪到生产目录。
后续版本新增的字段会以其默认值读取，因此旧的数据行仍然可用。
