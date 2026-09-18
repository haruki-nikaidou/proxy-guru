---
title: 健康监控
description: Worker 上报什么、master 如何把它变成服务器与 Pod 状态，以及控制台的健康状况页面展示什么。
---

每个 Worker 都与控制平面保持一条打开的流，并按固定间隔往里推送上报。这条流就是整个健康监控：它决
定一台服务器是否 `Online`，承载着控制台绘图所用的流量计数器，而它的沉默正是把一台服务器标记为
`Offline` 的依据。

## Worker 上报什么

每个间隔通过 `ReportHealth` 上报一个 `HealthReport` —— 这是 `WorkerAgent` 服务上的一条双向流，
用 Worker 当前的刷新密钥认证；master 对它记录下的每条上报都回一条应答：

| 字段 | 含义 |
|---|---|
| `running_revision` | Worker 认为自己正在运行的配置修订版本。`0` 表示全新安装，或状态目录被清空。 |
| `upload_bytes`、`download_bytes` | **自上一次上报以来**的字节数，对所有 forwarding 求和。 |
| `current_connections` | **上报那一刻**打开着的连接数 —— 这是一个瞬时量，不是每个间隔的累计量。 |
| `max_connections` | 自上一次上报以来的峰值水位。它从仍然打开的连接数开始重新计，而不是从零开始。 |
| `pods` | 每个正在运行的 forwarding 一个条目：它的 tag（该 pod 的 id）和一个可选的错误。 |
| `reported_addresses` | 只在地址发现发生了变化的那次上报里出现。 |

间隔由 master 说了算：`Register` 的应答中带有存储的 `orchestration` 配置里的
`health_report_interval_secs`（默认 15 秒），而 Worker 自己的 `--health-interval` 只在 master 发送
`0` 时才生效。Worker 一注册完这条流就会打开 —— 不等配置流 —— 并立即发出第一次上报。

上报任务的生命周期与会话完全一致。它若因任何原因结束，Worker 就会拆掉会话并以带上限的指数退避重
连，这意味着一次新的 `Register`、一个新的刷新密钥和一个重新协商的间隔。计数器无法跨越这个过程：丢
失的那个间隔的增量被丢弃，连接数瞬时量则从当时打开着的连接重新开始。

master 的应答也是 Worker 判断整条链路是否还通的依据。Worker 的 HTTP/2 PING 只到第一跳，而像 Cloudflare
这样的代理即使远端已经断了也照样应答，所以 Worker 只认 master 自己发来的数据：上报连续三个间隔没有应答，
或者配置流连续三个周期什么都没带来 —— 连每隔 `stream_keepalive_secs` 一条的保活都没有 —— 这次会话就会被
放弃并重新开始。应答之前构建的 master 会把 `stream_keepalive_secs` 报为 `0`，此时两个看门狗都不运行。
一次活过五分钟的会话结束后，重连退避会从一秒重新开始。

## master 存储什么

每一次被接受的上报都是一个事务。它以 Worker 的刷新密钥 generation 为栅栏 —— 来自已被取代的会话的
上报会被拒绝，而不是被记录 —— 并写入：

- 服务器行上的 `last_health_report_at`、`last_seen_at` 和当前的 `health_status`；
- 一行 `server_health_record`：状态、上报时间和那四个计数器；
- 该次上报能解析出的每个 pod 各一行 `pod_health_record`。

上报的 tag 先对照服务器的 `applied` 快照解析，再对照 `desired` 快照；两者都没有的 tag 会被跳过，不
写入任何东西。当一个 pod 以两个监听器被同时保留时 —— 这发生在它的依赖方切换到新端口的过程中 ——
这两个条目会合并成一行，取两者中**最差**的那个状态。

该写入提交之后，服务才会把这条记录发布到实时总线上 —— 这是一个独立的步骤，并不属于该事务 —— 而这
正是控制台的各个流以及某个画布的 `WatchGraph` 订阅者所读取的内容。

## 服务器状态

`Online`、`Degraded` 和 `Offline`。一台新服务器从 `Offline` 开始：还没有人听到过它的消息。每次被接
受的上报都会按以下顺序重新评估这个结论：

1. 服务器的配置视图上带有 `apply_error`，或任何 pod 被记录为失败 → `Degraded`；
2. `running_revision` 与已应用的修订版本不同 → `Degraded`（这正是用来捕捉上报 `0` 的 Worker 的那条
   规则）；
3. 存在一个更新的 desired 修订版本，而它的发布时间已超过 `degraded_grace_secs`（60 秒）→
   `Degraded`；
4. 否则为 `Online`。

`AckConfig` 也会直接写入 `Online`/`Degraded`，因此一次失败的应用无需等到下一次上报就会显现。

`Offline` 有两个执行点，都由 `health_report_interval_secs × health_offline_after_intervals` 驱动
—— 默认值下是 **45 秒**：

- master 以该值作为每次上报的超时来读取这条流，并以 `no health report within 45s` 结束一条沉默的
  流；
- `sweep_liveness` 任务会把最近一次被接受的上报早于该阈值的每一台非离线服务器翻转为离线。

仅仅是流结束了，不属于其中任何一种：代理会出于自己的原因切断流，而在阈值之内重连的 Worker 从来就不算
离线。流结束只是让 Worker 的下一次会话能够进来；一去不回的 Worker 由巡检来判定。

进入 `Offline` 还会清除该服务器的会话租约并递增它的 watch epoch，于是配置流结束，Worker 的下一次
`Register` 会被立即接受，而不是作为重复会话被拒绝。状态翻转会写入一行计数器全为零的
`server_health_record`，并且不会移动 `last_health_report_at`。

## Pod 状态

`Ready`、`Deploying` 或 `Failed`，每次上报都从快照来评估，而不是从措辞来评估：

- **Failed** —— 上报为该 pod 带了一个错误，或最近一次 ack 把它记录为失败。错误文本会成为该行的
  message。
- **Deploying** —— 该 pod 的条目在 `desired` 与 `applied` 之间不同，或只存在于 `desired` 中。
- **Ready** —— 已解析、没有失败、也没有待办。

在派生发布一个修订版本的那一刻，也会为每个 forwarding 发生了变化的 pod 写入 `Deploying`，其
message 为 `revision <n> published` —— 包括 TOML 逐字节相同、但磁盘上的材料是新的那种证书续期。

## 周期任务

两者都在 `--mode consumer` 中运行；`--mode cron` 只按固定节奏发布它们的信号，不打开数据库。

| 任务 | 信号节奏 | 配置键（默认值） | 它做什么 |
|---|---|---|---|
| `sweep_liveness` | 30 s | `liveness_interval_secs`（30） | 把沉默的服务器标记为 `Offline`，并吊销那些已经离线、注册后却从未上报过的服务器的会话。 |
| `trim_health_history` | 300 s | `health_retention_interval_secs`（300） | 删除早于 `server_health_ttl_secs` / `pod_health_ttl_secs`（各 7 天）的记录。 |

每个消费者在开始工作前都会在一行 `orchestration_job_run` 中声明这一次 tick，因此无论有多少个消费者
在运行，该任务在每个配置间隔内都只执行一次。这个声明比较的是调度 tick，而不是墙上时钟：小于或等于
信号节奏的间隔意味着“每个信号都运行一次”，更大的间隔则会让整个集群的该任务都慢下来。

## 回读

两个 unary 方法读取已存储的历史 —— `ListServerHealthHistory`（闭区间时间范围，最旧的在前，不限条
数）和 `ListPodHealthHistory`（最新的在前，`limit` 为 0 表示 500）—— 另有两个服务端流式方法实时跟
随它：`WatchServerHealth` 和 `WatchPodHealth` 以来自 `since` 的快照开场（留空时为一小时前），随后每
提交一条记录就推送一个事件，并每 `stream_keepalive_secs` 发送一次保活。两条流都在读取历史之前先订
阅总线，因此快照与第一个事件之间不会漏掉任何东西，并且会按已见到的最新上报时间去重，这也是重连不
会重复投递的原因。

对外提供的一切都是已存储的行。实时流只是让数字无需重新加载就能到达；它自己从不测量任何东西，因此
一台 `Offline` 的服务器仍会显示它最后一次发送的内容。

## 健康状况页面

![某个画布的健康状况页面：一个 Live 指示器和停在 Last hour 的时间窗口选择器，四张汇总卡片分别统计 online、degraded 和 offline 的服务器数量以及其中有多少台有上报，随后是一张服务器卡片，带 Online 徽标、最后一次上报时的连接数、峰值、那次上报的时间和上报次数，下方则是吞吐量与连接数两张图表](/img/features/health-page.avif)

`/canvas/<canvas-id>/health` 是实时的：它会打开一条 `WatchGraph` 流获取画布成员关系，并为每台服务器
各开一条 `WatchServerHealth` 流，而且会从它收到的最后一条记录处重开断掉的流，因此只需重新获取中断
的那一段。时间窗口选择器提供 1 小时 / 6 小时 / 24 小时 / 7 天。

汇总条按状态统计服务器数量，以及其中有多少台在该窗口内产生了至少一条记录。每张服务器卡片显示它的
状态徽标、该窗口内*最后一次上报时*的连接数、峰值水位中的最高值、那次上报的时间以及它覆盖了多少次
上报，然后是共用同一 x 轴的两张图表：由每次上报的字节增量堆叠而成的吞吐量，以及连接数与它后方的峰
值水位。

**Pod 事件**标签页列出在该服务器上运行的每个 pod，最新的在前，带状态、时间和 message —— 一行
`Failed` 记录的 `message` 就是 Worker 自己的错误文本。它只在被展开时才挂载，因为它每个 pod 要占一
条流。

## 配置

存储在 `orchestration` 配置键下，在 master 启动时读取一次，因此修改需要重启才生效：

| 键 | 默认值 | 作用 |
|---|---|---|
| `health_report_interval_secs` | 15 | 下发给 Worker 的间隔；决定离线阈值的大小。 |
| `health_offline_after_intervals` | 3 | 离线阈值的倍数（默认值下为 45 秒）。 |
| `degraded_grace_secs` | 60 | 一台服务器落后于 desired 修订版本多久之后才算 `Degraded`。 |
| `server_health_ttl_secs` | 604800（7 天） | 服务器记录的保留时长。 |
| `pod_health_ttl_secs` | 604800（7 天） | Pod 记录的保留时长。 |
| `liveness_interval_secs` | 30 | `sweep_liveness` 的执行节奏。 |
| `health_retention_interval_secs` | 300 | `trim_health_history` 的执行节奏。 |
| `stream_keepalive_secs` | 15 | 控制台 `Watch*` 流和 Worker 的 `WatchConfig` 上的保活间隔。 |

在 Worker 上，`--health-interval` / `GURU_HEALTH_INTERVAL_SECS`（默认 15，最小 1）是 master 不下发
间隔时的兜底。完整文档见[配置参考](/zh-cn/reference/configuration/)，一个修订版本最初如何抵达
Worker 见[发布](/zh-cn/reference/rollout/)。
