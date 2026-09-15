---
title: 发布模型
description: 一次画布编辑如何变成由 Worker 应用的配置修订版本。
---

## 从编辑到已应用的配置

每次修改都会递增画布的 generation 并发布 `CanvasDirty`；Worker 的 ack、注册和地址变化则只递增它自己服务器
配置视图上的一个计数器，所以整批 Worker 同时确认也不会在画布行上互相冲突。派生钩子会重新派生整个画布；周期性的
`derive_stale_canvases` 信号由同一个钩子消费，把这两个计数器与上一次派生记下的值比较，兜住消息丢失时漏掉的画布。这两个触发源都是消息代理上
的消息，所以这道兜底并不独立于消息代理：`CanvasDirty` 丢失的画布只能等到投递恢复，而连不上消息代理的
master 根本不会派生任何东西。

每台服务器都有**一份配置视图**，其中保存三个快照 —— `desired`、`in_flight` 和 `applied`。Worker 的流
会把 `desired` 提升为 `in_flight`，而它的 `AckConfig` 会把 `in_flight` 提升为 `applied`。

```text
canvas mutation ──▶ CanvasDirty ──┐
                                  ├──▶ derivation hook (--mode consumer)
cron: derive_stale_canvases ──────┘
                                       │
                                       ▼
                             config view: desired
                                       │ worker stream
                                       ▼
                                   in_flight
                                       │ AckConfig
                                       ▼
                                    applied
```

## 收敛

派生是收敛的：只有当目标真正开始提供服务之后，一台服务器才会切换目的地，因此发布过程中不会有修订版本
丢弃流量。正是这一性质决定了要用三快照视图，而不是单个“当前配置”字段 —— master 始终知道 Worker 实际
应用了什么。

修订版本是**按 pod** 应用的：Worker 会提交每一个准备和绑定都成功的 `[[forwarding]]`，对失败的条目保留
其原有监听器，并逐条确认结果。随后 master 把 `applied` 存为这种混合状态的快照（应用成功的 pod 用新形态，
失败的用原形态），在该服务器的视图上记录失败的 pod，并把服务器标记为 `Degraded`、把受影响的节点标记为
`Failed`，同时带上 Worker 给出的消息。完全无法应用的修订版本（TOML 无法解析、证书文件无法写入）不会改变
Worker 上的任何东西，并被记录为 `apply_error`。

## 健康状况

每个 Worker 都会按 `--health-interval`（默认 15 秒）通过 `ReportHealth` 流式上报一个 `HealthReport`：
当前运行的修订版本、自上次上报以来的上传/下载字节数和连接数（max 为峰值水位），以及每个正在运行的
forwarding 对应的一个 `PodStatus`。每次上报会生成一行 `server_health_record`，以及该上报所涉及的每个
节点各一行 `node_health_record`（即该 pod 以及派生它时经过的所有节点 —— 监听侧的 Entry/Relay，目的地
侧的 exit、relay 和负载均衡器；被多个 pod 共享的节点只生成一行，取其中最差的状态）。节点状态：`Ready`
（pod 运行的正是 `desired` 要求的内容）、`Deploying`（涉及该节点的更新修订版本已派生但尚未应用 —— 在
派生发布的那一刻就写入）、`Failed`（pod 应用失败或运行失败）。服务器状态：`Online`、`Degraded`
（落后于 `desired` 且超过宽限期，或最近一次确认的修订版本在某个 pod 上失败）、`Offline`（健康流已关闭，
或连续三个间隔没有上报 —— 即 `sweep_liveness` 任务的判定）。两份历史都是原始记录，由
`trim_health_history` 任务裁剪；`ListServerHealthHistory` / `ListNodeHealthHistory` 按时间范围读取。

这两个任务都在 `--mode consumer` 中运行，其触发信号由 `cron` 调度器在任务到期时发布。调度器除了自己的
时钟之外不保存任何状态；消费者会在一行 `orchestration_job_run` 中声明每一次运行，因此无论有多少个消费者
在运行，该任务在每个配置间隔内都只执行一次。

在控制台中，画布的**健康状况**页面会按选定的时间窗口（1 小时 / 6 小时 / 24 小时 / 7 天）读取这两份历史：
每台服务器一张卡片，显示其状态、该窗口内*最后一次上报时*的连接数以及峰值，另外还有两张图表 —— 由每次
上报的上传/下载增量得出的吞吐量，以及连接数与峰值水位的对比。页面上没有任何实时数据：每个数字都来自已
存储的上报，因此一台 `Offline` 的服务器仍会显示它最后一次发送的内容。每张卡片的 *Pod 事件* 标签页列出
该服务器各 pod 的节点健康记录，包括 `Failed` 记录所携带的 `message`；另有一张覆盖整个画布的
*节点事件* 卡片，对应记录规则的另一侧 —— 上报在派生时经过的 entry、relay、exit 和负载均衡器。两者都
只在展开时才会加载。

## 证书

带有 `tls` 块的 Entry（`sni`、DNS 提供商、`domain_id`，以及可选的 ACME 目录 —— 留空表示使用存储的
`orchestration` 配置中的 `default_acme_directory`，即 Let's Encrypt）会按 `(sni, acme_directory)`
解析出一行
`certificate` 记录。ACME 任务会创建该行记录，通过 DNS 提供商执行 DNS-01 挑战（Cloudflare：`domain_id`
是 zone id；Vercel：`domain_id` 是域名，提供商的 `account_id` 是 team id），等待 TXT 记录在公共解析器
上可见，然后存储证书链和加密后的私钥。在此之前，该 Entry 背后的 pod 只是一条 `invalid_pods` 条目
（“certificate for <sni> is pending”），而绝不是服务器故障。续期发生在到期前 30 天；续期会递增该行的
版本号，而由于每个快照都会固定其 TOML 所引用的版本，因此每台提供该证书服务的服务器都会收到一个新的修订
版本 —— 文件路径不变，内容更新。

基于 TLS 或 QUIC 的中继监听器改用**内部 CA**：`manage-tool orchestration init-ca` 一次性创建它，派生
钩子为每个中继 pod 签发一张 30 天的叶证书（SAN 为 `<pod-key>.relay.guru.internal`），
`rotate_relay_certificates` 任务在到期前十天轮换叶证书，而 Worker 只根据 `certs/ca.pem`（TOML 中的
`relay_ca`）校验中继对端。DNS 提供商、证书和 CA 都由 Admin 通过运维 API 管理（`CreateDnsProvider`、
`ListCertificates`、`RetryCertificate` 等）；密钥材料使用 `GURU_MASTER_KEY` 加密，且从不返回。

控制台的 **TLS** 页面是这一切面向 Admin 的一侧：DNS 提供商在这里创建、编辑（API 令牌留空则保留已存储的
令牌）和删除，证书表格则显示每行的状态、解析出的提供商、带到期提示的有效期窗口，以及失败时的
`last_error`，每行都带有 *重试*（清除失败状态或强制续期）和 *删除*。它从不签发证书：只有当派生任务读取
到某个 Entry 的 `tls` 块之后，对应的记录行才会出现，而那个块是在画布编辑器的 Entry 节点上编辑的。

## 查看派生出的配置

`manage-tool` 会打印 master 为某台服务器派生出的确切 TOML：

```sh
cargo run -p manage-tool -- \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  orchestration export-config --server <orchestration_server key>
```

独立模式的 Worker 背后是同一个模型：打印出来的文件可以直接交给 `guru-worker --config`。

控制台展示的是同一份 TOML —— 服务器面板中的 *Worker 配置* —— 就放在它所属的发布读数旁边：desired、
in-flight 和 applied 三个修订版本及其时间戳、`derive_error` 和 `apply_error`、这台服务器正在等待的其他
服务器（已解析为名称），以及以 pod、监听地址和错误三列表格呈现的 `invalid_pods`。`ForgetServerApplied`
也在那里，仅限 Admin 且需要二次确认，因为它宣告一台可能仍在提供服务的服务器已经死亡。

## 转发形态

## 监听器标识

收敛过程按 `(server, port, protocol)` 匹配监听器，从不按地址匹配。服务器的地址是从它的 Worker 学到的
（或由运维人员固定指定），并且只决定中继*拨号*的目标；修改其中一个地址会重新派生所有指向该服务器的
目的地，却不会触动无缝切换协议，因为依赖方引用的那个监听器并没有变化。按 pod 的派生失败会以 `bind:port`
命名该 pod 的监听器（通配绑定为 `[::]:port`），而目标服务器尚无已知地址的中继会在那里报告为
`server … has no address yet`。

监听器发生移动的 pod（换了端口，或中继协议变更导致落地端口重新分配）会同时提供新旧两个监听器，直到
所有依赖方都切换完毕。Worker 按 tag 区分监听器，所以被保留的旧监听器在 TOML 里以其套接字命名，
如 `osaka-hop (9443/relay_tcp)`，并在无人再指向它时连同这个 tag 一起撤下。

每个转发都把一个监听器与一个目的地配对：

- **监听器** —— `raw`、`tls`，或一个入向中继。
- **目的地** —— 直接 **exit**、经由 TLS-over-TCP 或 QUIC 到另一个节点的 **relay**，或一个**负载均衡**
  分组。

两端都支持 PROXY protocol v1 和 v2。
