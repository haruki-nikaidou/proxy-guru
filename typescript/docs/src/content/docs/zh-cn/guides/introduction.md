---
title: 简介
description: Proxy Guru 是什么、由哪些组件构成，以及流量如何在整个代理网络中流动。
---

Proxy Guru 是一套托管式 TCP/TLS 代理网络。运维人员在**画布**上设计拓扑，控制平面据此为每台
服务器推导出一份配置，数据平面的 Worker 则自动获取每个新的修订版本。

## 两个平面

**控制平面** —— `bin/guru-master`。一个二进制文件，通过 `--mode`（`GURU_WORKER_MODE`）在四种
模式之间选择：

| 模式 | 职责 |
|---|---|
| `dashboard_grpc` | 供前端调用的运维 API |
| `workers_grpc` | Worker API，以及配置视图轮询器 |
| `consumer` | 所有 AMQP 钩子：配置推导钩子，以及全部六个周期任务 |
| `cron` | 时钟：为每个到期的周期任务发布一次执行信号 |

后两者是有意被拆成两半的同一项工作。`cron` 不读取任何配置，也不打开数据库连接；收到信号的
`consumer` 会认领这次运行并完成实际工作，因此一次清扫、一次存活检查或一次证书续期的扩展与
故障转移方式，和一次画布编辑完全相同。四种模式都需要消息代理（broker）。

**数据平面** —— `bin/guru-worker`。负责终结监听器并转发流量。它既可以基于 TOML 文件独立运行
（收到 `SIGHUP` 时重新加载），也可以运行在 agent 模式下，从 master 流式获取配置。

Worker 的配置模型本身位于 `lib/guru_worker_config`，由两个平面共享：master 负责推导它，Worker
负责消费它。由文件驱动的 Worker 完全不需要控制平面 —— 参见
[独立 Worker 部署](/zh-cn/guides/independent-worker/)。

## 拓扑术语

一棵画布树持有一张 **Pod 图**：

- **服务器**是一台运行 `guru-worker` 的机器，也是在它上面运行的 Pod 的集合。
- **Pod** 是某台服务器上的一个监听。*客户端* Pod 直接接受客户端连接（裸 TCP，或用 ACME 证书终结 TLS，
  可以接收 PROXY）；*中继* Pod 接受其他 Pod 经由 TCP、TLS 或 QUIC 中继过来的流量。
- **出口**是转发网络之外、流量离开的 `host:port`。
- **边**是 Pod 流量的一条去向 —— 去往某个中继 Pod（按该 Pod 的接入方式拨号），或去往某个出口。
- Pod 的**路由**在它自己的边之间按权重负载均衡，或分档故障转移，可以任意嵌套。

比如十条规则从一台机器进来，要分散到四台中转服务器上：那就是十个客户端 Pod，每个都有四条边，分别连到它在
每台中转服务器上自己的中继 Pod，而这些中继 Pod 的边再通往出口。画布会把选法相同的四个负载均衡画成一个分流器，
把四十条边画成几条总线；每一种卡片见[节点](/zh-cn/reference/nodes/)，图的画法和编辑方式见
[画布](/zh-cn/reference/canvas/)。

Pod 默认绑定主机的所有地址；也可以限制为仅 IPv4，或固定到某一个网卡。保存时不填端口的 Pod 会分配到
40000–59999 之间的一个空闲端口。

没有人需要手动填写服务器的 IP。Worker 在注册时（以及此后每分钟，若地址发生变化）会上报自己的
公网 IPv4/IPv6 和网卡地址，master 会记住注册请求来自哪里，其他服务器则拨向由此得出的地址 ——
优先 IPv4，除非边自己指定了 IPv4 或 IPv6（在总线上可以一次改全部边）。只有当学习到的地址不适用于你的
网络时（NAT、覆盖网络），才需要在服务器上固定一个
地址；或者当某个 pod 需要以不同方式被访问时，只在该 pod 上固定。

## 模块

业务逻辑位于 `modules/`，每个功能一个 crate：

| 模块 | 范围 |
|---|---|
| `auth` | 账户、会话、API 密钥、RBAC |
| `orchestration` | 画布、服务器和 Pod 图；图的检查、配置推导、Worker 发布 |
| `notify` | 通知模块 —— 已从 `base` 生成骨架，尚未实现 |
| `base` | 共享基础设施，以及所有模块共同遵循的布局 |

## 技术栈

基于 Tokio 的 Rust 2024、[`wakuwaku`](https://crates.io/crates/wakuwaku) +
[`kanau`](https://crates.io/crates/kanau)（一切皆 `Processor`）、通过 Tonic 提供的 gRPC、用于
存储的 PostgreSQL（sqlx；migration 位于 `migrations/`）、承载运维 API 实时 `Watch*` 流的
Redis pub/sub、用于模块间事件的 AMQP、用于链路追踪的 OpenTelemetry，以及 `typescript/` 下共享
同一份生成的 API 客户端的 Bun 工作区。

## 下一步

- [本地开发](/zh-cn/guides/local-development/) —— 在一台机器上启动整套技术栈。
- [画布](/zh-cn/reference/canvas/) —— Pod 图、画布如何画它，以及每一种编辑操作。
- [节点](/zh-cn/reference/nodes/) —— 服务器、Pod、出口、分流器和聚合器，逐张卡片讲解。
- [架构](/zh-cn/reference/architecture/) —— crate 的职责与分层规则。
- [发布](/zh-cn/reference/rollout/) —— 一次编辑如何抵达 Worker。
- [健康监控](/zh-cn/features/health-monitor/) —— Worker 上报什么，以及状态是如何判定的。
- [基于 DNS 的 ACME](/zh-cn/features/acme-dns/) —— 为 TLS Pod 签发的 DNS-01 证书，以及中继 CA。
- [独立 Worker 部署](/zh-cn/guides/independent-worker/) —— 不依赖 master，仅用 TOML 文件运行
  一个 Worker。
