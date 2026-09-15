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
| `consumer` | 所有 AMQP 钩子：配置推导钩子，以及全部五个周期任务 |
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

每条转发都有一个**监听器**和一个**目标**。

- 监听器：`raw`、`tls`，或一个入站中继。
- 目标：直接**出口**、通过 TLS-over-TCP 或 QUIC **中继**到另一个节点，或一个**负载均衡**组。

两端都支持 PROXY protocol v1 和 v2。

### 服务器、Pod 与地址

一台**服务器**就是一台运行 `guru-worker` 的机器。一个 **pod** 是某台服务器上的一个监听端口 ——
也就是某条规则的套接字。你的入口规则就是你添加的 pod（比如为 SOCKS 入口添加 `1080`）。pod 默认
绑定主机的所有地址；也可以限制为仅 IPv4，或固定到某一个网卡。

每台服务器还自带一个**通用 pod（universal pod）**：其他服务器的流量在这里落地，无需为每条规则
单独画一个 pod。把你的入口 pod 连到**负载均衡（分发）**节点的通道句柄上（所有规则共用一种策略和
一种中继协议），再把它打包（bundle）到中转服务器的通用 pod，然后把这些再打包到一个**负载均衡
（聚合）**节点上，该节点会为每条规则生成一个带颜色的输入，用于连接到出口。每条规则都是一条在
整条路径上拥有自己颜色的*通道*；一个 bundle 就是一根承载所有通道的粗线。在幕后，控制平面会生成
真实的 pod、中继和负载均衡器（即 "lanes"）—— 每条规则在每台中转服务器上的落地 pod 都会出现在该
服务器的面板中，端口可以编辑。这些负载均衡节点依然可以在通道旁接受手工绘制的成员；一个被打包
*进来*的分发节点会把一切重新扇出（构成第二层，或一种嵌套策略）；而当某条规则只需要一跳、不需要
任何均衡时，也可以把入口 pod 直接画到某台服务器的通用 pod 上。

没有人需要手动填写服务器的 IP。Worker 在注册时（以及此后每分钟，若地址发生变化）会上报自己的
公网 IPv4/IPv6 和网卡地址，master 会记住注册请求来自哪里，其他服务器则拨向由此得出的地址 ——
优先 IPv4。只有当学习到的地址不适用于你的网络时（NAT、覆盖网络），才需要在服务器上固定一个
地址；或者当某个 pod 需要以不同方式被访问时，只在该 pod 上固定。

## 模块

业务逻辑位于 `modules/`，每个功能一个 crate：

| 模块 | 范围 |
|---|---|
| `auth` | 账户、会话、API 密钥、RBAC |
| `orchestration` | 画布、服务器、节点、边；拓扑校验、配置推导、Worker 发布 |
| `notify` | 通知模块 —— 已从 `base` 生成骨架，尚未实现 |
| `base` | 共享基础设施，以及所有模块共同遵循的布局 |

## 技术栈

基于 Tokio 的 Rust 2024、[`wakuwaku`](https://crates.io/crates/wakuwaku) +
[`kanau`](https://crates.io/crates/kanau)（一切皆 `Processor`）、通过 Tonic 提供的 gRPC、用于
存储的 SurrealDB（Schema 位于 `database/`，由 surrealkit 管理）、用于缓存的 Redis、用于模块间
事件的 AMQP、用于链路追踪的 OpenTelemetry，以及 `typescript/` 下共享同一份生成的 API 客户端的
Bun 工作区。

## 下一步

- [本地开发](/zh-cn/guides/local-development/) —— 在一台机器上启动整套技术栈。
- [节点](/zh-cn/reference/nodes/) —— 画布上的每种节点类型，逐个句柄讲解。
- [架构](/zh-cn/reference/architecture/) —— crate 的职责与分层规则。
- [发布模型](/zh-cn/reference/rollout/) —— 一次画布编辑如何抵达 Worker。
- [独立 Worker 部署](/zh-cn/guides/independent-worker/) —— 不依赖 master，仅用 TOML 文件运行
  一个 Worker。
