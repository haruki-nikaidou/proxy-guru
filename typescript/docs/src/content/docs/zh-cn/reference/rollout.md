---
title: 发布
description: 一次 Pod 图编辑如何变成由 Worker 应用的配置修订版本。
---

## 从编辑到已应用的配置

每次编辑都是一批经过检查的图修改（`ApplyGraph`，见[画布](/zh-cn/reference/canvas/)），它会递增根画布的 generation 并发布 `CanvasDirty`；Worker 的 ack、注册和地址变化则只递增它自己服务器
配置视图上的一个计数器，所以整批 Worker 同时确认也不会在画布行上互相冲突。派生钩子会重新派生整棵画布树；周期性的
`derive_stale_canvases` 信号由同一个钩子消费，把这两个计数器与上一次派生记下的值比较，兜住消息丢失时漏掉的画布。这两个触发源都是消息代理上
的消息，所以这道兜底并不独立于消息代理：`CanvasDirty` 丢失的画布只能等到投递恢复，而连不上消息代理的
master 根本不会派生任何东西。

每台服务器都有**一份配置视图**，其中保存三个快照 —— `desired`、`in_flight` 和 `applied`。Worker 的流
会把 `desired` 提升为 `in_flight`，而它的 `AckConfig` 会把 `in_flight` 提升为 `applied`。

```text
graph edit ───────▶ CanvasDirty ──┐
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
失败的用原形态），在该服务器的视图上记录失败的 pod，并把服务器标记为 `Degraded`、把每个失败的 pod 标记为
`Failed`，同时带上 Worker 给出的消息。完全无法应用的修订版本（TOML 无法解析、证书文件无法写入）不会改变
Worker 上的任何东西，并被记录为 `apply_error`。

## 健康状况

发布只是故事的一半：Worker 是否真的在运行已发布的内容，由健康流来回答。每个 Worker 按间隔推送一个
`HealthReport`，每次上报会生成一行服务器记录以及每个 pod 各一行记录，而它得出的结论 —— 服务器的
`Online`、`Degraded`、`Offline`，pod 的 `Ready`、`Deploying`、`Failed` —— 就是控制台展示的内容。其
中有两个结论是由这条流水线写入的，而不是由某次上报写入的：应用失败时 `AckConfig` 会把服务器标记为
`Degraded`，而一次派生会在发布的那一刻把每个发生变化的 pod 标记为 `Deploying`。字段、阈值和周期任务
见[健康监控](/zh-cn/features/health-monitor/)。

## 证书

一个 TLS 客户端 Pod 会按 `(sni, acme_directory)` 解析出一行 `certificate` 记录，而基于 TLS 或 QUIC
的中继监听器改用内部 CA。两者在这里之所以重要，原因是同一个：快照会固定它的 TOML 所引用的每张证书
的版本，因此续期其中一张就会为每台提供它服务的服务器产生一个新的修订版本，即便 TOML 的字节并没有变
化；而证书尚未签发的 pod 会作为一条 `invalid_pods` 条目被排除在其服务器的配置之外，却不会让该服务器
故障。签发任务、DNS 提供商和中继 CA 见[基于 DNS 的 ACME](/zh-cn/features/acme-dns/)。

## 查看派生出的配置

`manage-tool` 会打印 master 为某台服务器派生出的确切 TOML：

```sh
cargo run -p manage-tool -- --database-url "$GURU_DATABASE_URL" \
  orchestration export-config --server <orchestration_server id>
```

独立模式的 Worker 背后是同一个模型：打印出来的文件可以直接交给 `guru-worker --config`。

控制台展示的是同一份 TOML —— 服务器面板中的 *Worker 配置* —— 就放在它所属的发布读数旁边：desired、
in-flight 和 applied 三个修订版本及其时间戳、`derive_error` 和 `apply_error`、这台服务器正在等待的其他
服务器（已解析为名称），以及以 pod、监听地址和错误三列表格呈现的 `invalid_pods`。`ForgetServerApplied`
也在那里，仅限 Admin 且需要二次确认，因为它宣告一台可能仍在提供服务的服务器已经死亡。

## 监听器标识

收敛过程按 `(server, port, protocol)` 匹配监听器，从不按地址匹配。服务器的地址是从它的 Worker 学到的
（或由运维人员固定指定），并且只决定中继*拨号*的目标；修改其中一个地址会重新派生所有指向该服务器的
目的地，却不会触动无缝切换协议，因为依赖方引用的那个监听器并没有变化。按 pod 的派生失败会以 `bind:port`
命名该 pod 的监听器（通配绑定为 `[::]:port`），而目标服务器尚无已知地址的中继会在那里报告为
`server … has no address yet`。

监听器发生移动的 pod（换了端口）会同时提供新旧两个监听器，直到所有依赖方都切换完毕。Worker 按 tag 区分
监听器，而 pod 的 tag 就是它的 id，所以被保留的旧监听器在 TOML 里以其套接字命名，如
`<pod id> (9443/relay_tcp)`，并在无人再指向它时连同这个 tag 一起撤下。对仍有服务器在拨号的端口更换协议
没有无缝的路径：这样的编辑会被拒绝，需要给 pod 换一个新端口。

## 转发形态

每个 pod 变成一个 `[[forwarding]]`：它的监听器（`raw`、`tls`，或一个入向中继）和它的路由。报告了
`route_table` 能力的 Worker 以分组和上游组成的表来接收路由；旧版 Worker 则接收内联树（权重用重复成员表示，
故障转移变成 `fallback`，按客户端 IP 固定的均衡变成 `ip_hash`）。两端都支持 PROXY protocol v1 和 v2。
参见[配置参考](/zh-cn/reference/configuration/#路由表)。
