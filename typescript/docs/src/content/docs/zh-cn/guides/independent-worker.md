---
title: 独立 Worker 部署
description: 仅凭一个 TOML 文件独立运行 guru-worker —— 不需要 master、不需要数据库、不需要 API 密钥 —— 并用 SIGHUP 重新加载配置。
---

`guru-worker` 并不需要控制平面。只要指向一个 TOML 文件，它就是一个自包含的 TCP/TLS
代理：按文件描述绑定监听器、转发流量，并在收到 `SIGHUP` 时重新读取该文件。本指南部署的正是这种形态 —— 一个二进制、一个配置文件、一个 systemd
unit。

这是受支持的运行模式，而不是降级模式。代码里的分支只是对 `--master` 的一次 `match`：不带该参数时，Worker 会走
[`run_standalone`](https://github.com/haruki-nikaidou/proxy-guru/blob/main/bin/guru-worker/src/lib.rs)，
从不构造 gRPC 客户端、从不读取 `GURU_API_KEY`，也从不使用 `--state-dir`（last-known-good
文件是 Agent 模式才有的概念）。CLI 会强制这种区分：`--config` 与 `--master` 互斥，而 `--master` 必须配合 `--server`。

## 1. 先判断这是否是你要的模式

|                              | 独立模式（`--config`） | Agent 模式（`--master`） |
|---|---|---|
| 配置的唯一来源 | 节点上的文件 | 数据库中的画布 |
| 是否需要 PostgreSQL / RabbitMQ / master | 否 | 是 |
| 是否需要运维 API 密钥 | 否 | 是 |
| 变更如何生效 | 你编辑文件，然后 `SIGHUP` | master 流式下发一个修订版本 |
| 跨节点的发布顺序 | 由你自行安排 | 收敛式、按依赖排序 |
| 重启后是否沿用上一份配置 | 文件*本身*就是配置 | 来自 `--state-dir` |
| 磁盘上的 TLS 私钥与证书链 | 必需 | 必需 |

对于单个节点、气隙环境或一次性的中继、用 Ansible/Nix/Puppet 管理的跳板机，以及在笔记本上复现某个节点的行为，独立模式都是正确选择。当多个节点组成一个拓扑，并且你希望有序发布而不是手工协调一堆
`SIGHUP` 时，就该选择 Agent 模式。

:::note[两种模式读取的是同一套模型]
配置格式由 `lib/guru_worker_config` 定义，两个平面共用 —— 参见
[Worker 配置文件](/zh-cn/reference/configuration/#worker-配置文件)。文件中没有任何“仅独立模式可用”的内容，因此一个节点日后可以并入画布，而无需重写其拓扑。
:::

## 2. 前置条件

- 一台 Linux `x86_64` 主机。每个 release 提供两个二进制：使用 glibc 的发行版（较新的
  Debian/Ubuntu/RHEL）用 `x86_64-unknown-linux-gnu`，Alpine 以及任何其他 musl 发行版用把 musl libc
  静态链接进来的 `x86_64-unknown-linux-musl`（运行时不依赖宿主上的 libc）。
- `guru-worker` 二进制，来自某个 `worker-v*` GitHub release —— 如何定位资产、该挑哪一个以及需要核对什么，参见
  [使用 Docker 部署 §11](/zh-cn/guides/deploy-with-docker/#11-从-github-release-获取-worker-二进制文件)。那篇文章中控制平面相关的内容在这里都不需要。
- 对任何 TLS 或 QUIC 监听器：一份 PEM 格式的私钥和完整证书链，且**必须已经存在于主机上**。Worker
  只会从配置里的路径读取它们；两种模式下它都不会申请或生成证书。

你**不**需要 Docker、PostgreSQL、RabbitMQ、控制台、API 密钥，也不需要能连通除你自己的上游之外的任何网络。

## 3. 安装二进制

```sh
sudo install -m 0755 guru-worker /usr/local/bin/guru-worker
/usr/local/bin/guru-worker --help        # there is no --version; --help is the smoke test
```

创建一个系统组以及对应的系统用户（无家目录、无 shell），再创建一个该用户可读的配置目录。请显式创建这个组，而不要指望
`useradd` 自动派生一个 —— 它是否这么做取决于发行版的 `useradd` 默认设置（`USERGROUPS_ENAB`）—— 因为下面每一条
`chown`，以及 unit 中的 `Group=`，都要求这个组已经存在：

```sh
sudo groupadd --system guru-worker
sudo useradd --system --gid guru-worker \
  --no-create-home --home-dir /nonexistent \
  --shell /usr/sbin/nologin guru-worker      # RHEL: /sbin/nologin
sudo mkdir -p /etc/guru-worker
sudo chown root:guru-worker /etc/guru-worker
sudo chmod 0750 /etc/guru-worker
```

如果配置管理工具会重复执行，请给这两条命令都加上判断：
`getent group guru-worker || sudo groupadd --system guru-worker` 和
`getent passwd guru-worker || sudo useradd --system --gid guru-worker …`。

## 4. 编写配置

`/etc/guru-worker/config.toml` 是默认路径，所以即使 unit 不传 `--config` 也能正常工作。先从能证明数据通路可用的最小配置开始 ——
一个裸 TCP 监听器指向一个后端：

```toml
# /etc/guru-worker/config.toml
ipv6_resolve = "tolerated"

[log]
level = "info"

[[forwarding]]
tag = "edge"
listen = "0.0.0.0:8443"
listen_as = "raw"

[forwarding.to]
type = "exit"
destination = "10.0.0.5:8080"
```

一个终结 TLS 并向负载均衡组分发流量的入口如下 —— 注意证书路径都是普通文件，Worker 必须能够打开它们：

```toml
[[forwarding]]
tag = "public-https"
listen = "0.0.0.0:443"

[forwarding.listen_as.tls]
key = "/etc/guru-worker/tls/key.pem"
full_chain = "/etc/guru-worker/tls/fullchain.pem"

[forwarding.to]
type = "load_balance"
strategy = "round_robin"

[[forwarding.to.members]]
type = "exit"
destination = "10.0.0.5:8080"

[[forwarding.to.members]]
type = "exit"
destination = "backend.internal:8080"
```

每一个键、每一种监听器形态以及每一条校验规则都记录在
[Worker 配置文件](/zh-cn/reference/configuration/#worker-配置文件)中。有两点最容易让初次编写配置的人踩坑：未知键会直接导致硬错误（不存在静默默认值），并且在独立模式下，进程日志级别来自文件中的
`log.level` —— `--log-level`/`GURU_LOG_LEVEL` 是 Agent 模式的参数，在这里会被忽略。

证书和私钥必须能被服务用户读取，而私钥不能对所有用户可读：

```sh
sudo chown root:guru-worker /etc/guru-worker/tls/key.pem /etc/guru-worker/tls/fullchain.pem
sudo chmod 0640 /etc/guru-worker/tls/key.pem
```

在给它套上 unit 之前，先在前台跑一遍这个文件。程序没有“仅检查”的参数：Worker 会在绑定任何端口*之前*解析并校验配置，因此有问题的文件会立刻退出，而正确的文件会绑定监听器并持续服务，直到你用
`Ctrl-C` 中断。做这个测试时，请使用端口全部大于 1024 的配置，或者以 root 运行 —— 非特权的前台运行无法绑定 `:443`。

```sh
sudo -u guru-worker /usr/local/bin/guru-worker -c /etc/guru-worker/config.toml
# fatal: parse toml: TOML parse error at line 4, column 1 … unknown field `listenas`  ← exits
# INFO guru_worker: loaded config path=/etc/guru-worker/config.toml                   ← serving; Ctrl-C to stop
```

只有当后端确实支持 PROXY 协议时（nginx 的 `proxy_protocol`、HAProxy 的 `accept-proxy`、Envoy 的 proxy-protocol
监听器过滤器），才给 `exit` 加上 `send_proxy_protocol = "v2"`。把它发给一个普通 HTTP
服务器会让每个请求都因请求行格式错误而失败，用这种方式来确认数据通路是否正常只会让人更困惑。

## 5. 以 systemd 运行

```ini
# /etc/systemd/system/guru-worker.service
[Unit]
Description=guru data-plane worker (standalone)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=guru-worker
Group=guru-worker
ExecStart=/usr/local/bin/guru-worker --config /etc/guru-worker/config.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=2

# Ports below 1024 without running as root
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE

# The worker keeps no state of its own in this mode
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadOnlyPaths=/etc/guru-worker
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now guru-worker
```

`LimitNOFILE` 很关键：每条被代理的连接都要占用两个文件描述符，默认的 1024
是你在有负载时一定会撞上的天花板，而不是什么安全余量。这里特意没有 `StateDirectory=` —— 独立模式不写入任何内容。

## 6. 验证

```sh
# 1. Started, and one "listener started" line per [[forwarding]]
journalctl -u guru-worker -n 20 --no-pager
#   INFO guru_worker: loaded config path=/etc/guru-worker/config.toml
#   INFO guru_worker::supervisor: listener started addr=0.0.0.0:8443 transport=Tcp tag=edge

# 2. The socket is actually bound. Match the port, not the process name:
#    unprivileged `ss -p` hides process info for other users' sockets.
sudo ss -ltnp 'sport = :8443'          # QUIC/UDP listener: sudo ss -lunp 'sport = :8443'

# 3. Traffic reaches the backend through the listener
curl -sv telnet://127.0.0.1:8443 </dev/null    # or exercise the real protocol
```

如果 unit 在运行，但 `ss` 里缺少某个监听器，请看日志：应用配置时的错误会以 `<tag>: <reason>` 的形式点出出问题的条目 ——
`Address already in use`、在特权端口上的 `Permission denied`（缺少 `CAP_NET_BIND_SERVICE`），或者某个打不开的证书文件。

## 7. 修改配置：先编辑，再重新加载

```sh
sudoedit /etc/guru-worker/config.toml
sudo systemctl reload guru-worker          # sends SIGHUP
journalctl -u guru-worker -n 5 --no-pager  # "config reloaded"
```

重新加载是全有或全无的，并且不会打断本来不必打断的流量：

- 文件会先被解析、校验，并且所有新的套接字都会在任何变更生效**之前**完成绑定。如果任何一步失败，什么都不会被改动，日志会输出
  `reload failed` 或 `reload apply failed; keeping running config` —— Worker 继续按旧拓扑提供服务。
- 在本次变更中保留下来的监听器会热替换其编译后的配置；下一条连接就会使用新的目标地址。已有连接会沿用旧路径直到关闭。
- 被删除的监听器会停止接受新连接，但其正在处理的连接不会被强行终止。
- 如果某个监听器被同一 `ip:port` 上的另一个监听器取代，会先关闭旧的，并且重新加载会等待套接字真正释放，因此替换者会在同一次重新加载内完成绑定。QUIC
  端点会先排空其活跃连接，若超过三秒仍未完成则被强制关闭 —— 一个正在被接管的地址不能被无限期占用。如果替换者最终仍然绑定失败，则本次重新加载停掉的所有监听器都会被重新拉起。

由于证书是在应用配置时解析的，证书续期同样只需重新加载 —— 无需重启，也不会丢连接。把它接入你的 ACME 客户端即可，例如 certbot：

```sh
# /etc/letsencrypt/renewal-hooks/deploy/guru-worker.sh
#!/bin/sh
install -o root -g guru-worker -m 0640 \
  /etc/letsencrypt/live/example.com/privkey.pem   /etc/guru-worker/tls/key.pem
install -o root -g guru-worker -m 0644 \
  /etc/letsencrypt/live/example.com/fullchain.pem /etc/guru-worker/tls/fullchain.pem
systemctl reload guru-worker
```

`SIGTERM`/`SIGINT`（也就是 `systemctl stop`、`systemctl restart`）会停止所有监听器并关闭进程。注意日志级别只在启动时读取一次：修改
`log.level` 需要重启，而不是重新加载。

## 8. 故障排查

| 现象 | 原因 |
|---|---|
| `fatal: read /etc/guru-worker/config.toml: No such file or directory` | 没有传 `--config`，而默认路径上也没有文件；或者服务用户读不到它 |
| `fatal: parse toml: TOML parse error at line N …  unknown field …` | 某个键拼错了；每张表都会拒绝未知字段，并且报错会指出具体行号 |
| `fatal: duplicate listener 0.0.0.0:443 (edge)` | 两个条目使用了相同的 `ip:port` 和传输方式。只有一个 TCP 条目和一个 QUIC 条目可以共用同一端口 |
| `fatal: forwarding edge relay to tls/quic requires sni` | 某个 `tls`/`quic` 中继跳没有配置 `sni`，无论嵌套在哪一层 |
| `fatal: edge: Permission denied (os error 13)` | 使用了特权端口但没有 `AmbientCapabilities=CAP_NET_BIND_SERVICE`（应用配置的错误都以该条目的 `tag` 作为前缀） |
| `fatal: tls-term: error:80000002:… calling fopen(/etc/guru-worker/tls/key.pem, r)` | 某个证书或私钥路径 Worker 打不开 —— 这是 OpenSSL 报的错，所以信息很啰嗦，但会指明文件名 |
| `WARN config lint … used ip_hash for load balancing` | 在没有 `receive_proxy_protocol` 的监听器下使用了 `ip_hash`。对位于代理之后的 `raw`/`tls` 监听器，这是真实问题 —— 所有连接都会把代理的地址哈希到同一个成员上。对 `relay` 监听器则无害，因为它总会自己解码 PROXY 头，从而哈希真实客户端地址；这条 lint 并不区分两者 |
| 重新加载好像什么都没发生 | 查看日志里是否有 `reload failed` —— 此时仍在用旧配置服务。同时确认 `ExecReload` 发送的是 `SIGHUP`，而不是 `SIGUSR1` |
| 后端看到的是代理的 IP，而不是客户端的 IP | 在 `exit` 上加 `send_proxy_protocol`（并让后端能解析它）；如果 Worker 本身也位于代理之后，再设置 `receive_proxy_protocol` |

## 9. 日后把节点纳入管理

独立模式和 Agent 模式是同一个二进制在读同一套模型，因此迁移只是改一个 unit 文件，而不是重写配置：

1. 在画布中把该节点建模为一台服务器，让 master 派生它的配置。
2. 对比两份文件 —— `manage-tool orchestration export-config --server <key>`
   会打印画布当前派生出的内容，也就是 master 将要下发的那份文件。（该命令需要访问数据库，所以要在运维机器上运行，而不是在 Worker 节点上。）
3. 把 `--config <file>` 换成 `--master <url> --server <key>`，通过 `GURU_API_KEY` 或 `--api-key-file` 提供 API
   密钥，并加上一个可写的 `--state-dir`，让节点在重启后能恢复它的 last-known-good 配置。
