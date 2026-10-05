---
title: 远程 Shell
description: 在控制台里对 Worker 主机执行 Shell 命令、实时查看输出，并在断线后从断开处接着读 —— 在主机自己选择开启之前，每台主机上都是关闭的。
---

远程 Shell 让 Admin 可以在控制台里对 `guru-worker` 所在的主机执行命令，并在输出产生的同时看到它。这个
功能默认完全关闭：除非 **Worker 所在的主机**自己选择开启，Worker 不会运行任何 Shell，控制台或 master
上的任何设置都改变不了这一点。

## 会话是什么

一个会话就是 Worker 主机上的一个 `bash` 进程，为该会话启动，一直保留到会话结束。会话里的每条命令都发给
同一个进程，所以状态会像在终端里一样延续：`cd /tmp` 之后执行 `pwd` 会打印 `/tmp`，`export` 和 Shell
变量在后续命令中仍然有效。新会话在 `$HOME` 中启动（当它是一个存在的目录时），否则在 `/` 中启动。

- **一次一条命令。** 在另一条命令还在运行时发来的命令会被拒绝，不会排队。控制台会在运行中的命令结束前
  禁用输入框。
- **退出码。** 每条结束的命令都会报告它的退出状态（被信号 `n` 杀死的命令为 `128 + n`）。
- **实时输出。** stdout 和 stderr 在写出的同时被推送，每个数据块都标明它来自哪个流。
- **没人看时也照常运行。** Shell 属于 Worker 进程，而不属于任何连接。关闭浏览器标签页、前端或 master
  重启、Worker 与 master 之间的链路中断，都不会让命令停下。
- **续传。** Worker 把每个会话的记录 —— 命令、输出、退出码 —— 保存在一个大小为
  `--remote-shell-buffer-bytes`（默认 1 MiB）的环形缓冲区里。每一条记录都有一个位置，重新连上的查看者
  会请求它看到的最后位置之后的全部内容。如果缓冲区已经越过那个位置绕了一圈，记录会先给出一个标明丢弃了
  多少字节的截断标记，然后从仍保留着的最旧记录继续。缓冲区只会丢弃最旧的记录：写入它从不等待慢速的
  查看者，所以一个卡住的浏览器卡不住 Shell。
- **多个查看者。** 任意多个 Admin 可以同时查看同一个会话，各自从自己的位置开始，其中任何人都可以发送下一条
  命令。

一个 Worker 上同时最多运行 `--remote-shell-max-sessions` 个会话（默认 4 个）；再打开新的会被拒绝，直到其中
某个结束。

## 它不做什么

- **不是终端。** 没有 PTY：不能用 `top`、编辑器、分页器，不能输入密码，也不能调整终端大小。命令的 stdin
  是 `/dev/null`，所以读取输入的程序会立刻读到 EOF，而不是一直等待。请使用非交互的写法
  （`systemctl --no-pager`、`journalctl --no-pager -n 100`、`apt-get -y`）。
- **不能中断。** 无法单独停止一条正在运行的命令。关闭会话是停止它的唯一办法，而这会杀死该会话启动的一切。
- **没有审计日志。** Proxy Guru 不记录谁执行了哪条命令。
- **不保存记录。** 输出只存在于 Worker 的环形缓冲区中；master 只做转发，什么都不保留，也不会写入
  PostgreSQL。
- **不会在 Worker 重启后存活。** 停止或重启 Worker 会结束所有会话，
  [自动更新](/zh-cn/guides/agent-install/#3-update-a-running-worker)也一样，因为它本身就是一次重启。

## 谁能开启它

共有四道关卡，必须全部打开：

1. **构建。** Worker 的 Cargo feature `remote-shell` 属于它的默认 feature，所以官方发布的构建和普通的
   `cargo build -p guru-worker` 都包含它。用 `--no-default-features` 构建的二进制则不包含：它完全没有
   Shell 相关的代码，如果主机仍然选择开启，它会拒绝启动，而不是默默忽略这个设置。
2. **主机。** 开启开关是 Worker 进程的本地设置 —— 主机上的一个命令行参数或环境变量，读取方式与
   `--no-self-update` 相同。它不属于 master 推送给 Worker 的配置，所以控制台和被攻破的 master 都无法打开
   它：只有本来就能修改这台主机上 Worker unit 的人才能打开。
3. **能力声明。** 选择开启的 Worker 在向 master 注册时声明能力 `remote_shell`。对于 Worker 没有声明这项
   能力的服务器，master 会拒绝所有远程 Shell 请求，控制台也不会为它显示入口。
4. **账号。** 每一次远程 Shell 调用都需要 `RemoteShell` 权限，只有 Admin 角色拥有它，而且只能来自已登录
   的控制台会话 —— API 密钥不能使用它。

### 设置

全部是 Worker 主机的本地设置，无论是否带有该 feature，每个构建中都存在：

| 参数 | 环境变量 | 默认值 |
|---|---|---|
| `--remote-shell` | `GURU_REMOTE_SHELL` | 关闭（开启开关；仅 agent 模式） |
| `--remote-shell-buffer-bytes` | `GURU_REMOTE_SHELL_BUFFER_BYTES` | `1048576`（1 MiB；每个会话保留的记录，计入输出、命令以及每条记录少量的额外开销；至少 `4096`） |
| `--remote-shell-idle-timeout` | `GURU_REMOTE_SHELL_IDLE_TIMEOUT_SECS` | `1800`（会话在没有命令运行、也没有人查看的情况下可以保留的秒数；至少 `1`） |
| `--remote-shell-max-sessions` | `GURU_REMOTE_SHELL_MAX_SESSIONS` | `4`（这个 Worker 上同时运行的会话数；至少 `1`） |
| `--remote-shell-allow-plaintext` | `GURU_REMOTE_SHELL_ALLOW_PLAINTEXT` | 关闭（允许在 `http://` 的 master 下开启；见[安全](#安全)） |

设置了开启开关时，Worker 在以下情况会拒绝启动 —— 以一行写明原因的 `fatal:` 退出：

- 二进制构建时没有 `remote-shell` feature；
- 以独立模式运行（`--config`，没有 `--master`）：Shell 要经由 master 访问，独立模式的 Worker 用不上它；
- `--master` 是 `http://` 地址，且没有设置 `--remote-shell-allow-plaintext`；
- 在 Worker 的 `PATH` 中找不到 `bash`。它在 `PATH` 中查找，而不是使用 `/bin/bash`；systemd 默认的 `PATH`
  覆盖了常见位置，而在 NixOS 上需要给 unit 一个包含它的 `PATH`，例如
  `Environment=PATH=/run/current-system/sw/bin`。

## 在主机上开启

对于用控制台的安装命令安装的 Worker（见[安装与更新 Agent](/zh-cn/guides/agent-install/)），把开启开关加到
实例的环境文件中，然后重启该实例：

```sh
echo 'GURU_REMOTE_SHELL=1' | sudo tee -a /etc/guru-worker/hk-1.env
sudo systemctl restart guru-worker@hk-1
```

默认值不合适时，其他设置也写进同一个文件，例如 `GURU_REMOTE_SHELL_IDLE_TIMEOUT_SECS=600`。重新执行安装命令
会重写这个文件，所以之后需要再把这些行加回去。

你自己编写的 unit —— 例如[从独立模式纳入管理](/zh-cn/guides/independent-worker/#9-日后把节点纳入管理)的
那种 —— 在命令行上传参数，或在环境中设置变量：

```ini
ExecStart=/usr/local/bin/guru-worker --master https://guru.example.com --server <key> --remote-shell
```

Worker 重新注册后，控制台里该服务器的 **Agent** 区域会出现**远程 Shell** 按钮。要关闭 Shell，删掉设置并
重启：Worker 注册时不再声明该能力，按钮随之消失，而重启本身已经结束了所有会话。

## 在控制台中使用

打开画布，点击服务器，在它的面板里找到 **Agent** 区域。对于 Worker 声明了该能力的每台服务器，Admin 都会
在那里看到**远程 Shell** 按钮。点击后会打开 *远程 Shell · &lt;服务器名&gt;*：

- **会话**列出该 Worker 上正在运行的会话。**新建会话**启动一个，**连接**加入一个已经在运行的会话 ——
  你之前打开的，或者另一位 Admin 的 —— 并回放缓冲区仍保留的那部分记录。
- 在输入框中输入命令并点击**运行**。每条命令都与其输出一起显示，结束时后面跟着 `exit <code>`；命令运行
  期间输入框保持禁用。
- **关闭会话**在确认后杀死该会话及其启动的一切。

关闭对话框或标签页只是断开查看：会话和正在运行的命令都会继续，再次连接就能从当前的记录接着看。断掉的
连接会自动从浏览器收到的最后位置续上。

会话结束时，记录中会写明原因：被运维人员关闭、空闲超时、Shell 退出（例如执行了 `exit`），或 Worker
停止。

## 生命周期

会话以下面其中一种方式结束：

| 结束方式 | 何时 |
|---|---|
| 关闭 | 有人点击了**关闭会话**。Worker 向该会话的整个进程组发送 `SIGKILL`，所以 Shell 以及它在该组中启动的所有进程会一起消失。 |
| 空闲超时 | 没有命令在运行**并且**没有任何查看者连接，持续了 `--remote-shell-idle-timeout`（默认 30 分钟）。只要有一条命令在运行或有一个查看者在看，会话就会一直保留。 |
| Shell 退出 | Shell 自己结束了，例如执行了 `exit`。如果当时有命令在运行，会先报告它的退出状态。 |
| Worker 停止 | Worker 关闭或重启了 —— 包括自动更新。所有会话都会被杀死，也不会被恢复。 |

被命令移入独立进程组的进程 —— `setsid`、自行脱离的守护进程 —— 不属于会话的进程组，会在关闭后继续存活。
不过在安装脚本的 unit 下，它仍会随 Worker 一起结束：停止或重启 `guru-worker@<unit>` 时，systemd 会杀死该
unit 控制组中的一切。

## 数据如何传输

主动拨号的总是 Worker；master 从不连接 Worker。选择开启的 Worker 在注册后会向 master 额外打开一条流，
用会话的 refresh key 认证，并像配置流一样被下一次注册取代；流断开时 Worker 会自行重新打开。master 在这条
流和控制台的调用之间、跨副本地转发请求和记录条目，不保存其中任何内容。浏览器以 Server-Sent Events 的形式
接收记录，事件的 id 就是记录中的位置，这正是重新连接时能精确请求漏掉部分的原因。

## 安全

开启远程 Shell 会让控制台成为进入这台主机的入口。请逐台主机做决定，只在需要的主机上开启。

- **它以 Worker 的用户身份、按该用户的权限运行。** 命令以 Worker 运行时所用的用户执行，环境、资源限制和
  沙箱都与 Worker 相同。在安装脚本的 unit 下，这个用户是 `guru-worker` 系统用户，受 unit 的
  `ProtectSystem=strict`、`ProtectHome=yes`、`PrivateTmp=yes` 和 `NoNewPrivileges=yes` 约束 —— 所以
  `sudo` 和 setuid 程序无法提权，`/tmp` 是该 unit 私有的，可写的只有状态目录和实例自己的 `bin/`。unit 的
  `CAP_NET_BIND_SERVICE` 也会被继承，所以命令可以绑定 1024 以下的端口。如果你自己编写的 unit 以 root
  运行 Worker，那就等于给了每个 Admin 一个 root Shell。
- **一个 Admin 账号就是每台已开启主机上的 Shell。** 以 Admin 身份登录的人 —— 或者窃取了 Admin 会话的人
  —— 可以在每台已开启的主机上执行命令。请让 Admin 的数量尽量少、密码尽量强；Maintainer 和 Observer 账号
  无法使用 Shell。
- **密钥不在环境里，但并非够不着。** `GURU_API_KEY` 会从 Shell 的环境中移除，但 Worker 用户能读的任何
  文件在 Shell 里都能读。在安装脚本的布局下，这包括 `/etc/guru-worker/<unit>.env` —— 其中有该实例的
  密钥，并且同一主机上其他所有实例的密钥也都能读到，因为它们对 `guru-worker` 组都是可读的。拿到 Shell 的
  人还能替换实例可写的 `bin/` 中的二进制，unit 会在下次启动时运行它。请把远程 Shell 视为 Worker 在这台
  主机上的全部权力，而不是一个只读视图。
- **默认拒绝明文。** 命令及其输出会经过 Worker ↔ master 的链路，所以设置了开启开关、而 `--master` 是
  `http://`（h2c）的 Worker 会拒绝启动。请改为让 Worker 指向 `https://` 的 master。
  `--remote-shell-allow-plaintext` 会解除这项拒绝；只在链路底层已经加密时才设置它 —— WireGuard 或 SSH
  隧道、你像信任 TLS 一样信任的私有网络。这项检查只覆盖 Worker 自己的链路：控制台也请通过 HTTPS 提供，
  因为每条命令及其输出同样会经过浏览器的连接。
- **什么都不记录。** 没有谁执行了什么的审计记录，记录也不会被保留，所以命令做了什么，唯一的记录就是主机
  自己的日志。
