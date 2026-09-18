---
title: 通知
description: 一次服务器或 Pod 的健康状态变化如何变成一封邮件或一条 Telegram 消息、谁会收到它，以及 notifier 模式做什么。
---

健康监控决定集群的状态*是什么*。通知决定*谁会听到它*：master 记录下的每一次状态变化，都可以按收件
人自己选定的语言，变成一封邮件或一条 Telegram 消息。

在运维人员主动开启之前，什么都不会被发送。全新安装根本没有任何通知数据行，而缺失的数据行意味着“没
有任何事件”，因此部署这个功能不会通知任何人。

## 两种作用域

两者都以画布为键 —— 也就是一条通知所关乎的那个工作区。

| 作用域 | 数据行 | 谁来编辑 | 目标地址 |
|---|---|---|---|
| 工作区 | `notify_canvas_setting` | 读取需要 `ViewWorkspace`，替换需要 `EditWorkspace` | 一组邮件地址和一组 Telegram 会话 id，由所有关注该画布的人共用 |
| 个人 | `notify_account_setting` | 账号自己（必须是人类会话；API 密钥从来不行） | 该账号自己的邮件地址，以及一个 Telegram 会话 id |
| 个人默认 | `notify_account_default` | 账号自己 | 同上，用于该账号没有单独数据行的每个画布 |

每一行都带有一种语言和一组事件种类。工作区行上的目标地址**不是**对账号的一次扇出：它们就是普通的地
址和会话 id，因此一个值班别名或一个团队频道无需在这套安装里拥有账号。

个人设置是按画布解析的：为该画布拥有数据行的账号由那一行服务，没有的则由它的默认行服务 —— 绝不会
两者同时生效。正是这一点让“默认全都要，只有这一个工作区用日语”无需逐个画布去改。

控制台在某个画布侧边栏的**通知**页里同时编辑这两者。工作区卡片对 Observer 是只读的；个人卡片始终可
编辑，因为它只会写入调用者自己的那一行 —— 账号 id 来自会话，绝不来自请求。

## 六种事件种类

| 种类 | 何时发送 |
|---|---|
| `server_online` | 某台服务器的状态变成了 `Online` |
| `server_degraded` | 某台服务器的状态变成了 `Degraded` |
| `server_offline` | 某台服务器的状态变成了 `Offline`（上报中断、流被关闭，或存活检测清扫） |
| `pod_ready` | 某个 Pod 的状态变成了 `Ready` |
| `pod_deploying` | 某个 Pod 的状态变成了 `Deploying`（为它发布了一个新的修订版本） |
| `pod_failed` | 某个 Pod 的状态变成了 `Failed`；Worker 的 message 会作为该通知的详情一起带上 |

这些拼写在数据库的 `CHECK`、API 和配置文档中完全一致。

## 只有变化才会被通报

Worker 默认每 15 秒上报一次，而每次上报都会为每个 Pod 写入一行。若按数据行来通知，就等于每个 Pod
每个间隔一条消息，因此扇出自己记住了它上一次就每个对象通报过什么 —— `notify_server_state` 和
`notify_pod_state`，每台服务器一行、每个 Pod 一行 —— 并丢弃一切没有移动过的东西：

- 存储的状态与新状态相同 → 什么都不发送，该行的 `changed_at` 也不动；
- 完全没有存储的状态（这个对象第一次被看到）→ 记录下该状态，并且**什么都不发送**。否则安装这个功
  能、或者新增一台服务器，就会把整个集群一次性通报出去；
- 存储的状态不同 → 每个受众一条通知，并且该行向前推进。

删除一台服务器或一个 Pod 会通过外键一并删掉它的行；在写入与扇出之间消失了的对象会被丢弃，而不是被
重试。

## 一条通知如何流转

```text
健康写入 ──▶ orchestration 发布 `health_changed`（事实 + 标签）
                        │
                        ▼
              --mode consumer：扇出
                 · 丢弃并非变化的事实
                 · 读取设置，解析出各个受众
                 · 为每个受众发布一条通知
                        │
                        ▼
              --mode notifier（恰好一个）：投递
                 · 按该通知的语言把主题 + 正文渲染一次
                 · 发送到该通知携带的每个目标地址
```

这些事实自带标签 —— 服务器或 Pod 的名称、所属画布及其名称 —— 因此 notifier 完全不打开数据库，而一
条关于某台此后被改名的服务器的通知，读起来仍然是它当时的样子。

这样拆分是有意的。扇出是一个数据库读取方，与其他消费者一同扩展；投递侧要与 SMTP 中继和 Telegram 通
信，绝不能被运行两份。

## `--mode notifier`

`guru-master` 的第五种运行模式，也是本工作区唯一要求必须单实例的一种：

```sh
guru-master --mode notifier \
  --database-url "$GURU_DATABASE_URL" \
  --amqp-uri 'amqp://guru:guru@127.0.0.1:5672/'
```

- **恰好一个实例。** 在绑定队列之前，它会取得一把会话级的 PostgreSQL advisory lock；第二个实例会以
  非零码退出，并给出 `another notifier already holds the advisory lock; run exactly one`。这把锁随
  连接消亡而释放，因此被杀掉的 notifier 绝不会挡住它的后继者，而一次与前任重叠的重启会快速失败，而
  不是把消息发两遍。
- **不需要 master key，也不需要 Redis。** 它不解密任何东西，也不发布任何东西，因此既不需要
  `GURU_MASTER_KEY` 也不需要 `REDIS_URL` —— 只需要数据库（用于那把锁，以及它在启动时读取的
  `notify` 配置）和消息代理。
- 它会把自己最终拿到的渠道记进日志，这是检查它的机密是否到位的最快方式：

```text
INFO guru_master: notification channel channel="email: configured"
INFO guru_master: notification channel channel="telegram: disabled (no GURU_TELEGRAM_BOT_TOKEN)"
```

notifier 停机并不会丢掉通知：它们会在 `guru_notify_health_notify_group` 和
`guru_notify_health_notify_personal` 中排队，直到它回来。

## 渠道及其机密

这两个机密**只来自环境变量**，从不属于配置文档 —— Admin 可以在控制台里读取那份文档，而一个机器人
令牌不是一项设置：

| 变量 | 渠道 | 未设置意味着 |
|---|---|---|
| `GURU_SMTP_PASSWORD` | 邮件 | 以不认证的方式使用该中继（这正是 localhost 上的中继所期待的） |
| `GURU_TELEGRAM_BOT_TOKEN` | Telegram | Telegram 被禁用；点名了某个会话 id 的通知会被记入日志并跳过 |

邮件通过一条带连接池的 SMTP 连接发出，并用 STARTTLS 升级，除非 `smtp_starttls` 被关掉（那样就是明文
SMTP，仅适用于 localhost 上的中继或测试接收端）。Telegram 是针对 `telegram_api_base` 的每个会话一次
`sendMessage` 调用；令牌位于 URL 路径中，因此任何请求 URL 都绝不会被记进日志。

两个渠道都会把失败的发送重试 `delivery_attempts` 次，两次之间相隔 `delivery_retry_delay_secs`。

## 投递是尽力而为的

对象的状态行是在通知被发布时向前推进的，也就是在它被发出之前。因此一条因 SMTP 或 Telegram 中断而丢
失的通知**不会**在 `delivery_attempts` 之后继续重试：失败的发送会被记入日志，剩下的目标地址仍会照样
尝试。重新入队投递会让扇出再跑一次，而此时那个对象看起来已经没有变化，于是关于它的下一条消息就是它
的下一次真实变化。

两个 `consumer` 副本也可能在一个很窄的窗口内同时观察到同一次转变，并发布重复的通知。这是被接受的：
为通知而给每个对象加一把锁并不值得，而重复的那一条也只是把同一件真事说了两遍。

通知是一种便利，不是控制平面的记录。集群实际运行什么，由发布方已经提交的那些数据行决定，而健康状况
页面始终展示当前的事实。

## 配置

一个 `app_config` 键 `notify`，和其他所有键一样在启动时读取一次：

```sh
manage-tool config set notify '{
  "smtp_host": "smtp.example.com",
  "smtp_port": 587,
  "smtp_starttls": true,
  "smtp_username": "guru@example.com",
  "smtp_from": "guru <noreply@example.com>",
  "default_language": "en"
}'
```

`smtp_host` 为空 —— 也就是默认值 —— 会彻底禁用邮件。完整的字段清单见[配置参考](/zh-cn/reference/configuration/#模块配置)。
保存它（通过 CLI，或在控制台的**管理 → 配置**中）会在 master 重启后生效；notifier
只在启动时构建它的传输层一次。

## 发送一条测试通知

通知页上的**发送测试通知**按钮会向调用者自己的渠道发布一条通知，按这些渠道在该画布上生效的样子，并
忽略他们订阅了哪些种类 —— 重点在渠道，而不在订阅。它会走完整条 发布 → notifier → 发送 的路径，因此
一条真正到达的消息就证明了 notifier、它的机密和中继三者都能正常工作。

当调用者根本没有配置任何渠道时，它会返回 `FAILED_PRECONDITION`：请先打开邮件，或设置一个 Telegram
会话 id，然后保存。
