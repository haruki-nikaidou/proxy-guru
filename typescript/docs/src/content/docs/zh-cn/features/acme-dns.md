---
title: 基于 DNS 的 ACME
description: 一个 TLS Pod 如何通过 DNS-01 挑战拿到受公共信任的证书，以及独立的内部 CA 如何保护中继跳。
---

终结 TLS 的客户端 Pod 需要一张为其客户端所连接的那个名称签发、且受公共信任的证书。控制平面自己通过
ACME 的 DNS-01 挑战获取这张证书，因此不需要任何东西在 80 端口上可达，而且可以为一个流量还没开始流
动的名称签发证书。你自己的服务器之间的中继跳使用另一套内部 CA —— 那是本页的后半部分。

## Pod 声明什么

一个 TLS 客户端 Pod 带有四项设置，在[该 Pod 的面板](/zh-cn/reference/nodes/#pod-的面板与它的路由)
中编辑：

| 设置 | 含义 |
|---|---|
| `sni` | 客户端出示的主机名。存储时转为小写；证书以这个规范形式为键。 |
| DNS 提供商 | 由哪个已配置的提供商来应答挑战。 |
| `domain_id` | Cloudflare：zone id。Vercel：已注册的域名，例如 `example.com`。 |
| ACME 目录 | 留空表示使用存储的 `default_acme_directory`（Let's Encrypt 生产环境）。 |

有三项图检查守卫着这个组合，触发其中任何一项的编辑都会被拒绝：

- `invalid_sni` —— 该名称不是一个由至少两段 ASCII 字母、数字和连字符组成的主机名。不接受通配符。
- `unknown_dns_provider` —— Pod 指定了一个并不存在的提供商。
- `certificate_issued_elsewhere` —— 同一 SNI 与同一目录的证书已经存在，但用的是不同的提供商或
  domain id。证书仅以 `(sni, directory)` 为键，因此共享同一个 SNI 的每个 Pod 都必须就它如何签发达
  成一致，而不是让其中某个 Pod 的设置被悄悄忽略。

## 每个名称与目录一行记录

每一组不同的 `(sni, acme_directory)` 解析出一行 `certificate` 记录；第一个提出请求的 Pod 固定了它的
提供商和 domain id，后来的 Pod 共享这一行。该行保存着用 `GURU_MASTER_KEY` **加密**的
（XChaCha20-Poly1305）ACME 账户凭据和私钥、明文的证书链、有效期窗口、一个 `version`，以及来自最近
一次尝试的 `last_error` 和 `last_attempt_at`。

它的状态是 `pending`、`issued` 或 `failed`，而只有当该行已签发*并且*确实持有私钥和证书链时，Pod 才
会编译进配置。一次失败的续期绝不会降级一张在用的证书：失败会记录在该行上，而状态仍为 `issued`。

## 签发任务

`renew_certificates` 每 60 秒发布一次，并在 `--mode consumer` 中消费，其 tick 在
`orchestration_job_run` 中声明，因此该任务在整个集群内每 `acme_interval_secs` 只运行一次。一次任务
会：

1. 读取每个 `client_tls` Pod，解析它的目录，并创建缺失的证书记录行；
2. 列出到期的记录行 —— 从未尝试过的，或上次尝试早于 `acme_retry_after_secs`（1 小时）、且尚未签发
   或将在 `acme_renew_before_secs`（30 天）内过期的；
3. 用刚刚观察到的 `last_attempt_at` 值对每一行做 compare-and-set 来声明它，这既是避免两个 master
   订购同一张证书的手段，也是某个 master 在订购中途崩溃时该行会被搁置一小时的原因；
4. 一次只跑一个订单，因为一个 DNS-01 订单要花上几分钟，而提供商的 API 有速率限制。

订单本身：恢复已存储的 ACME 账户或注册一个新账户，为该 SNI 开启订单，通过提供商的 API 在
`_acme-challenge.<name>` 创建一条 TTL 为 60 的 TXT 记录来应答每个待处理授权的 `dns-01` 挑战，等待传
播，告知 CA 挑战已就绪，然后轮询、finalize 并存储。无论订单成功与否，这些 TXT 记录随后都会被删除。

传播情况会对照 Cloudflare 和 Google 的公共解析器检查，两者都必须返回准确的挑战值，每 5 秒轮询一
次，最多 120 秒；订单轮询和证书轮询各自也有 120 秒的上限。这四个数值是编译进代码的，不是配置键，而
超时会报告为 `TXT record _acme-challenge.<name> did not propagate within 120s`。

存储一张证书会递增它的 `version`、清除 `last_error`，并触碰每一个含有该 SNI 的 Pod 的画布，以触发派
生。任何东西都不会被永久放弃：任何失败都会被记录下来，并在重试窗口过后由后续的某次任务重试，没有尝
试计数器，也没有额外的退避。

## 证书缺失期间

该 Pod 不会编译。它不贡献任何 forwarding，它所在服务器的其余部分照常派生和发布，而该 Pod 会带着以
下三条消息之一出现在服务器视图的 `invalid_pods` 里：

| 消息 | 含义 |
|---|---|
| `certificate for <sni> is pending (not requested yet)` | 还没有记录行 —— 包括被签发方更严格的校验拒绝的 SNI，这种情况根本不会有记录行。 |
| `certificate for <sni> is pending` | 记录行存在，但尚未签发。 |
| `certificate for <sni> is failed: <error>` | 最近一次尝试失败了；挑战不工作时要读的就是这条消息。 |

服务器**不会**因为一张待签发的证书而变成 `Degraded`：`invalid_pods` 不参与[状态判定](/zh-cn/features/health-monitor/#服务器状态)。一个已经在提供服务的 Pod 即使后来不再编译，也会保留它的监听器。

## 续期

一行已签发的记录会在 `not_after` 落入 `acme_renew_before_secs`（30 天）之内时续期。续期就地重写材料
—— 同一行、同一 id，因此每台 Worker 上的文件路径保持不变 —— 并递增 `version`。正是这个版本号承载着
新材料的下发：快照会固定它的 TOML 所引用的版本，因此即便 TOML 的字节完全相同，快照也不同，每台提供
该证书服务的服务器都会获得一个新的修订版本。Worker 会原子地写入新文件，私钥的权限为 0600。

## DNS 提供商

支持两种提供商。一行提供商记录是一个名称、一种种类、一个可选的 `account_id` 和 API 令牌，令牌加密存
储，并且**绝不会**被任何 API 返回 —— 每条读取路径给出的都是省略了它的摘要。

| 提供商 | Pod 上的 `domain_id` | 提供商上的 `account_id` |
|---|---|---|
| Cloudflare | Zone id | 不使用 |
| Vercel | 已注册的域名 | Team id；留空表示个人账户 |

`CreateDnsProvider`、`UpdateDnsProvider` 和 `DeleteDnsProvider` 仅限 Admin。令牌留空的更新会保留已
存储的令牌。仍被某个 Pod 引用的提供商无法删除：`dns provider is still used by <n> pod(s)`。

## TLS 页面

控制台中的 `/tls` 仅限 Admin，有两个标签页。**DNS 提供商**用于创建、编辑和删除提供商；种类在创建之
后不能更改，编辑时令牌留空则保留已存储的令牌。**证书**列出每一行记录及其 SNI、状态、解析出的提供
商、domain id、ACME 目录、带到期提示的有效期窗口和最近一次尝试；失败的行可以展开查看 `last_error`。

每行有两个操作，而且都不会同步签发任何东西：

- **立即重试**会清除 `last_error` 和 `last_attempt_at`，使该行在紧接着的下一次任务中就到期 —— 对一
  张已签发的证书来说这就是强制续期。
- **删除**在仍有任何 Pod 解析到该证书时会被拒绝。

该页面从不创建记录行。只有当下一次 `renew_certificates` 任务读取到某个 TLS Pod 的证书设置之后，
对应的记录行才会出现，而这些设置是在画布上编辑的；派生只是观察这一行，并在证书签发之前把该 Pod
报告为无效。

## 内部中继 CA

基于 TLS 或 QUIC 的中继跳是你自己的服务器之间的流量，因此它们完全不使用 ACME。`manage-tool
orchestration init-ca` 会创建一个自签名根证书 —— CN 为 `guru internal relay CA`，有效期十年，其私钥
用 `GURU_MASTER_KEY` 加密 —— 并把证书打印到标准输出。这里故意没有提供覆盖或轮换根证书的路径。

自此之后，派生任务会为每个中继 Pod 签发一张叶证书，有效期为 `relay_cert_valid_secs`（30 天），CN 和
SAN 均为 `<pod-key>.relay.guru.internal`。`rotate_relay_certificates` 任务每小时运行一次，轮换所有
将在 `relay_cert_renew_before_secs`（10 天）内过期的叶证书，然后触碰受影响的画布，让新材料作为一个
修订版本下发。Worker 以 `certs/ca.pem`（TOML 中的 `relay_ca`）接收根证书，并据此校验中继对端。

基于 TLS 或 QUIC 的中继 Pod 在 CA 存在之前无法编译 —— 编辑会被接受，但该 Pod 会被报告为无效，而解
决办法就是 `manage-tool orchestration init-ca`。走普通 TCP 的中继 Pod 两种 CA 都不需要。

## 配置

存储在 `orchestration` 配置键下，在 master 启动时读取一次：

| 键 | 默认值 |
|---|---|
| `default_acme_directory` | `https://acme-v02.api.letsencrypt.org/directory` |
| `acme_renew_before_secs` | 2592000（30 天） |
| `acme_retry_after_secs` | 3600（1 小时） |
| `acme_interval_secs` | 60 |
| `relay_cert_valid_secs` | 2592000（30 天） |
| `relay_cert_renew_before_secs` | 864000（10 天） |
| `relay_rotation_interval_secs` | 3600 |

消费者需要能出网访问 ACME 目录和 DNS 提供商的 API，还需要 DNS 访问它用来检查传播情况的那些公共解析
器。完整文档见[配置参考](/zh-cn/reference/configuration/)。
