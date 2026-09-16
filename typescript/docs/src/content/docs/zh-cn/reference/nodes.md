---
title: 节点
description: 画布上的每一种节点 —— 卡片显示什么、有哪些连接点、哪些字段可编辑，以及它最终在链路上变成什么。
---

画布是由**节点**通过**连线**连接而成的图。节点本身不承载流量：控制平面读取这张图，为每台服务器派生
出一份配置，并把配置下发给各个 Worker（参见[发布模型](/zh-cn/reference/rollout/)）。本页就是节点目录
—— 每种节点一节，并附上控制台实际绘制的卡片。

## 读懂一张卡片

每个节点都渲染同样的外框：一个图标、节点**名称**、一个可选的徽标，以及右上角的**种类**。表头下方是
一行根据节点自身配置生成的摘要，再往下是它的连接点。如果设置了运维备注，它会以一行浅色文字显示在表头
下方。

新建的节点会得到一个 `<colour>-<animal>` 形式的名字（`Entry harlequin-weasel`、
`Exit moccasin-gorilla`），取自一份颜色与动物词典 —— 从不使用形容词，因为像 *broken* 这样的词会被误读
成状态。名字只是一个标签：你可以随意改成任何内容。

**暖橙色描边**表示该节点正在检查器中打开（也就是下面 Exit 截图里的那道选中轮廓）。红色描边表示节点存在
校验错误，琥珀色描边表示警告；在节点处于选中状态时，检查器描边会刻意盖过这两者。

### 连接点

每个连接点就是一个**端口**。端口有种类和方向，两者都由节点种类生成 —— 你永远不需要自己发明端口名。

| 外观 | 端口种类 | 含义 |
|---|---|---|
| 蓝色圆点 | `listen` | 监听器：这一端接受连接 |
| 橄榄色圆点 | `destination` | 目标：这一端向外发起连接 |
| 带通道颜色的圆点 | `destination` | 该端口属于某一个通道；这个颜色在整条路径上标识同一个通道 |
| 灰色方块 | `bundle` | 不承载流量 —— 这是展开用的元数据，把一个节点的所有通道带到下一个节点 |

所有连接都由三条规则覆盖：

- **输出位于卡片左边，输入位于卡片右边。** 因此一条链路是从右往左读的：某个 pod 的 `listen` 输出
  （左侧）接入某个 Entry 的 `listen` 输入（右侧）。
- **种类必须匹配。** 蓝色连蓝色，橄榄色连橄榄色，方块连方块。listen 端口永远不能连到 destination
  端口。
- **一个端口只接受一条连线。** 一旦接好，该连接点就不再接受拖拽。只有「add」节点的*分组*连接点可以
  接受多条连线。

控制台会在你拖拽时就拒绝非法连接，而服务端在每次写入时都会重新校验整张拓扑，所以一次编辑要么完整生效，
要么被拒绝。

画到一半的工作是合法的，也不算错误：没人接入的 Relay、四个成员里只连了两个的负载均衡、还没有目标地址
的 Exit。这样的 pod 只是不会被派生，而同一台服务器上其他所有 pod 都不受影响。

## Entry

![一张标题为 “Entry harlequin-weasel” 的 Entry 节点卡片，摘要行为 “Receive PROXY protocol: None”，并带有一个蓝色 listen 连接点](/img/nodes/node-entry.avif)

网络的入口边界：Entry 描述的是一个监听器*如何*接受客户端连接。

| 连接点 | 种类 | 方向 |
|---|---|---|
| `listen` | listen（蓝色） | 输入 —— 由恰好一个 pod 接入 |

- **Receive PROXY protocol** —— `None`、`PROXY v1` 或 `PROXY v2`，原样显示在摘要行上。取 `None` 时
  真实客户端 IP 是未知的，因此该 pod 下游任何位置上的 `ip_hash` 负载均衡都会构成错误。
- **TLS** —— 关闭意味着该 pod 以裸协议监听；开启意味着该 pod 使用 ACME 证书终结 TLS。启用 TLS 的
  Entry 需要一个 SNI（一个普通主机名：至少两级标签，不支持通配符）、一个 DNS 提供商、该提供商的
  域名/zone id，以及可选的 ACME directory URL（留空则使用已配置的默认值）。TLS 开启后，卡片会显示一个
  带 SNI 的锁形徽标。证书以 `(SNI, ACME directory)` 为键，因此共用同一个 SNI 的所有 Entry 必须在提供商
  和域名 id 上保持一致；在证书签发完成之前，那个 pod 一直处于无效状态。

## Exit

![一张标题为 “Exit moccasin-gorilla” 的 Exit 节点卡片，带橙色选中描边，摘要行为 “Not set”，并带有一个橄榄色 destination 连接点](/img/nodes/node-exit.avif)

最后一跳：流量在这里离开该网络。

| 连接点 | 种类 | 方向 |
|---|---|---|
| `destination` | destination（橄榄色） | 输出 |

- **Destination** —— 一个 `host:port` 形式的远端地址。留空会显示 `Not set` 并产生一条警告；填了但无法
  解析则是错误。
- **Pass PROXY protocol** —— 向远端发送 PROXY v1/v2，或者什么都不发。

## Relay

![一张标题为 “Relay moccasin-rooster” 的 Relay 节点卡片，摘要行为 “TCP (raw)”，带有一个蓝色 listen 连接点和一个橄榄色 destination 连接点](/img/nodes/node-relay.avif)

你自己两台服务器之间的一跳。Relay 是画成一个节点的*一对*端点：它的 `listen` 输入由**接受**该中继连接
的 pod 接入，而它的 `destination` 输出接到**发起**该连接的 pod。

| 连接点 | 种类 | 方向 |
|---|---|---|
| `listen` | listen（蓝色） | 输入 |
| `destination` | destination（橄榄色） | 输出 |

- **Relay protocol** —— `TCP (raw)`、`TCP (TLS)` 或 `QUIC`。TLS 与 QUIC 使用该网络的内部 CA 以及
  按 pod 签发的叶子证书；裸 TCP 在接入时会自动探测 PROXY，因此中继一跳总能知道客户端 IP。
- **Override IP address / Override port** —— 留空则使用派生出来的值。实际拨号地址依次取：覆盖值、
  目标 pod 的 advertise 地址、目标服务器的生效地址；端口依次取：覆盖值、目标 pod 的端口。两个覆盖值
  都会被追加到摘要行上。

两端落在同一台服务器上会产生警告 —— 你几乎肯定本意是两台服务器。

## 负载均衡（distribute）

![一张标题为 “fan-out” 的 Distribute 节点卡片，摘要行为 “Round robin · QUIC · Members: 4”，左侧是三个以其 Entry pod 命名的彩色通道连接点以及浅色的 “+ bundle”、“+ channel” 连接点，右侧是四个名为 hk-1 到 hk-4 的方形成员连接点](/img/nodes/node-distribute.avif)

把经它捆绑的每一个**通道**扇出到它的**成员**上（参见[通道与捆绑](#通道与捆绑)）。成员就是你写下的
规则：每台中转服务器一个，名字由你填，每个成员一条出向捆绑。四台 AWS 就是四个成员；第五台就是再加一个
成员、再拉一条捆绑。

| 连接点 | 种类 | 方向 |
|---|---|---|
| 每个成员一个，名字是你填的 | bundle（灰色方块） | 源端 —— 一条连线，连到某个 universal pod 或某个 distribute 节点的 `+ bundle` |
| 每条入向捆绑一个 | bundle（灰色方块），以对端节点命名 | 目标端 —— 一条连线，来自某个 universal pod 的 `bundle out` 或另一个 distribute 节点的成员 |
| 每个通道一个 | destination，通道颜色 | 源端 —— 一条连线，连到作为该通道的 Entry pod 的 `destination` |
| `+ bundle` | bundle（灰色方块），add | 目标端 —— 上游捆绑落到这里：它携带的每个通道都会从这里再次扇出 |
| `+ channel` | destination（橄榄色），add | 源端 —— 拖到某个 Entry pod 的 `destination`；被连上的 pod 就成为又一个彩色通道 |

- **Balance mode** —— `Round robin`、`Random`、`IP hash` 或 `Fallback`。`IP hash` 需要已知的客户端
  IP，因此它位于一个不接收 PROXY 协议的 Entry 之下时是错误。
- **Relay protocol** —— 该节点扇出的这些通道，以何种方式中继到成员所捆绑到的 universal pod
  （`TCP (raw)`、`TCP (TLS)`、`QUIC`）。任何没有声明协议的跳（Entry pod 直连到 universal pod，或
  universal pod 捆绑到下一个 universal pod）都是裸 TCP。修改它会让每个落地 pod 拿到一个新端口。
- **Members** —— 1 到 256 个，每个都有你自己起的名字（不重复，最多 64 个字符）。检查器会列出每个
  成员以及它那条捆绑的对端；用 *添加成员* 加一个，用行尾的垃圾桶删一个。成员改名、调顺序都会保留它的
  连接点和捆绑；删除一个还挂着捆绑的成员会被拒绝，先把捆绑断开。恰好只有一个成员接了捆绑会产生警告。
  这里没有权重 —— 扇出是按成员进行的。

作为捆绑的*接收方*时，distribute 节点会把这些捆绑携带的一切再次扇出 —— 这就是扇出的第二层（四台中转
服务器铺开到两台落地服务器），或者当捆绑来自另一个 distribute 节点时，构成嵌套策略（一个 `Fallback`
节点，其成员是两组 `Round robin`）。每条上游路径在目标服务器上都会得到属于自己的 lane。

一个通道必须起始于某个 **pod** 的 destination 连接点；其他任何起点都会被拒绝。成员下方的通道列表
会为每个通道显示一枚彩色标签。

## 负载均衡（aggregate）

![一张标题为 “join” 的 Aggregate 节点卡片，摘要行为 “Members: 4”，左侧是四个名为 hk-1 到 hk-4 的方形成员连接点，右侧是四个以其 Entry pod 命名的彩色通道输出](/img/nodes/node-aggregate.avif)

distribute 的镜像：捆绑重新汇合的地方。它的**成员**就是它要汇合的那些捆绑，每台中转服务器一个，名字
由你填；捆绑进来之后，它会为这些捆绑携带的**每个通道各长出一个输出**，每个输出都等待一个 Exit。它本身
不向派生出的配置贡献任何内容。

| 连接点 | 种类 | 方向 |
|---|---|---|
| 每个成员一个，名字是你填的 | bundle（灰色方块） | 目标端 —— 一条连线，来自某个 universal pod 的 `bundle out` |
| 每个通道一个 | destination，通道颜色 | 输出 —— 为每一个连上一个 Exit |

**Members**（1–256 个，带名字）是唯一的设置项；aggregate 没有 balance mode，而 distribute ↔ aggregate
的选择在节点创建时就固定下来。改名会保留成员的捆绑；删除一个还挂着捆绑的成员会被拒绝。它的检查器会
列出每个通道以及接入它的 Exit，或者显示 `no exit`；在还没有任何 universal pod 捆绑到成员上之前，卡片
显示 *No channels yet*。

## Server

![一张标题为 “hk-1” 的 Server 节点卡片，带 Online 徽标、地址行与最后上报时间行，一个包含两个通道圆点的 Universal pod 区块，左侧有一个名为 fan-out 的方形 bundle 连接点以及一个浅色的 “+ bundle” 连接点，右侧有一个方形 bundle out 连接点，并显示 “No pods yet”](/img/nodes/node-server.avif)

一台运行 `guru-worker` 的机器。Server 不是一份节点规格，而是一个容器：它容纳该机器上的每一个
**pod**，以及这台机器的 **universal pod**。pod 从不作为独立卡片出现。

**图标。** 表头字形由你自行选择，可以从两个图标集里取，用简短前缀区分：`flag:<code>` 表示国旗
（`flag:us`、`flag:jp`），`logo:<name>` 表示品牌 logo（`logo:tauri`、`logo:svelte`）。名称来自
[circle-flags](https://icon-sets.iconify.design/circle-flags/) 和
[theSVG Color](https://icon-sets.iconify.design/thesvg-color/)；图标本身是按需从 Iconify API 获取的，
所以在与外网隔离的浏览器中会保留默认字形。其他任何输入 —— 裸的 Iconify 名称、未知前缀、图标集里不存在
的名称、空字段 —— 同样渲染默认的服务器字形；在你输入时，检查器会在字段旁预览效果。

表头徽标表示健康状况：`Online`、`Degraded`（应用配置出错、有失败的 pod，或修订版本落后超过宽限期）、
`Offline`（连续三个健康检查间隔没有任何上报 —— 默认为 45 秒）或 `Unknown`。两行摘要依次是日志级别、
IPv6 解析策略（`Required`/`Preferred`/`Tolerated`/`Forbidden`，默认 `Tolerated`）、生效地址或
`no address yet`、上报的国家 —— 然后是最后一次 watch 流心跳，服务器离线期间会追加 `not reporting`。
服务器填了 QUIC 速率之后，中间会多出 `brutal ↑1000/↓1000`（或 `Cubic ↑…/↓…`）。

**QUIC 中继链路。** 检查器的最后一组是这台服务器在它参与的每条 QUIC 中继链路上的一侧：拥塞控制（`Cubic`，
或不管路径怎样都按上行速率固定发送的 `Brutal`）、上行和下行速率（Mbit/s），以及两个可选的接收窗口（字节；
0 = 按下行速率推导 / 不限）。它们会成为 Worker 的 `[quic]` 段；每条链路上 master 会把两端配对，服务器
永远不会发得比对端的下行更快（见[配置参考](/zh-cn/reference/configuration/#quic)）。`Brutal` 需要填上行速率。

服务器的地址不需要任何人手工填写：Worker 在注册时上报自己的 IPv4/IPv6，master 也会记住注册请求来自
哪里。优先级顺序为：手工固定的 v4 → 上报的 v4 → 观测到的 v4 → 手工固定的 v6 → 上报的 v6 → 观测到的
v6。`no address yet` 会在该服务器的每个 pod 上产生警告 —— 它自己的 pod 仍然能够派生；只有*其他*服务器
向它拨号时才会一直无效。

**Pod。** 一个 pod 就是一个监听套接字：一个名字、一个端口、一个可选的绑定地址（留空则绑定主机的所有
地址，渲染为 `[::]:port`），以及一个可选的 advertise 地址（留空则使用服务器的生效地址）。每一行 pod
都带有两个连接点：

| 连接点 | 种类 | 方向 |
|---|---|---|
| `listen` | listen（蓝色） | 输出 —— 把该监听器提供给一个 Entry 或一个 Relay |
| `destination` | destination（橄榄色） | 输入 —— 消费一棵目标子树 |

同一台服务器上的两个 pod 不能占用同一个套接字；通配符绑定会与该端口上的所有具体地址冲突。两个连接点
中任意一个未连接的 pod 只是不会被派生 —— 这在你连线过程中是正常状态。

一个 pod 对应服务器 TOML 中的一条 `[[forwarding]]` 条目，并以 pod 名称标记。自身派生失败的 pod 会在
该服务器的发布面板中显示为一个无效 pod 并附上原因，且绝不会干扰配置的其余部分。

**Universal pod。** 随服务器一起创建，每台服务器一个，既不能手工创建也不能手工删除。它是其他服务器的
流量落地之处，无需为每条规则各画一个 pod：每条入向捆绑对应一个灰色方块（以来源命名），每个直连进来的
Entry pod 对应一个彩色圆点（它自身就是一次裸 TCP 的跳，不做负载均衡），浅色的 `+ bundle` / `+ channel`
连接点用于接受下一个，以及唯一的 `bundle out`（灰色方块，恰好一条连线），把该捆绑交给下一个
universal pod、一个 distribute 节点，或负载均衡（aggregate）节点的某个成员。捆绑携带的每一个通道都会
在这里得到一个真实的**落地 pod**，并列在该服务器的检查器中，其端口可编辑。这一区块的标题行为落到
这里的每个通道（规则）画一个圆点，并分别统计通道数和落地 pod 数：一条规则经两条路径（比如直接一条、
再经第二层一条）到达同一台服务器时会落地两次并在这里汇合，所以是一个通道、两个 pod。

## 通道与捆绑

一个**通道**就是一条规则 —— 一个 Entry pod —— 从一个负载均衡（distribute）节点出发，经过你各台中转
服务器的 universal pod，到达一个负载均衡（aggregate）节点，并且自始至终带着属于自己的颜色。一条
**捆绑**是一条粗灰线，把所有通道一起带到下一跳；连线标签会显示通道数量。十条规则经四台中转服务器的
图景就是：十个 Entry pod 接入一个 distribute 节点，从它出来四条捆绑，四条捆绑汇入一个 aggregate 节点，
再有十条彩色线连到十个 Exit。

捆绑纯粹是画布上的语法糖。在任何东西被派生之前，它们会先**展开**成普通节点（即「lane」）：按通道、
按目标服务器展开出一个落地 pod、一个使用该 distribute 节点协议的 Relay、当目标多于一个时再加一个
distribute lane，以及在同一通道的多个落地 pod 汇合处加一个 aggregate lane。捆绑在链路上不存在任何
对应物，派生、收敛和健康检查看到的永远只是展开后的图。

lane 节点是受管的：它们不能被退役也不能重新连线，只有落地 pod 的端口和地址可以编辑。lane 在多次编辑
之间保持自己的身份，所以它会保留自己的端口、数据行和健康历史。

一条捆绑总是从一个已经存在的连接点出发 —— distribute 节点的成员，或 universal pod 的 `bundle out`
—— 并在对端被收集：universal pod 或 distribute 节点会为落到它 `+ bundle` 上的每条捆绑长出一个方块，
aggregate 节点则用它的某个成员来接。合法的捆绑方向是：成员 → universal pod、成员 → distribute
（嵌套）、universal pod → universal pod、universal pod → distribute（下一层），以及 universal pod →
aggregate 的成员。distribute → aggregate 会被拒绝：请先把 distribute 节点捆绑到你中转服务器的
universal pod。同一个节点的两个成员不会捆绑到同一个对端。捆绑成环是错误；一个通道没有捆绑到任何
服务器，或者落地之后无处可去，则是警告。

## Export

![四张 Export 卡片：左侧是 “listen · Out of this canvas” 和 “destination · Out of this canvas”，右侧是 “listen · Into this canvas” 和 “destination · Into this canvas”](/img/nodes/node-export.avif)

它所在画布的一个边界端口。父画布把它看作子画布节点上的一个连接点，标签就是该 Export 的名字。

Export 恰好只有一个连接点 `export`，以及两个用于决定它是四种变体中哪一种的设置：

| 端口种类 | 方向 | 该画布内部的连接点 |
|---|---|---|
| `listen` | Into this canvas | 蓝色输出 |
| `listen` | Out of this canvas | 蓝色输入 |
| `destination` | Into this canvas | 橄榄色输出 |
| `destination` | Out of this canvas | 橄榄色输入 |

*Into this canvas* 意味着流量从父画布到来并在这里发出，这也正是它在画布内部表现为**输出**的原因。
这两个设置都在节点创建时询问，因为之后修改其中任何一个都会重塑父画布上的连接点，并且**丢掉挂在它上面
的连线**。纵向移动一个 Export 会重新排序父画布上的连接点。

Export 不是一个流量顶点：校验和派生都会直接穿过它进行解析。

## Subcanvas

![一张标题为 “Subcanvas amber-…” 的 Subcanvas 卡片，列出了以子画布 Export 节点命名的四个连接点](/img/nodes/node-subcanvas.avif)

把另一张画布作为一张卡片嵌入。它的连接点是派生出来的，嵌入画布的每个 Export 节点对应一个 —— 种类照搬，
方向**取反**，按 Export 的纵向位置排序，标签为 Export 的名字。所以一个 `listen · Into this canvas`
的 Export（在子画布内部是输出）在父卡片上表现为一个蓝色输入。

双击卡片即可下钻进入子画布。

嵌入的画布无法更改：删除该节点并重新导入即可。一张画布最多只能被导入一次，且不能导入自己或自己的任何
后代。被导入的画布不会出现在画布列表中，除非你主动要求显示子画布；在它处于被导入状态时也不能删除；
退役该 subcanvas 节点会把它释放回一张独立的根画布。

一棵画布树会作为**一张扁平的图**统一校验和派生：一个 pod 可以位于这棵树上的任意服务器，而子画布内部的
编辑会在同一个事务中重塑父画布的连接点。

## 问题

每次写入都会对整棵树运行校验，并报告两种严重级别。错误会阻止本次编辑；警告只是提示，绝不会妨碍你保存。

**错误** —— 端口种类不匹配、连线不是「输出 → 输入」、自环、跨画布连线、端口被超额占用、端口形态非法、
成环、两个 pod 占用同一个套接字、pod 位于树外的服务器上、Exit 的目标地址无法解析、没有客户端 IP 却使用
`ip_hash`、通道不是起始于某个 pod、捆绑非法或成环，以及 subcanvas 嵌套的四类错误（自身、祖先、重复、
无法解析）。

**警告** —— Exit 还没有目标地址、服务器还没有地址、Relay 两端共用一台服务器、distribute 分组只连了
一个成员、通道没有中转或没有 Exit，以及过期的 lane（下一次编辑被捆绑的节点时会重新生成它们）。
