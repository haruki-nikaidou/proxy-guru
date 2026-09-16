---
title: 独立ワーカーのデプロイ
description: guru-worker を TOML ファイルだけでスタンドアロン実行する方法 — マスターもデータベースも API キーも不要で、SIGHUP でリロードできます。
---

`guru-worker` はコントロールプレーンを必要としません。TOML ファイルを指定すれば、それだけで完結した TCP/TLS プロキシとして動作します。ファイルに記述されたリスナーをバインドし、トラフィックを転送し、`SIGHUP` を受けるとファイルを再読み込みします。本ガイドで構築するのはまさにそれです。バイナリ 1 つ、設定ファイル 1 つ、systemd ユニット 1 つです。

これはサポートされた動作モードであり、機能を削ったモードではありません。分岐は `--master` に対する単一の `match` です。`--master` を指定しなければ、ワーカーは [`run_standalone`](https://github.com/haruki-nikaidou/proxy-guru/blob/main/bin/guru-worker/src/lib.rs) を実行し、gRPC クライアントを一切構築せず、`GURU_API_KEY` も読まず、`--state-dir` にも触りません（last-known-good ファイルはエージェントモード固有の概念です）。CLI もこの分離を強制します。`--config` と `--master` は排他であり、`--master` は `--server` を必須とします。

## 1. この構成が自分に合うか判断する

|                              | スタンドアロン（`--config`） | エージェント（`--master`） |
|---|---|---|
| 設定の正となる場所 | ノード上のファイル | データベース内のキャンバス |
| PostgreSQL / RabbitMQ / マスターが必要か | 不要 | 必要 |
| オペレーターの API キーが必要か | 不要 | 必要 |
| 変更の反映方法 | ファイルを編集して `SIGHUP` | マスターがリビジョンをストリーム配信 |
| ノード間のロールアウト順序 | 自分で調整する | 収束的かつ依存関係順 |
| 再起動後に直前の設定で復帰するか | ファイル*そのもの*が設定 | `--state-dir` から復元 |
| ディスク上の TLS 鍵と証明書チェーン | 必要 | 必要 |

スタンドアロンが適しているのは、単一ノード、エアギャップ環境や一時的なリレー、Ansible/Nix/Puppet で管理する踏み台、そしてノードの挙動をノート PC 上で再現したい場合です。複数のノードが 1 つのトポロジーを構成し、`SIGHUP` を手作業で調整する代わりに順序付けられたロールアウトを行いたいなら、エージェントモードを選んでください。

:::note[2 つのモードは同じモデルを読みます]
設定フォーマットは `lib/guru_worker_config` で、両方のプレーンで共有されています。[ワーカー設定ファイル](/ja/reference/configuration/#ワーカー設定ファイル)を参照してください。ファイル内に「スタンドアロン専用」の要素は一切ないため、ノードは後からトポロジーを書き直すことなくキャンバスへ移行できます。
:::

## 2. 前提条件

- Linux `x86_64` のホスト。公開バイナリは 2 種類あり、glibc のディストリビューション（最近の Debian/Ubuntu/RHEL）では `x86_64-unknown-linux-gnu` を、Alpine やその他の musl ディストリビューションでは musl libc を静的リンク（static-pie）した `x86_64-unknown-linux-musl` を使います。後者は実行時に libc を必要としないため、libc がインストールされていないホストでもそのまま動きます。
- `worker-v*` の GitHub リリースから取得した `guru-worker` バイナリ。どちらのアセットを選ぶか、その特定方法と確認事項は [Docker でデプロイ §11](/ja/guides/deploy-with-docker/#11-github-リリースからワーカーバイナリを取得する) を参照してください。そのページのコントロールプレーンに関する内容は、ここでは一切必要ありません。
- TLS または QUIC のリスナーを使う場合は、PEM 形式の秘密鍵とフルチェーン証明書が**あらかじめホスト上にあること**。ワーカーは設定に書かれたパスからそれらを読み込むだけで、どちらのモードでも証明書を取得・生成することはありません。

Docker、PostgreSQL、RabbitMQ、ダッシュボード、API キー、そして自分のアップストリーム以外へのネットワーク到達性は、いずれも**不要**です。

## 3. バイナリのインストール

```sh
sudo install -m 0755 guru-worker /usr/local/bin/guru-worker
/usr/local/bin/guru-worker --help        # there is no --version; --help is the smoke test
```

システムグループと、それに対応する、ホームディレクトリもシェルも持たないシステムユーザーを作成し、続いてそのユーザーが読める設定ディレクトリを作ります。グループは `useradd` に任せず明示的に作成してください。`useradd` がグループを派生させるかどうかはディストリビューションの `useradd` のデフォルト（`USERGROUPS_ENAB`）によって変わり、以下のすべての `chown` とユニットの `Group=` はグループが存在することを前提としています。

```sh
sudo groupadd --system guru-worker
sudo useradd --system --gid guru-worker \
  --no-create-home --home-dir /nonexistent \
  --shell /usr/sbin/nologin guru-worker      # RHEL: /sbin/nologin
sudo mkdir -p /etc/guru-worker
sudo chown root:guru-worker /etc/guru-worker
sudo chmod 0750 /etc/guru-worker
```

構成管理ツールが再実行する可能性がある場合は、どちらもガードしてください。
`getent group guru-worker || sudo groupadd --system guru-worker` と
`getent passwd guru-worker || sudo useradd --system --gid guru-worker …` です。

## 4. 設定を書く

`/etc/guru-worker/config.toml` はデフォルトのパスなので、`--config` を渡さないユニットでも動作します。まずはデータパスが通っていることを確認できる最小構成、つまり 1 つの生 TCP リスナーから 1 つのバックエンドへ、という形から始めましょう。

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

TLS を終端するエントリポイントから負荷分散グループへ振り分ける場合は次のようになります。証明書のパスは、ワーカーが開けなければならない単なるファイルであることに注意してください。

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

すべてのキー、すべてのリスナー形態、すべての検証ルールは[ワーカー設定ファイル](/ja/reference/configuration/#ワーカー設定ファイル)にまとめてあります。初めて設定を書く人がまず引っかかる点が 2 つあります。未知のキーは致命的エラーになること（暗黙のデフォルトは存在しません）、そしてスタンドアロンモードではプロセスのログレベルがファイル内の `log.level` から決まることです。`--log-level`/`GURU_LOG_LEVEL` はエージェントモードのフラグで、ここでは無視されます。

証明書と鍵はサービスユーザーが読める必要があり、鍵は誰からでも読める状態にしてはいけません。

```sh
sudo chown root:guru-worker /etc/guru-worker/tls/key.pem /etc/guru-worker/tls/fullchain.pem
sudo chmod 0640 /etc/guru-worker/tls/key.pem
```

ユニットを用意する前に、まずフォアグラウンドでファイルを試してください。チェック専用のフラグはありません。ワーカーは何かをバインドする*前に*設定をパースして検証するので、不正なファイルなら即座に終了し、正しいファイルならリスナーをバインドし、`Ctrl-C` で中断するまで動き続けます。この確認にはポートがすべて 1024 より大きい設定を使うか、root で実行してください。非特権のフォアグラウンド実行では `:443` をバインドできません。

```sh
sudo -u guru-worker /usr/local/bin/guru-worker -c /etc/guru-worker/config.toml
# fatal: parse toml: TOML parse error at line 4, column 1 … unknown field `listenas`  ← exits
# INFO guru_worker: loaded config path=/etc/guru-worker/config.toml                   ← serving; Ctrl-C to stop
```

`exit` に `send_proxy_protocol = "v2"` を追加するのは、バックエンドが PROXY プロトコルに対応してから（nginx の `proxy_protocol`、HAProxy の `accept-proxy`、Envoy の proxy-protocol リスナーフィルター）にしてください。素の HTTP サーバーに送ると、すべてのリクエストがリクエストラインの不正で失敗します。データパス自体は動いていると気づくまでに、非常に紛らわしい失敗の仕方です。

## 5. systemd で動かす

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

`LimitNOFILE` は重要です。プロキシされる接続 1 本ごとにディスクリプタを 2 つ消費するため、デフォルトの 1024 は安全網ではなく、負荷がかかれば必ず当たる上限です。`StateDirectory=` が無いのは意図的で、スタンドアロンモードは何も書き込みません。

## 6. 動作確認

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

ユニットは動いているのに `ss` にリスナーが出てこない場合は、ログを読んでください。適用エラーは問題のエントリを `<tag>: <reason>` の形で示します。`Address already in use`、特権ポートでの `Permission denied`（`CAP_NET_BIND_SERVICE` が無い）、あるいは開けない証明書ファイルなどです。

## 7. 設定の変更: 編集してリロード

```sh
sudoedit /etc/guru-worker/config.toml
sudo systemctl reload guru-worker          # sends SIGHUP
journalctl -u guru-worker -n 5 --no-pager  # "config reloaded"
```

リロードは全部か無かで、中断する必要のないトラフィックは中断しません。

- ファイルはパースされ、検証され、新しいソケットはすべて、何かが変わる**前に**バインドされます。いずれかの段階が失敗した場合は何も変更されず、ログには `reload failed` もしくは `reload apply failed; keeping running config` と出ます。ワーカーは古いトポロジーのまま処理を続けます。
- 変更をまたいで生き残ったリスナーは、コンパイル済みの設定をホットスワップします。次の接続からは新しい宛先が使われます。既存の接続は、閉じるまで従来の経路を維持します。
- 消えたリスナーは受け付けを停止しますが、処理中の接続が強制終了されることはありません。
- 同じ `ip:port` 上で別のリスナーに置き換えられる場合は、先に旧リスナーを閉じ、ソケットが実際に解放されるまでリロードが待つので、置き換え後のリスナーは同一のリロード内でバインドできます。QUIC のエンドポイントはまず生存中の接続をドレインし、3 秒を超える場合は強制的に停止されます。奪われるアドレスを無期限に保持することはできないからです。それでも置き換え後のリスナーがバインドに失敗した場合は、そのリロードが停止したすべてのリスナーが元に戻されます。

証明書は適用時にパースされるため、更新もリロードで済みます。再起動は不要で、接続も切れません。ACME クライアントに組み込んでください。certbot の場合は次のようになります。

```sh
# /etc/letsencrypt/renewal-hooks/deploy/guru-worker.sh
#!/bin/sh
install -o root -g guru-worker -m 0640 \
  /etc/letsencrypt/live/example.com/privkey.pem   /etc/guru-worker/tls/key.pem
install -o root -g guru-worker -m 0644 \
  /etc/letsencrypt/live/example.com/fullchain.pem /etc/guru-worker/tls/fullchain.pem
systemctl reload guru-worker
```

`SIGTERM`/`SIGINT`（したがって `systemctl stop`、`systemctl restart`）は、すべてのリスナーを停止してシャットダウンします。リロードは変更された `log.level` も適用し、プロセスのログは再起動なしで切り替わります。

## 8. トラブルシューティング

| 症状 | 原因 |
|---|---|
| `fatal: read /etc/guru-worker/config.toml: No such file or directory` | `--config` が指定されておらず、デフォルトパスにもファイルが無い。あるいはサービスユーザーがそれを読めない |
| `fatal: parse toml: TOML parse error at line N …  unknown field …` | キーの綴り間違い。どのテーブルも未知のフィールドを拒否し、メッセージは該当行を指し示します |
| `fatal: duplicate listener 0.0.0.0:443 (edge)` | 同じ `ip:port` とトランスポートのエントリが 2 つある。ポートを共有できるのは TCP のエントリと QUIC のエントリだけです |
| `fatal: forwarding edge relay to tls/quic requires sni` | `sni` の無い `tls`/`quic` のリレーホップ（ネストの深さは問いません） |
| `fatal: edge: Permission denied (os error 13)` | `AmbientCapabilities=CAP_NET_BIND_SERVICE` の無い状態での特権ポート（適用エラーにはそのエントリの `tag` が接頭辞として付きます） |
| `fatal: tls-term: error:80000002:… calling fopen(/etc/guru-worker/tls/key.pem, r)` | ワーカーが開けない証明書または鍵のパス。OpenSSL が報告するためメッセージは騒々しいですが、ファイル名は示してくれます |
| `WARN config lint … used ip_hash for load balancing` | `receive_proxy_protocol` の無いリスナー配下での `ip_hash`。プロキシ背後の `raw`/`tls` リスナーでは実際に問題で、すべての接続がプロキシのアドレスをハッシュして 1 つのメンバーに寄ってしまいます。`relay` リスナーでは常に自身で PROXY ヘッダーをデコードし真のクライアントをハッシュするため無害ですが、この lint は両者を区別しません |
| リロードが何もしていないように見える | ログに `reload failed` が無いか確認してください。古い設定がまだ動いています。`ExecReload` が `SIGUSR1` ではなく `SIGHUP` を送っているかも確認してください |
| バックエンドにクライアントではなくプロキシの IP が見える | `exit` に `send_proxy_protocol` を追加し（バックエンド側でも読めるようにし）、ワーカー自身がプロキシの背後にいる場合は `receive_proxy_protocol` を設定してください |

## 9. 後からノードを取り込む

スタンドアロンモードとエージェントモードは、同じモデルを読む同じバイナリなので、移行は書き直しではなくユニットファイルの変更で済みます。

1. そのノードをキャンバス上のサーバーとしてモデル化し、マスターに設定を導出させます。
2. 2 つのファイルを比較します。`manage-tool orchestration export-config --server <key>` は、キャンバスが現在導出している内容、つまりマスターがストリーム配信するはずのファイルを出力します。（このコマンドはデータベースと通信するので、ワーカーノードではなくオペレーターのマシンで実行します。）
3. `--config <file>` を `--master <url> --server <key>` に差し替え、API キーを `GURU_API_KEY` または `--api-key-file` で渡し、再起動後にノードが last-known-good の設定を復元できるよう、書き込み可能な `--state-dir` を追加します。
