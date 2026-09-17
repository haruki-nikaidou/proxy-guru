---
title: Install and Update Agents
description: Publish the worker binary once, install it on any server with the one-line command the dashboard renders, and move running workers to a new release from the same panel.
---

A server on a canvas is only a row until a `guru-worker` registers as it. This guide is the path
from "the control plane is up" to "every server runs a worker the dashboard can update": you publish
the binary once per release, the dashboard renders an install command per server, and the update
button moves a running worker to the published version. It assumes a deployed control plane
([Docker](/guides/deploy-with-docker/) or [native](/guides/deploy-natively/)) whose nginx serves a
directory as `/agent/`.

## 1. Publish a release

The dashboard hands out whatever `manage-tool agent publish` last recorded. It needs the built
binary and the directory nginx serves under `/agent/` (`/srv/guru/agent` in both deployment guides):

```sh
cargo build --release --locked -p guru-worker
(set -a; . ./.env; set +a; ./target/release/manage-tool agent publish \
    --binary target/release/guru-worker --dir /srv/guru/agent)
# published guru-worker 0.2.0-beta (x86_64) to /srv/guru/agent/0.2.0-beta
#   sha256 6665c280…
#   the dashboard now offers this version to servers running another one
```

It runs the binary once (`--version`), so the recorded version is the binary's own; reads the
architecture from the ELF header; and writes, through sibling temp files and renames:

| Path under `--dir` | What it is |
|---|---|
| `<version>/guru-worker` | the binary, mode `0755` |
| `install.sh` | the installer the dashboard's command pipes into `sh` |
| `guru-worker@.service` | the systemd template unit the installer lays down |
| `guru-worker-guard` | the `ExecStartPre=` start guard that rolls back a bad update |

The three scripts are embedded in `manage-tool` from `bin/guru-worker/deploy/`, so a publish always
ships the installer that matches the binary's source revision. Version, digest and architecture go
into one database row (`orchestration_agent_release:current`), which is state rather than
configuration: a publish takes effect immediately, no master restart, and publishing the same
version again simply overwrites it. Older version directories stay on disk; nothing prunes them.

The control plane also has to know the public origin workers dial, so the command it renders points
somewhere. That is `agent_public_base_url` on the orchestration configuration — the origin the
worker API and `/agent/` are served on, `https://guru.example.com` in these guides. It need not be
the dashboard's hostname, but it must be the one workers are told to dial, since a worker refuses
an update URL outside its own `GURU_MASTER` origin. Set it once and restart the masters
(`config set` replaces the whole document, so start from `config get`):

```sh
./target/release/manage-tool config get orchestration > /tmp/orch.json
# add "agent_public_base_url": "https://guru.example.com" to it, then
./target/release/manage-tool config set orchestration "$(cat /tmp/orch.json)"
```

`agent_download_path` (default `/agent`) and `agent_update_poll_secs` (default 60) live on the same
key; see [Configuration → Module configuration](/reference/configuration/#module-configuration).

## 2. Install a worker from the dashboard

Open the canvas, click the server, and find the **Agent** section in its panel. It shows the version
the worker last registered as (nothing yet for a fresh server), the published release, and an
**Install command** button. The button is disabled, with the reason spelled out, until a release is
published and the base URL is set.

The dialog asks for the systemd instance name — `guru-worker@<name>` on the host, prefilled with the
server name as a slug — and **Generate** issues the server's key and renders the command:

```sh
curl -fsSL https://guru.example.com/agent/install.sh \
  | sudo env GURU_MASTER=https://guru.example.com \
             GURU_SERVER_ID=uz0ih3b30nrekqzs1h1y \
             GURU_UNIT=hk-1 \
             GURU_AGENT_VERSION=0.2.0-beta \
             GURU_AGENT_SHA256=6665c280… \
             GURU_API_KEY=gs_… \
             sh
```

Paste it on the server as a user who can `sudo`. Everything the installer needs travels in the
environment (`sudo env … sh`, which sidesteps `sudo`'s environment reset); nothing is on a command
line, though the key is briefly visible in `ps` on that host while the installer runs.

:::note[The key is the server's own]
`GURU_API_KEY` here is not an operator API key. It is an agent key (`gs_…`) issued for this one
server: it can register this server and nothing else, only its SHA-256 is stored, and it is shown
exactly once, inside this command. **Regenerate install command** issues a new one and invalidates
the old — a worker still holding it is refused at its next reconnect, which is the intended way to
cut a host loose. An operator API key (`gk_…`) keeps working for hand-rolled units, as in
[Independent Worker Deployment → Adopting the node later](/guides/independent-worker/#9-adopting-the-node-later)
— but it can register *any* server, so a leaked one lets its holder impersonate every worker in the
fleet, where a leaked `gs_…` exposes one server.
:::

What `install.sh` does, in order — and it is idempotent, so re-running the command on the same host
repairs or upgrades that instance in place:

1. Checks it runs as root, that `curl`, `sha256sum`, `useradd`, `systemctl` and `install` exist,
   that the host is `x86_64` (the published binary is glibc x86_64), and that `/opt/guru-worker`
   does not exist — that is the tree every instance shared before instances got their own (see
   *Moving off the shared layout* below).
2. Creates the `guru-worker` system user, the instance's tree `/opt/guru-worker-<unit>` (root-owned)
   with `bin/` inside it (owned by that user — self-update writes there), and `/etc/guru-worker`
   (root-owned).
3. Downloads the binary, refuses it on a SHA-256 mismatch, installs it as
   `/opt/guru-worker-<unit>/bin/<version>/guru-worker`, runs `--version` as a smoke test, and points
   the instance's `bin/current` symlink at it. A version that was current before becomes
   `bin/previous`.
4. Writes `/etc/guru-worker/<unit>.env` (`root:guru-worker`, mode `0640`) — the only place the
   instance's settings live: `GURU_MASTER`, `GURU_SERVER_ID`, `GURU_API_KEY`, `GURU_STATE_DIR`
   (`/var/lib/guru-worker/<unit>`), `GURU_LOG_LEVEL`.
5. Writes `/opt/guru-worker-<unit>.uninstall.sh` (root only, mode `0700`), which purges the
   instance again (section 4).
6. Installs the start guard at `/usr/local/libexec/guru-worker-guard` and the template unit at
   `/etc/systemd/system/guru-worker@.service`, both shared by the instances on the host.
7. `systemctl daemon-reload`, `enable`, `restart guru-worker@<unit>`.

Every instance runs its own binary from its own tree, so two servers on one host never share a
binary, a `current` link or an update marker: installing, updating or rolling back one instance
leaves the other exactly as it was. For an instance `hk-1`:

```
/opt/guru-worker-hk-1/bin/0.2.0-beta/guru-worker
/opt/guru-worker-hk-1/bin/current -> 0.2.0-beta     what the unit execs
/opt/guru-worker-hk-1/bin/previous                  the start guard's rollback target
/opt/guru-worker-hk-1/bin/pending, failed           update markers, this instance's alone
/opt/guru-worker-hk-1.uninstall.sh
/etc/guru-worker/hk-1.env
/var/lib/guru-worker/hk-1
```

The unit runs as `guru-worker` with `CAP_NET_BIND_SERVICE` (ports below 1024 without root),
`ProtectSystem=strict` with the instance's own `/opt/guru-worker-<unit>/bin` as the only writable
path outside the state directory, `StateDirectory=guru-worker/<unit>`, `LimitNOFILE=65535`,
`Restart=always` and `StartLimitIntervalSec=0` — a data-plane node never stops trying to come back;
a binary that cannot start is the guard's job (section 3), not systemd's.

Then:

```sh
systemctl status guru-worker@hk-1
journalctl -u guru-worker@hk-1 -f
#   INFO guru_worker::agent: registered with master server=… version=0.2.0-beta …
```

The card on the canvas shows `v0.2.0-beta` next to *Last seen* once the worker has registered, and
the panel's Agent section shows the unit name and when the key was issued.

## 3. Update a running worker

Publish the new build (section 1). Every server whose worker registered as another version now says
*update available* in its Agent section, with an **Update to v…** button. Clicking it records the
request on the server; the worker asks for updates every `agent_update_poll_secs` (60 s, jittered)
over its refresh-key session, and on the next poll:

1. downloads `https://<base>/agent/<version>/guru-worker` into
   `/opt/guru-worker-<unit>/bin/<version>/` —
   refusing any URL that is not under its own `GURU_MASTER` origin — and verifies the SHA-256 the
   master sent while streaming it;
2. records the version that is current as `previous`, writes a `pending` marker, repoints
   `current` at the new version;
3. exits through the same path as `SIGTERM`, and `Restart=always` starts the new version.

The new binary registers as its version, which clears the request; the panel goes from
*Updating to v…* back to the plain version line. The `pending` marker is cleared once the new
binary has registered and sent its first health report — that is what "proven" means.

:::caution[An update is a restart]
The worker does not drain: in-flight proxied connections are cut exactly as they are on any restart
or `SIGTERM`. Update one server at a time and outside its busiest hour if that matters.
:::

**When it fails.** Any failure the worker can see — the download, the checksum, a host that refuses
self-update — is reported at once; the request is dropped and the reason is shown in the panel under
*The last update failed*. Fix the cause and click again. A new binary that starts but cannot come up
is caught by the start guard: `ExecStartPre=guru-worker-guard` counts the starts a pending version
has had, and after three it repoints the instance's `current` at its `previous`, removes the marker
and leaves a `failed` note; the rolled-back binary reports that note at its next registration, and
the panel shows it. A binary that does not even exec (wrong architecture, missing glibc) is caught the
same way — the guard lives outside the instance trees and is never touched by an update.

A request outlived by a newer publish is not served: the worker would fetch a version whose digest
is no longer the recorded one, so the master drops the request with a reason and you click again.

**Opting a host out.** `GURU_NO_SELF_UPDATE=1` in `/etc/guru-worker/<unit>.env` (then
`systemctl restart guru-worker@<unit>`) makes the worker refuse offered updates and report why; the
panel shows the refusal. Re-running the install command still upgrades such a host by hand.

## 4. Uninstall a worker

Every install leaves a purge script next to the instance's tree. On the host:

```sh
sudo bash /opt/guru-worker-hk-1.uninstall.sh
# guru-worker@hk-1: service, binaries, environment and state removed
```

It stops and disables `guru-worker@<unit>`, then deletes `/opt/guru-worker-<unit>/` (every version of
the binary), `/etc/guru-worker/<unit>.env` (with the key) and the state directory
`/var/lib/guru-worker/<unit>`, and finally itself. Other instances on the host keep running. When the
instance was the last one — no `/etc/guru-worker/*.env` is left — it also removes what the instances
shared: the template unit, the start guard, `/etc/guru-worker`, `/var/lib/guru-worker` and the
`guru-worker` user. The server stays on the canvas and goes *Offline*; its key stays valid until you
regenerate the install command, so regenerate it if the host is not coming back.

**Moving off the shared layout.** Workers installed before instances got their own tree all ran from
one `/opt/guru-worker`, and the installer refuses to install next to it, because the template unit it
lays down would strand those workers at their next start. Purge the host once, then run a freshly
generated install command for each server on it:

```sh
for unit in $(systemctl list-units --all --plain --no-legend 'guru-worker@*' | awk '{print $1}'); do
    sudo systemctl disable --now "$unit"
done
sudo rm -rf /opt/guru-worker /etc/guru-worker /var/lib/guru-worker \
    /etc/systemd/system/guru-worker@.service /usr/local/libexec/guru-worker-guard
sudo userdel guru-worker
sudo systemctl daemon-reload
```

## 5. Troubleshooting

| Symptom | Cause |
|---|---|
| *Install command* is disabled | No release published (`manage-tool agent publish`) or `agent_public_base_url` is empty; the panel says which. |
| `curl … 404` | nginx has no `/agent/` location, or `--dir` was not the directory it serves. |
| `SHA-256 mismatch` from the installer | The file under `/agent/<version>/` is not the one recorded — republish. |
| `guru-worker install: the published binary is x86_64 (glibc); this host is aarch64` | Only x86_64 glibc is built. |
| `Missing identity` / `permission denied` at registration | The key in the env file is not the server's current one (regenerated since?) — generate a new command and re-run it. |
| `another worker session is live for this server` right after an update | The old session's lease (30 s) has not lapsed; the new process retries with backoff. Harmless. |
| `another worker session is live for this server` for minutes, server *Offline* | A registration whose connection died before its first report, its watch stream still held open by the proxy. The `sweep_liveness` pass revokes that session once the registration is three report intervals old; the next retry is accepted. |
| Panel shows *The last update failed: … outside the master origin* | `agent_public_base_url` differs from what workers dial in `GURU_MASTER`; make them the same origin. |
| Panel stays on *Updating to v…* | The worker is not polling: it is down, or too old to poll (installed before self-update existed) — re-run the install command once. |
| `guru-worker install: /opt/guru-worker holds workers installed before instances got their own tree` | The host still has the shared layout; purge it as in section 4, then install each server again. |
| Panel shows *The last update failed: not installed under a version directory with a `current` link* | The worker was not started from an installer's tree (a hand-run binary, or the shared layout); re-run the install command. |
