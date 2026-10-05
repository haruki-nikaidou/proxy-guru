---
title: Remote Shell
description: Run shell commands on a worker host from the dashboard, watch the output live, and pick it up again after a disconnect — off on every host until that host opts in.
---

The remote shell lets an Admin run commands on a `guru-worker` host from the dashboard and watch
their output as it is produced. Nothing about it is on by default: a worker runs no shell unless
**the host it runs on** has opted in, and no setting in the dashboard or on the master can change
that.

## What a session is

A session is one `bash` process on the worker host, started for the session and kept until it ends.
Every command of the session goes to that same process, so state carries over the way it does in a
terminal: `cd /tmp` followed by `pwd` prints `/tmp`, and an `export` or a shell variable stays set
for the commands after it. A new session starts in `$HOME` when that is an existing directory,
otherwise in `/`.

- **One command at a time.** A command sent while another one is still running is refused; nothing
  is queued. The dashboard disables its input until the running command has finished.
- **Exit codes.** Every finished command reports its exit status (`128 + n` for a command killed by
  signal `n`).
- **Live output.** stdout and stderr are streamed as they are written, each chunk tagged with the
  stream it came from.
- **It keeps running when nobody watches.** The shell belongs to the worker process, not to any
  connection. Closing the browser tab, a restart of the frontend or the master, or a broken link
  between the worker and the master does not stop a command.
- **Resume.** The worker keeps each session's transcript — commands, output, exit codes — in a
  ring buffer of `--remote-shell-buffer-bytes` (1 MiB by default). Every entry has a position, and
  a viewer that reconnects asks for everything after the last position it saw. When the buffer has
  wrapped past that point the transcript continues with an *Output truncated: N bytes dropped*
  marker and then the oldest entries still held. The buffer only ever drops its oldest entries:
  writing to it never waits for a slow viewer, so a stalled browser cannot stall the shell.
- **Several viewers.** Any number of Admins can watch the same session at once, each from their own
  position, and any of them can send the next command.

At most `--remote-shell-max-sessions` sessions (4 by default) run on one worker at a time; opening
another one is refused until one of them ends.

## What it does not do

- **No terminal.** There is no PTY: no `top`, no editors, no pagers, no password prompts, no
  terminal resize. A command's stdin is `/dev/null`, so a program that reads input sees EOF at once
  instead of waiting for it. Prefer non-interactive forms (`systemctl --no-pager`,
  `journalctl --no-pager -n 100`, `apt-get -y`).
- **No interrupt.** A running command cannot be stopped on its own. Closing the session is the only
  way to stop it, and that kills everything the session started.
- **No audit log.** Proxy Guru does not record who ran which command.
- **No stored transcripts.** Output lives only in the worker's ring buffer; the master relays it and
  keeps nothing, and nothing is written to PostgreSQL.
- **No survival across a worker restart.** Stopping or restarting the worker ends every session,
  and so does a [self-update](/guides/agent-install/#3-update-a-running-worker), which is a restart.

## Who can turn it on

Four gates, all of which must be open:

1. **The build.** The worker's Cargo feature `remote-shell` is part of its default features, so the
   official release builds and a plain `cargo build -p guru-worker` have it. A binary built with
   `--no-default-features` does not: it contains no shell code at all, and if the host opts in
   anyway it refuses to start rather than silently ignoring the setting.
2. **The host.** The opt-in is a local setting of the worker process — a flag or an environment
   variable on the host, read the same way as `--no-self-update`. It is not part of the config the
   master pushes to the worker, so neither the dashboard nor a compromised master can turn it on:
   only someone who can already change the worker's unit on that host can.
3. **The capability.** A worker that has opted in advertises the capability `remote_shell` when it
   registers with the master. The master refuses every remote-shell request for a server whose
   worker did not advertise it, and the dashboard shows no entry point for it.
4. **The account.** Every remote-shell call needs the `RemoteShell` permission, which only the
   Admin role holds, and only from a signed-in dashboard session — an API key cannot use it.

### Settings

All of them are local to the worker host and exist in every build, with or without the feature:

| Flag | Environment | Default |
|---|---|---|
| `--remote-shell` | `GURU_REMOTE_SHELL` | off (the opt-in; agent mode only) |
| `--remote-shell-buffer-bytes` | `GURU_REMOTE_SHELL_BUFFER_BYTES` | `1048576` (1 MiB; transcript kept per session, counting output, commands and a small overhead per entry; at least `4096`) |
| `--remote-shell-idle-timeout` | `GURU_REMOTE_SHELL_IDLE_TIMEOUT_SECS` | `1800` (seconds a session may sit with no command running and nobody watching; at least `1`) |
| `--remote-shell-max-sessions` | `GURU_REMOTE_SHELL_MAX_SESSIONS` | `4` (concurrent sessions on this worker; at least `1`) |
| `--remote-shell-allow-plaintext` | `GURU_REMOTE_SHELL_ALLOW_PLAINTEXT` | off (allow the opt-in with an `http://` master; see [Security](#security)) |

With the opt-in set, the worker refuses to start — it exits with a `fatal:` line naming the reason —
when:

- the binary was built without the `remote-shell` feature;
- it runs standalone (`--config`, no `--master`): the shell is reached through the master, so a
  standalone worker has no use for it;
- `--master` is an `http://` URL and `--remote-shell-allow-plaintext` is not set;
- no `bash` is found on the worker's `PATH`. It is looked up there, not at `/bin/bash`; systemd's
  default `PATH` covers the usual locations, and on NixOS the unit needs one that includes it, e.g.
  `Environment=PATH=/run/current-system/sw/bin`.

## Enabling it on a host

For a worker installed by the dashboard's install command (see
[Install and Update Agents](/guides/agent-install/)), add the opt-in to the instance's environment
file and restart the instance:

```sh
echo 'GURU_REMOTE_SHELL=1' | sudo tee -a /etc/guru-worker/hk-1.env
sudo systemctl restart guru-worker@hk-1
```

The other settings go into the same file when their defaults do not suit, for example
`GURU_REMOTE_SHELL_IDLE_TIMEOUT_SECS=600`. Re-running the install command rewrites that file, so add
the line again afterwards.

A unit you wrote yourself — for example one [adopted from standalone
mode](/guides/independent-worker/#9-adopting-the-node-later) — takes the flag on its command line or
the variable in its environment:

```ini
ExecStart=/usr/local/bin/guru-worker --master https://guru.example.com --server <key> --remote-shell
```

Once the worker has registered again, the server's **Agent** section in the dashboard shows the
**Remote shell** button. To turn the shell off, remove the setting and restart: the worker
registers without the capability, the button disappears, and the restart has already ended every
session.

## Using it from the dashboard

Open the canvas, click the server, and find the **Agent** section of its panel. Admins see a
**Remote shell** button there for every server whose worker advertises the capability. It opens
*Remote shell · &lt;server&gt;*:

- **Sessions** lists the sessions running on that worker. **New session** starts one, **Attach**
  joins one that is already running — yours from an earlier visit, or another Admin's — and replays
  as much of its transcript as the buffer still holds.
- Type a command into *Command to run (bash)* and press **Run**. Each command is shown with its
  output, followed by `exit <code>` when it finishes; the input stays disabled while it runs.
- **Close session** kills the session and everything it started, after a confirmation.

Closing the dialog or the tab only detaches: the session and any running command go on, and
attaching again picks the transcript up where it is. A dropped connection resumes on its own, from
the last position the browser received.

When a session ends the transcript says why: *closed by an operator*, *idle timeout*, *the shell
exited* (for example after `exit`), or *the worker stopped*.

## Lifetime

A session ends in exactly one of these ways:

| End | When |
|---|---|
| Closed | Someone pressed **Close session**. The worker sends `SIGKILL` to the session's whole process group, so the shell and every process it started in that group are gone at once. |
| Idle timeout | No command has been running **and** no viewer has been attached for `--remote-shell-idle-timeout` (30 minutes by default). A running command or one open viewer keeps the session alive. |
| Shell exited | The shell ended on its own, for example after `exit`. A command that was still running reports its exit status first. |
| Worker stopped | The worker shut down or restarted — including a self-update. Every session is killed, and none is restored. |

A process that a command moved into a process group of its own — `setsid`, a daemon that detaches
itself — is not part of the session's group and survives a close. Under the installer's unit it
still ends with the worker: stopping or restarting `guru-worker@<unit>` makes systemd kill
everything in the unit's control group.

## How it travels

The worker always dials out; the master never connects to a worker. A worker that has opted in opens
one extra stream to the master after it registers, authenticated with its session's refresh key and
superseded by the next registration, the same way as its config stream; it reopens the stream on
its own when it breaks. The master relays requests and transcript entries between that stream and
the dashboard's calls, across replicas, without storing any of it. The browser receives the
transcript as server-sent events whose ids are transcript positions, which is how a reconnect asks
for exactly what it missed.

## Security

Turning the remote shell on makes the dashboard a way into the host. Decide per host, and enable it
only where you need it.

- **It runs as the worker's user, with that user's privileges.** Commands run as whatever user the
  worker runs as, with the same environment, limits and sandbox. Under the installer's unit that is
  the `guru-worker` system user, confined by the unit's `ProtectSystem=strict`, `ProtectHome=yes`,
  `PrivateTmp=yes` and `NoNewPrivileges=yes` — so `sudo` and setuid programs do not elevate, `/tmp`
  is the unit's private one, and only the state directory and the instance's own `bin/` are
  writable. The unit's `CAP_NET_BIND_SERVICE` is inherited too, so commands can bind ports below
  1024. A unit you wrote yourself that runs the worker as root gives every Admin a root shell.
- **An Admin account is a shell on every opted-in host.** Whoever signs in as an Admin — or steals
  an Admin's session — can run commands on every host that opted in. Keep the number of Admins
  small and their passwords strong; Maintainer and Observer accounts cannot use the shell.
- **The key is out of the environment, not out of reach.** `GURU_API_KEY` is removed from the
  shell's environment, but every file the worker's user can read is readable from the shell. Under
  the installer's layout that includes `/etc/guru-worker/<unit>.env` — the key of that instance and
  of every other instance on the same host, since all of them are group-readable by
  `guru-worker`. Anyone with the shell can also replace the binary in the instance's writable
  `bin/`, which the unit runs at its next start. Treat a remote shell as the full power of the
  worker on that host, not as a read-only view.
- **No plaintext by default.** Commands and their output cross the worker ↔ master link, so a
  worker with an `http://` (h2c) `--master` refuses to start with the opt-in set. Point the worker at
  an `https://` master instead. `--remote-shell-allow-plaintext` lifts the refusal; set it only when
  the link is already encrypted underneath — a WireGuard or SSH tunnel, a private network you trust
  as much as TLS. The check covers the worker's own link: serve the dashboard over HTTPS as well,
  since every command and its output also passes through the browser's connection.
- **Nothing is recorded.** There is no audit trail of who ran what and transcripts are not kept,
  so the host's own logging is the only record of what a command did.
