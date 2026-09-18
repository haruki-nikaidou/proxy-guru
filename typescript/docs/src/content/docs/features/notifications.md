---
title: Notifications
description: How a server or pod health change becomes an email or a Telegram message, who receives it, and what the notifier mode does.
---

The health monitor decides *what* the fleet's status is. Notifications decide *who hears about it*:
every status change the master records can become an email or a Telegram message, in the language
the recipient chose.

Nothing is sent until an operator opts in. A fresh installation has no notification rows at all, and
an absent row means "no events", so deploying this feature notifies nobody.

## The two scopes

Both are keyed by a canvas — the workspace a notice is about.

| Scope | Row | Who edits it | Destinations |
|---|---|---|---|
| Workspace | `notify_canvas_setting` | `ViewWorkspace` to read, `EditWorkspace` to replace | A list of email addresses and a list of Telegram chat ids, shared by everyone watching that canvas |
| Personal | `notify_account_setting` | The account itself (a human session; an API key never) | The account's own email address, and one Telegram chat id |
| Personal default | `notify_account_default` | The account itself | The same, used for every canvas the account has no row for |

Each row carries a language and a set of event kinds. The workspace row's destinations are *not* a
fan-out over accounts: they are plain addresses and chat ids, so an on-call alias or a team channel
needs no account in this installation.

Personal resolution is per canvas: an account with a row for the canvas is served by that row, and
one without is served by its default row — never by both. That is what makes "everything by default,
except this one workspace in Japanese" expressible without touching every canvas.

The dashboard edits both on **Notifications** in a canvas's sidebar. The workspace card is read-only
for an Observer; the personal card is always editable, because it only ever writes the caller's own
row — the account id comes from the session, never from the request.

## The six event kinds

| Kind | Sent when |
|---|---|
| `server_online` | A server's status became `Online` |
| `server_degraded` | A server's status became `Degraded` |
| `server_offline` | A server's status became `Offline` (a report that stopped, a stream that closed, or the liveness sweep) |
| `pod_ready` | A pod's status became `Ready` |
| `pod_deploying` | A pod's status became `Deploying` (a new revision was published for it) |
| `pod_failed` | A pod's status became `Failed`; the worker's message is carried as the notice's detail |

The spelling is the same in the database's `CHECK`, in the API and in the config document.

## Only changes are announced

A worker reports every 15 s by default, and each report writes one row per pod. Notifying on rows
would mean a message per pod per interval, so the fan-out keeps its own memory of what it last
announced about each subject — `notify_server_state` and `notify_pod_state`, one row per server and
per pod — and drops everything that does not move:

- the stored status equals the new one → nothing is sent, and the row's `changed_at` is left alone;
- no stored status at all (the first time this subject is ever seen) → the status is recorded and
  **nothing is sent**. Otherwise installing the feature, or adding a server, would announce the
  whole fleet at once;
- the stored status differs → one notice per audience, and the row advances.

Deleting a server or a pod removes its row by foreign key; a subject that vanished between the write
and the fan-out is dropped rather than retried.

## How a notice travels

```text
health write ──▶ orchestration publishes `health_changed` (facts + labels)
                        │
                        ▼
              --mode consumer: fan-out
                 · drops facts that are not a change
                 · reads the settings, resolves the audiences
                 · publishes one notice per audience
                        │
                        ▼
              --mode notifier (exactly one): delivery
                 · renders subject + body once, in the notice's language
                 · sends to every destination the notice carries
```

The facts carry their own labels — the server's or pod's name, the canvas and its name — so the
notifier opens no database at all and a notice about a server that was since renamed still reads as
it did when it happened.

The split is deliberate. The fan-out is a database reader and scales with the other consumers; the
delivery side talks to an SMTP relay and to Telegram, and must not be run twice.

## `--mode notifier`

The fifth run mode of `guru-master`, and the only one this workspace requires to be single:

```sh
guru-master --mode notifier \
  --database-url "$GURU_DATABASE_URL" \
  --amqp-uri 'amqp://guru:guru@127.0.0.1:5672/'
```

- **Exactly one instance.** Before binding its queues it takes a session-scoped PostgreSQL advisory
  lock; a second instance exits non-zero with `another notifier already holds the advisory lock; run
  exactly one`. The lock is released by the connection dying, so a killed notifier never blocks its
  successor, and a restart that overlaps its predecessor fails fast instead of doubling messages.
- **No master key, no Redis.** It decrypts nothing and publishes nothing, so it takes neither
  `GURU_MASTER_KEY` nor `REDIS_URL` — only the database (for that lock and the `notify` config it
  reads at startup) and the broker.
- It logs which channels it ended up with, which is the quickest check that its secrets arrived:

```text
INFO guru_master: notification channel channel="email: configured"
INFO guru_master: notification channel channel="telegram: disabled (no GURU_TELEGRAM_BOT_TOKEN)"
```

A notifier that is down does not lose notices: they queue in `guru_notify_health_notify_group` and
`guru_notify_health_notify_personal` until it comes back.

## Channels and their secrets

The two secrets are **environment only**, never part of the config document — an Admin can read that
document in the dashboard, and a bot token is not a setting:

| Variable | Channel | Unset means |
|---|---|---|
| `GURU_SMTP_PASSWORD` | Email | The relay is used unauthenticated (what a relay on localhost expects) |
| `GURU_TELEGRAM_BOT_TOKEN` | Telegram | Telegram is disabled; a notice naming a chat id is logged and skipped |

Email goes out over one pooled SMTP connection, upgraded with STARTTLS unless `smtp_starttls` is
turned off (which speaks plain SMTP and is for a relay on localhost or a test sink only). Telegram is
one `sendMessage` call per chat against `telegram_api_base`; the token lives in the URL path, so no
request URL is ever logged.

Both channels retry a failed send `delivery_attempts` times, `delivery_retry_delay_secs` apart.

## Delivery is best-effort

The subject's state row advances when the notice is published, before it is sent. A notice lost to
an SMTP or Telegram outage is therefore **not** retried past `delivery_attempts`: a failed send is
logged and the remaining destinations are still attempted. Requeueing the delivery would re-run the
fan-out against a subject that now looks unchanged, and the next message about it would be its next
real change.

Two `consumer` replicas can also, in a narrow window, both observe the same transition and publish
duplicate notices. That is accepted: a lock per subject is not worth it for notifications, and the
duplicate says the same true thing twice.

Notifications are a convenience, not the control plane's record. What the fleet runs is decided by
the rows the publisher already committed, and the health page always shows the current truth.

## Configuration

One `app_config` key, `notify`, read once at startup like every other:

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

`smtp_host` empty — the default — disables email entirely. The full field list is in the
[configuration reference](/reference/configuration/#module-configuration). Saving it (from the CLI
or from **Management → Configuration** in the dashboard) takes effect when the masters restart; the
notifier builds its transport once, at startup.

## Sending a test notice

The **Send test notification** button on the Notifications page publishes one notice to the caller's
own channels, as they are in force for that canvas, ignoring which kinds they subscribed to — the
point is the channel, not the subscription. It travels the whole publish → notifier → send path, so
a message that arrives proves the notifier, its secrets and the relay all work.

It answers `FAILED_PRECONDITION` when the caller has no channel set up at all: turn email on, or set
a Telegram chat id, and save first.
