---
title: ACME with DNS
description: How a TLS pod gets a publicly trusted certificate through a DNS-01 challenge, and how the separate internal CA secures relay hops.
---

A client pod that terminates TLS needs a publicly trusted certificate for the name its clients
connect to. The control plane obtains one itself, over ACME with a DNS-01 challenge, so nothing has
to be reachable on port 80 and the certificate can be issued for a name whose traffic has not
started flowing yet. Relay hops between your own servers use a different, internal CA — the second
half of this page.

## What a pod declares

A TLS client pod carries four settings, edited in
[the pod's panel](/reference/nodes/#a-pods-panel-and-its-route):

| Setting | Meaning |
|---|---|
| `sni` | The host name clients present. Stored lower-cased; the certificate is keyed by that canonical form. |
| DNS provider | Which configured provider answers the challenge. |
| `domain_id` | Cloudflare: the zone id. Vercel: the registered domain, e.g. `example.com`. |
| ACME directory | Empty means the stored `default_acme_directory` (Let's Encrypt production). |

Three graph checks guard the combination, and an edit that trips one is refused:

- `invalid_sni` — the name is not a host name of at least two labels of ASCII letters, digits and
  hyphens. Wildcards are not accepted.
- `unknown_dns_provider` — the pod names a provider that does not exist.
- `certificate_issued_elsewhere` — a certificate for the same SNI and directory already exists with
  a different provider or domain id. The certificate is keyed by `(sni, directory)` alone, so every
  pod sharing an SNI must agree on how it is issued rather than have one pod's setting silently
  ignored.

## One row per name and directory

Each distinct `(sni, acme_directory)` resolves to one `certificate` row; the first pod to ask fixes
its provider and domain id, and later pods share it. The row holds the ACME account credentials and
the private key **encrypted** with `GURU_MASTER_KEY` (XChaCha20-Poly1305), the chain in clear, the
validity window, a `version`, and `last_error` plus `last_attempt_at` from the most recent attempt.

Its status is `pending`, `issued` or `failed`, and a pod only compiles into a config once the row is
issued *and* actually holds key and chain. A renewal that fails never downgrades a live
certificate: the failure is recorded on the row while the status stays `issued`.

## The issuance pass

`renew_certificates` is published every 60 s and consumed in `--mode consumer`, where the tick is
claimed in `orchestration_job_run` so the pass runs once fleet-wide per `acme_interval_secs`. One
pass:

1. reads every `client_tls` pod, resolves its directory, and creates the missing certificate rows;
2. lists the rows that are due — never attempted, or attempted longer ago than
   `acme_retry_after_secs` (1 h) and either not issued yet or expiring within
   `acme_renew_before_secs` (30 days);
3. claims each row with a compare-and-set on the `last_attempt_at` value it just observed, which is
   what keeps two masters from ordering the same certificate — and what holds a row back for an hour
   if a master crashed mid-order;
4. runs one order at a time, because a DNS-01 order takes minutes and the provider APIs rate-limit.

An order itself: restore the stored ACME account or register a new one, open the order for the SNI,
answer each pending authorization's `dns-01` challenge by creating a TXT record at
`_acme-challenge.<name>` with TTL 60 through the provider's API, wait for propagation, tell the CA
the challenge is ready, then poll, finalize and store. The TXT records are deleted afterwards
whether the order succeeded or not.

Propagation is checked against the Cloudflare and Google public resolvers, both of which must return
the exact challenge value, polled every 5 seconds for at most 120 seconds; the order and certificate
polls have their own 120-second cap. These four values are compiled in, not configuration keys, and
a timeout is reported as `TXT record _acme-challenge.<name> did not propagate within 120s`.

Storing a certificate bumps its `version`, clears `last_error`, and touches every canvas with a pod
for that SNI so derivation runs. Nothing is ever given up on permanently: any failure is recorded
and retried by a later pass after the retry window, without an attempt counter or extra backoff.

## While a certificate is missing

The pod does not compile. It contributes no forwarding, the rest of its server is derived and
published normally, and the pod appears in the server view's `invalid_pods` with one of three
messages:

| Message | Meaning |
|---|---|
| `certificate for <sni> is pending (not requested yet)` | No row exists yet — including the case of an SNI the issuer's stricter validation rejects, which never gets a row at all. |
| `certificate for <sni> is pending` | The row exists and has not been issued yet. |
| `certificate for <sni> is failed: <error>` | The last attempt failed; this is the message to read when a challenge is not working. |

The server does **not** go `Degraded` for a pending certificate: `invalid_pods` is not part of the
[status verdict](/features/health-monitor/#server-status). A pod that was already serving keeps its
listener if it later stops compiling.

## Renewal

An issued row is renewed once `not_after` falls inside `acme_renew_before_secs` (30 days). The
renewal rewrites the material in place — same row, same id, so the file paths on every worker stay
the same — and increments `version`. That version is what ships the new material: a snapshot pins
the versions its TOML references, so even though the TOML bytes are identical, the snapshot differs
and every server serving the certificate gets a new revision. The worker writes the new files
atomically, private keys at mode 0600.

## DNS providers

Two providers are supported. A provider row is a name, a kind, an optional `account_id` and the API
token, which is stored encrypted and is **never returned** by any API — every read path answers with
a summary that omits it.

| Provider | `domain_id` on the pod | `account_id` on the provider |
|---|---|---|
| Cloudflare | Zone id | Unused |
| Vercel | The registered domain | Team id; empty means the personal account |

`CreateDnsProvider`, `UpdateDnsProvider` and `DeleteDnsProvider` are Admin-only. An update with an
empty token keeps the stored one. A provider still referenced by a pod cannot be deleted:
`dns provider is still used by <n> pod(s)`.

## The TLS page

`/tls` is Admin-only in the dashboard and has two tabs. **DNS providers** creates, edits and deletes
providers; the kind cannot be changed after creation, and leaving the token blank on an edit keeps
the stored one. **Certificates** lists every row with its SNI, status, resolved provider, domain id,
ACME directory, validity window with an expiry hint and last attempt; a failed row expands to show
`last_error`.

Two actions per row, and neither issues anything synchronously:

- **Retry now** clears `last_error` and `last_attempt_at`, which makes the row due on the very next
  pass — on an issued certificate this is a forced renewal.
- **Delete** is refused while any pod still resolves to that certificate.

The page never creates a row. A row appears once the next `renew_certificates` pass reads a TLS
pod's certificate settings, which are edited on the canvas; derivation only observes the row and
reports the pod as invalid until it is issued.

## The internal relay CA

Relay hops over TLS or QUIC are traffic between your own servers, so they do not use ACME at all.
`manage-tool orchestration init-ca` creates one self-signed root — CN `guru internal relay CA`,
valid ten years, its key encrypted with `GURU_MASTER_KEY` — and prints the certificate to stdout.
There is deliberately no overwrite or rotate path for the root.

From then on the derivation pass issues one leaf per relay pod, valid `relay_cert_valid_secs`
(30 days), with CN and SAN `<pod-key>.relay.guru.internal`. The `rotate_relay_certificates` pass
runs hourly and rotates every leaf expiring within `relay_cert_renew_before_secs` (10 days), then
touches the affected canvases so the new material ships as a revision. Workers receive the root as
`certs/ca.pem` (`relay_ca` in the TOML) and verify relay peers against it.

A relay TLS or QUIC pod cannot compile before the CA exists — the edit is accepted but the pod is
reported as invalid, and `manage-tool orchestration init-ca` is the fix. Relay pods over plain TCP
need neither CA.

## Configuration

Stored under the `orchestration` config key, read once at master startup:

| Key | Default |
|---|---|
| `default_acme_directory` | `https://acme-v02.api.letsencrypt.org/directory` |
| `acme_renew_before_secs` | 2592000 (30 d) |
| `acme_retry_after_secs` | 3600 (1 h) |
| `acme_interval_secs` | 60 |
| `relay_cert_valid_secs` | 2592000 (30 d) |
| `relay_cert_renew_before_secs` | 864000 (10 d) |
| `relay_rotation_interval_secs` | 3600 |

The consumer needs outbound HTTPS to the ACME directory and to the DNS provider's API, and DNS to
the public resolvers it checks propagation against. See the
[configuration reference](/reference/configuration/) for the full document.
