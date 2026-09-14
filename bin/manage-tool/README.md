# `manage-tool`

Command-line administration tool for operating the application outside the
request path.

## Commands

- **`create-admin --email <email> --password <password>`** — bootstrap the first
  administrator account. It goes straight through the `auth` entity layer,
  because no acting admin exists yet to authorise the call.
- **`generate-master-key`** — print a fresh `GURU_MASTER_KEY`. Needs no
  database.
- **`config seed` / `list` / `get <key>` / `set <key> <json>`** — the
  database-backed configuration store (`app_config`). `seed` writes the
  defaults for every registered key that has none and never overwrites an
  edited one, so it is safe to run after every `surrealkit sync`. `list` and
  `get` print the row as stored, without decoding it — so a document that no
  longer matches its type can still be inspected and repaired — and fall back
  to the defaults only when the key has no row at all. `set` validates the
  payload against the config's type before storing it. The registry of keys is
  the `ConfigKey` enum — add a variant and the match arms will not compile
  until it is wired.
- **`orchestration export-config --server <server-key>`** — derive one server's
  ideal `guru-worker` TOML from the live canvas and print it to stdout, without
  touching the rollout state. It reads the stored `orchestration` config, so the
  output matches what a master derives.
- **`orchestration init-ca`** — create the internal CA that signs relay
  TLS/QUIC leaves and print its certificate. Refuses to replace an existing CA;
  needs `GURU_MASTER_KEY`.

Connection settings are read from flags, each with an environment fallback:
`--address`/`SURREALDB_HOST`, `--username`/`SURREALDB_USER`,
`--password`/`SURREALDB_PASSWORD`, `--namespace`/`SURREALDB_NAMESPACE`, and
`--database`/`SURREALDB_NAME`.

## Schema is not managed here

The SurrealDB schema lives in [`database/schema/*.surql`](../../database/schema)
and is managed with **surrealkit**: `surrealkit sync` against a development
database, `surrealkit rollout` for shared ones. This tool only reads and writes
rows through the module entities.

## Why a separate binary

Keeping administrative actions out of [`guru-master`](../guru-master) means the
server image stays focused on serving traffic, while destructive or
infrequent operations live in a tool run deliberately by an operator (or a
deployment job). It reuses the same modules and entities, so commands share the
exact types and queries the server uses.

## Conventions

- Build subcommands with a CLI parser (e.g. `clap`).
- Take connection settings from flags with an environment fallback (`clap`'s
  `env` attribute), not from a config file.
- Reuse module `entities`/`services` rather than issuing ad-hoc SurrealQL, so
  the tool and the server never drift apart.
