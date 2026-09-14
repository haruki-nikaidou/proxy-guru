-- Base module schema.
--
-- Single source of truth for the `base` module's tables. Applied in development
-- with `surrealkit sync`; the module's integration tests apply this exact file
-- against an in-memory database.
--
-- `app_config` is the installation-wide configuration store: one row per config
-- key, holding the whole serialised config struct. The database is the only
-- source of truth (no cache, no second copy), so every `guru-master` process in
-- the fleet starts from the same values without any matching environment. The
-- record key *is* the config key, and `key` is stored alongside it so a row is
-- readable on its own; the UNIQUE index keeps a second row from claiming the
-- same key under a different record id.
DEFINE TABLE app_config SCHEMAFULL;
DEFINE FIELD key ON app_config TYPE string;
-- The serialised config struct. FLEXIBLE because each key has a different
-- shape; this is the SurrealDB equivalent of a `jsonb` column.
DEFINE FIELD content ON app_config TYPE object FLEXIBLE;
DEFINE INDEX app_config_key ON app_config FIELDS key UNIQUE;
