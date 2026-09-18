-- Notification settings and the fan-out's own memory.
--
-- Two scopes, both keyed by canvas ("workspace"): the canvas row carries the
-- destinations a whole workspace shares, and an account row carries one
-- operator's own channels. An account may keep a per-canvas row and one default
-- row (`notify_account_default`), which serves every canvas that has none.
--
-- `events` is the set of notice kinds a row wants, spelled exactly as
-- `notify::entities::db::setting::NoticeKind`'s `text_enum!`. It defaults to the
-- empty set on purpose: installing the module must notify nobody until an
-- operator opts in.

CREATE TABLE notify_canvas_setting (
    canvas          text PRIMARY KEY REFERENCES orchestration_canvas (id) ON DELETE CASCADE,
    language        text   NOT NULL DEFAULT 'en' CHECK (language IN ('en', 'ja', 'zh_cn')),
    events          text[] NOT NULL DEFAULT '{}' CHECK (events <@ ARRAY[
                        'server_online', 'server_degraded', 'server_offline',
                        'pod_ready', 'pod_deploying', 'pod_failed'
                    ]::text[]),
    -- The destinations the workspace shares; no per-account fan-out.
    emails          text[] NOT NULL DEFAULT '{}',
    telegram_chats  text[] NOT NULL DEFAULT '{}'
);

-- The account's default, used for any canvas without its own row.
CREATE TABLE notify_account_default (
    account         text PRIMARY KEY REFERENCES auth_account (id) ON DELETE CASCADE,
    language        text    NOT NULL DEFAULT 'en' CHECK (language IN ('en', 'ja', 'zh_cn')),
    events          text[]  NOT NULL DEFAULT '{}' CHECK (events <@ ARRAY[
                        'server_online', 'server_degraded', 'server_offline',
                        'pod_ready', 'pod_deploying', 'pod_failed'
                    ]::text[]),
    -- Mail goes to the account's own address; only the switch is stored.
    email_enabled   boolean NOT NULL DEFAULT false,
    telegram_chat   text
);

CREATE TABLE notify_account_setting (
    account         text NOT NULL REFERENCES auth_account (id) ON DELETE CASCADE,
    canvas          text NOT NULL REFERENCES orchestration_canvas (id) ON DELETE CASCADE,
    language        text    NOT NULL DEFAULT 'en' CHECK (language IN ('en', 'ja', 'zh_cn')),
    events          text[]  NOT NULL DEFAULT '{}' CHECK (events <@ ARRAY[
                        'server_online', 'server_degraded', 'server_offline',
                        'pod_ready', 'pod_deploying', 'pod_failed'
                    ]::text[]),
    email_enabled   boolean NOT NULL DEFAULT false,
    telegram_chat   text,
    PRIMARY KEY (account, canvas)
);
CREATE INDEX notify_account_setting_canvas ON notify_account_setting (canvas);

-- What the fan-out last announced about one subject, so a repeated report of the
-- same status notifies nobody. FK cascade is the only cleanup needed: a deleted
-- server or pod has nothing left to announce.
CREATE TABLE notify_server_state (
    server      text PRIMARY KEY REFERENCES orchestration_server (id) ON DELETE CASCADE,
    status      text NOT NULL CHECK (status IN ('online', 'degraded', 'offline')),
    changed_at  timestamptz NOT NULL
);

CREATE TABLE notify_pod_state (
    pod         text PRIMARY KEY REFERENCES orchestration_pod (id) ON DELETE CASCADE,
    status      text NOT NULL CHECK (status IN ('ready', 'deploying', 'failed')),
    changed_at  timestamptz NOT NULL
);
