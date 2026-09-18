INSERT INTO notify_account_setting (account, canvas, language, events, email_enabled, telegram_chat)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (account, canvas) DO UPDATE
    SET language = EXCLUDED.language,
        events = EXCLUDED.events,
        email_enabled = EXCLUDED.email_enabled,
        telegram_chat = EXCLUDED.telegram_chat
RETURNING account AS "account: AccountId", canvas AS "canvas?: CanvasId",
          language AS "language: Language", events AS "events: Vec<NoticeKind>",
          email_enabled, telegram_chat
