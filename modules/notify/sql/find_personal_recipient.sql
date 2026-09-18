-- One account's channels as they are in force for this canvas, whatever it
-- subscribed to: the test notification's recipient. Same fallback as
-- `list_personal_recipients.sql`; no row means no channel is set up.
SELECT a.id AS "account!: AccountId",
       s.language AS "language!: Language",
       CASE WHEN s.email_enabled THEN a.email END AS "email?",
       s.telegram_chat AS "telegram_chat?"
FROM auth_account a
JOIN LATERAL (
    SELECT language, email_enabled, telegram_chat
    FROM notify_account_setting
    WHERE account = a.id AND canvas = $2
    UNION ALL
    SELECT language, email_enabled, telegram_chat
    FROM notify_account_default
    WHERE account = a.id
      AND NOT EXISTS (
          SELECT 1 FROM notify_account_setting
          WHERE account = a.id AND canvas = $2
      )
) s ON true
WHERE a.id = $1
  AND (s.email_enabled OR s.telegram_chat IS NOT NULL)
