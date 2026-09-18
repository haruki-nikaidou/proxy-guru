-- Every account that asked for this notice kind on this canvas.
--
-- The fallback is resolved here: an account with its own row for the canvas is
-- served by that row, and one without is served by its default row — never by
-- both. A row that subscribed to the kind but turned every channel off is not a
-- recipient.
SELECT a.id AS "account!: AccountId",
       s.language AS "language!: Language",
       CASE WHEN s.email_enabled THEN a.email END AS "email?",
       s.telegram_chat AS "telegram_chat?"
FROM auth_account a
JOIN LATERAL (
    SELECT language, events, email_enabled, telegram_chat
    FROM notify_account_setting
    WHERE account = a.id AND canvas = $2
    UNION ALL
    SELECT language, events, email_enabled, telegram_chat
    FROM notify_account_default
    WHERE account = a.id
      AND NOT EXISTS (
          SELECT 1 FROM notify_account_setting
          WHERE account = a.id AND canvas = $2
      )
) s ON true
WHERE $1 = ANY (s.events)
  AND (s.email_enabled OR s.telegram_chat IS NOT NULL)
