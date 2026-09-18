INSERT INTO notify_canvas_setting (canvas, language, events, emails, telegram_chats)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (canvas) DO UPDATE
    SET language = EXCLUDED.language,
        events = EXCLUDED.events,
        emails = EXCLUDED.emails,
        telegram_chats = EXCLUDED.telegram_chats
RETURNING canvas AS "canvas: CanvasId", language AS "language: Language",
          events AS "events: Vec<NoticeKind>", emails, telegram_chats
