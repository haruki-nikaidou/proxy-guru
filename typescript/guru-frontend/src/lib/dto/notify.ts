/**
 * Notification settings, in the two scopes the control plane keeps them: the
 * workspace's shared destinations and one operator's own channels.
 *
 * Protobuf never reaches the client, so the enums are the same snake_case
 * spellings the database and the config document use.
 *
 * Both scopes default to **no events**: a canvas nobody configured notifies
 * nobody, and a `Get` of a row that does not exist answers those defaults
 * rather than failing.
 */

export const NOTIFY_LANGUAGES = ['en', 'ja', 'zh_cn'] as const;
export type NotifyLanguageName = (typeof NOTIFY_LANGUAGES)[number];

export const NOTICE_KINDS = [
	'server_online',
	'server_degraded',
	'server_offline',
	'pod_ready',
	'pod_deploying',
	'pod_failed'
] as const;
export type NoticeKindName = (typeof NOTICE_KINDS)[number];

/** The destinations everyone watching this canvas shares. */
export type CanvasNotifySettingDto = {
	canvasId: string;
	language: NotifyLanguageName;
	events: NoticeKindName[];
	emails: string[];
	telegramChats: string[];
};

/**
 * The caller's own channels as they are in force for a canvas: its row for that
 * canvas, or its account-wide default row when the canvas has none. Mail goes to
 * the account's own address, so only the switch is stored; an empty
 * `telegramChat` means unset.
 */
export type PersonalNotifySettingDto = {
	canvasId: string;
	language: NotifyLanguageName;
	events: NoticeKindName[];
	emailEnabled: boolean;
	telegramChat: string;
};
