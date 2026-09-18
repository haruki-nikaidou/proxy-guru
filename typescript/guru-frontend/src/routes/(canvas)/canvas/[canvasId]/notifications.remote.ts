import { error } from '@sveltejs/kit';
import {
	type CanvasNotifySetting,
	HealthNoticeKind,
	NotifyLanguage,
	type PersonalNotifySetting
} from 'app-protobuf/notify/notify';
import * as v from 'valibot';
import {
	type CanvasNotifySettingDto,
	NOTICE_KINDS,
	NOTIFY_LANGUAGES,
	type NoticeKindName,
	type NotifyLanguageName,
	type PersonalNotifySettingDto
} from '#lib/dto/notify.js';
import { callGrpc } from '#lib/server/errors.js';
import { notifyClient } from '#lib/server/grpc.js';
import { idSchema } from '#lib/server/schemas.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { command, query } from '$app/server';

/**
 * The wire enums, both ways. The dashboard speaks the snake_case spellings the
 * database and the config document use; protobuf numbers stay in this module.
 */
const LANGUAGE_TO_PROTO: Record<NotifyLanguageName, NotifyLanguage> = {
	en: NotifyLanguage.EN,
	ja: NotifyLanguage.JA,
	zh_cn: NotifyLanguage.ZH_CN
};
const KIND_TO_PROTO: Record<NoticeKindName, HealthNoticeKind> = {
	server_online: HealthNoticeKind.SERVER_ONLINE,
	server_degraded: HealthNoticeKind.SERVER_DEGRADED,
	server_offline: HealthNoticeKind.SERVER_OFFLINE,
	pod_ready: HealthNoticeKind.POD_READY,
	pod_deploying: HealthNoticeKind.POD_DEPLOYING,
	pod_failed: HealthNoticeKind.POD_FAILED
};

const languageSchema = v.picklist(NOTIFY_LANGUAGES, 'notify_language_invalid');
const eventsSchema = v.array(v.picklist(NOTICE_KINDS, 'notify_event_invalid'));
const emailsSchema = v.array(
	v.pipe(v.string(), v.trim(), v.minLength(1, 'email_required'), v.email('email_invalid'))
);
const chatsSchema = v.array(v.pipe(v.string(), v.trim(), v.minLength(1, 'notify_chat_required')));
const chatSchema = v.pipe(v.string(), v.trim(), v.maxLength(64, 'notify_chat_too_long'));

/**
 * An unrecognised enum value on the wire is a control plane newer than this
 * dashboard, not a value to guess at: naming it is what makes the mismatch
 * debuggable.
 */
function languageName(language: NotifyLanguage): NotifyLanguageName {
	const found = NOTIFY_LANGUAGES.find(name => LANGUAGE_TO_PROTO[name] === language);
	if (!found) {
		error(500, {
			message: 'Internal Error',
			code: 'internal',
			detail: `Unknown NotifyLanguage ${language}`
		});
	}
	return found;
}

/** Same for the kinds; an unknown one is dropped rather than failing the read. */
function kindNames(events: HealthNoticeKind[]): NoticeKindName[] {
	return events.flatMap(event => {
		const found = NOTICE_KINDS.find(name => KIND_TO_PROTO[name] === event);
		return found ? [found] : [];
	});
}

/**
 * A reply without a setting is a protocol violation, not the "no row yet"
 * state — that one arrives as the defaults.
 */
function canvasDto(
	canvasId: string,
	setting: CanvasNotifySetting | undefined
): CanvasNotifySettingDto {
	if (!setting) {
		error(500, {
			message: 'Internal Error',
			code: 'internal',
			detail: `Missing CanvasNotifySetting for ${canvasId}`
		});
	}
	return {
		canvasId: setting.canvas,
		language: languageName(setting.language),
		events: kindNames(setting.events),
		emails: setting.emails,
		telegramChats: setting.telegramChats
	};
}

function personalDto(
	canvasId: string,
	setting: PersonalNotifySetting | undefined
): PersonalNotifySettingDto {
	if (!setting) {
		error(500, {
			message: 'Internal Error',
			code: 'internal',
			detail: `Missing PersonalNotifySetting for ${canvasId}`
		});
	}
	return {
		canvasId: setting.canvas,
		language: languageName(setting.language),
		events: kindNames(setting.events),
		emailEnabled: setting.emailEnabled,
		telegramChat: setting.telegramChat
	};
}

export const getCanvasNotifySetting = query(
	v.object({ canvasId: idSchema }),
	async ({ canvasId }): Promise<CanvasNotifySettingDto> => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			notifyClient().getCanvasNotifySetting({ canvas: canvasId }, { metadata })
		);
		return canvasDto(canvasId, reply.setting);
	}
);

export const getPersonalNotifySetting = query(
	v.object({ canvasId: idSchema }),
	async ({ canvasId }): Promise<PersonalNotifySettingDto> => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			notifyClient().getPersonalNotifySetting({ canvas: canvasId }, { metadata })
		);
		return personalDto(canvasId, reply.setting);
	}
);

/**
 * Replaces the workspace row. A `command` rather than a `form`: the payload is
 * two lists and a set of switches the page assembles, which a form's flat
 * field names cannot carry, and the page has to keep an unsaved draft anyway.
 */
export const saveCanvasNotifySetting = command(
	v.object({
		canvasId: idSchema,
		language: languageSchema,
		events: eventsSchema,
		emails: emailsSchema,
		telegramChats: chatsSchema
	}),
	async ({ canvasId, language, events, emails, telegramChats }) => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			notifyClient().setCanvasNotifySetting(
				{
					setting: {
						canvas: canvasId,
						language: LANGUAGE_TO_PROTO[language],
						events: events.map(event => KIND_TO_PROTO[event]),
						emails,
						telegramChats
					}
				},
				{ metadata }
			)
		);
		const saved = canvasDto(canvasId, reply.setting);
		await getCanvasNotifySetting({ canvasId }).refresh();
		return saved;
	}
);

/** Replaces the caller's own row for this canvas. */
export const savePersonalNotifySetting = command(
	v.object({
		canvasId: idSchema,
		language: languageSchema,
		events: eventsSchema,
		emailEnabled: v.boolean(),
		telegramChat: chatSchema
	}),
	async ({ canvasId, language, events, emailEnabled, telegramChat }) => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			notifyClient().setPersonalNotifySetting(
				{
					setting: {
						canvas: canvasId,
						language: LANGUAGE_TO_PROTO[language],
						events: events.map(event => KIND_TO_PROTO[event]),
						emailEnabled,
						telegramChat
					}
				},
				{ metadata }
			)
		);
		const saved = personalDto(canvasId, reply.setting);
		await getPersonalNotifySetting({ canvasId }).refresh();
		return saved;
	}
);

/**
 * Sends one notice to the caller's own channels, through the same publish →
 * notifier → send path a real notice takes. `FAILED_PRECONDITION` (shown
 * verbatim) when no channel is set up.
 */
export const sendTestNotification = command(
	v.object({ canvasId: idSchema }),
	async ({ canvasId }) => {
		const metadata = sessionMetadata(requireSessionId());
		const { sent } = await callGrpc(() =>
			notifyClient().sendTestNotification({ canvas: canvasId }, { metadata })
		);
		return { sent };
	}
);
