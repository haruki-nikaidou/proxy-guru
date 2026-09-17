import { error } from '@sveltejs/kit';
import { ChangeEmailResult, ChangePasswordResult } from 'app-protobuf/auth/auth';
import * as v from 'valibot';
import { callGrpc } from '#lib/server/errors.js';
import { authClient } from '#lib/server/grpc.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { form } from '$app/server';

const emailSchema = v.pipe(
	v.string(),
	v.trim(),
	v.toLowerCase(),
	v.minLength(1, 'email_required'),
	v.email('email_invalid')
);
const currentPasswordSchema = v.pipe(v.string(), v.minLength(1, 'password_required'));
const passwordSchema = v.pipe(v.string(), v.minLength(8, 'password_too_short'));

export const changeEmail = form(
	v.object({ newEmail: emailSchema, currentPassword: currentPasswordSchema }),
	async ({ newEmail, currentPassword }) => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			authClient().changeEmail({ newEmail, currentPassword }, { metadata })
		);

		switch (reply.result) {
			case ChangeEmailResult.CHANGE_EMAIL_CHANGED:
				return { ok: true as const };
			case ChangeEmailResult.EMAIL_TAKEN:
				return { error: 'email_taken' as const };
			case ChangeEmailResult.CHANGE_EMAIL_WRONG_PASSWORD:
				return { error: 'wrong_password' as const };
			default:
				error(500, {
					message: 'Internal Error',
					code: 'internal',
					detail: 'Unexpected ChangeEmail result'
				});
		}
	}
);

export const changePassword = form(
	v.pipe(
		v.object({
			currentPassword: currentPasswordSchema,
			newPassword: passwordSchema,
			confirmPassword: v.string()
		}),
		v.forward(
			v.partialCheck(
				[['newPassword'], ['confirmPassword']],
				input => input.newPassword === input.confirmPassword,
				'password_mismatch'
			),
			['confirmPassword']
		)
	),
	async ({ currentPassword, newPassword }) => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			authClient().changePassword({ currentPassword, newPassword }, { metadata })
		);

		// The backend does not invalidate other sessions on a password change.
		switch (reply.result) {
			case ChangePasswordResult.CHANGED:
				return { ok: true as const };
			case ChangePasswordResult.WRONG_PASSWORD:
				return { error: 'wrong_password' as const };
			default:
				error(500, {
					message: 'Internal Error',
					code: 'internal',
					detail: 'Unexpected ChangePassword result'
				});
		}
	}
);
