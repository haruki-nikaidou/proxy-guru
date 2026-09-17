import { error } from '@sveltejs/kit';
import { CreateAccountResult } from 'app-protobuf/auth/auth';
import * as v from 'valibot';
import { type AccountRow, fromRoleName, toRoleName } from '#lib/dto/identity.js';
import { type AccountMutation, accountMutationBlock } from '#lib/guards.js';
import { callGrpc } from '#lib/server/errors.js';
import { authClient } from '#lib/server/grpc.js';
import { fetchIdentity } from '#lib/server/identity.js';
import { idSchema } from '#lib/server/schemas.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { command, form, query } from '$app/server';

const roleSchema = v.picklist(['admin', 'maintainer', 'observer'], 'role_invalid');
const emailSchema = v.pipe(
	v.string(),
	v.trim(),
	v.toLowerCase(),
	v.minLength(1, 'email_required'),
	v.email('email_invalid')
);
// The control plane accepts any password, including an empty one.
const passwordSchema = v.pipe(v.string(), v.minLength(8, 'password_too_short'));

export const listAccounts = query(async (): Promise<AccountRow[]> => {
	const metadata = sessionMetadata(requireSessionId());
	const { accounts } = await callGrpc(() => authClient().listAccounts({}, { metadata }));
	return accounts
		.map(account => ({ id: account.id, email: account.email, role: toRoleName(account.role) }))
		.sort((a, b) => a.email.localeCompare(b.email));
});

export const createAccount = form(
	v.object({ email: emailSchema, password: passwordSchema, role: roleSchema }),
	async ({ email, password, role }) => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			authClient().createAccount({ email, password, role: fromRoleName(role) }, { metadata })
		);

		if (reply.result === CreateAccountResult.CREATE_ACCOUNT_EMAIL_TAKEN) {
			return { error: 'email_taken' as const };
		}
		if (reply.result !== CreateAccountResult.CREATED) {
			error(500, {
				message: 'Internal Error',
				code: 'internal',
				detail: 'Unexpected CreateAccount result'
			});
		}

		await listAccounts().refresh();
		return { ok: true as const };
	}
);

/**
 * The backend has no self-demotion, self-deletion or last-admin guard, so this
 * layer is the authoritative one; the panel only mirrors it to disable controls.
 */
async function guard(sessionId: string, targetId: string, mutation: AccountMutation) {
	const metadata = sessionMetadata(sessionId);
	const [caller, roster] = await Promise.all([
		fetchIdentity(sessionId),
		callGrpc(() => authClient().listAccounts({}, { metadata }))
	]);

	const accounts = roster.accounts.map(account => ({
		id: account.id,
		email: account.email,
		role: toRoleName(account.role)
	}));

	const block = accountMutationBlock(caller.accountId, accounts, targetId, mutation);
	if (block) {
		error(400, {
			message:
				block === 'last_admin'
					? 'The last administrator cannot be removed'
					: 'Cannot modify your own account',
			code: block
		});
	}
}

export const setAccountRole = command(
	v.object({ accountId: idSchema, role: roleSchema }),
	async ({ accountId, role }) => {
		const sessionId = requireSessionId();
		await guard(sessionId, accountId, { kind: 'set_role', role });
		await callGrpc(() =>
			authClient().updateAccountRole(
				{ accountId, role: fromRoleName(role) },
				{ metadata: sessionMetadata(sessionId) }
			)
		);
		await listAccounts().refresh();
		return { ok: true as const };
	}
);

export const deleteAccount = command(v.object({ accountId: idSchema }), async ({ accountId }) => {
	const sessionId = requireSessionId();
	await guard(sessionId, accountId, { kind: 'delete' });
	await callGrpc(() =>
		authClient().deleteAccount({ accountId }, { metadata: sessionMetadata(sessionId) })
	);
	await listAccounts().refresh();
	return { ok: true as const };
});
