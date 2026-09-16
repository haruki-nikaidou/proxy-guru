import * as v from 'valibot';
import type { ApiKeyRow } from '#lib/dto/identity.js';
import { callGrpc } from '#lib/server/errors.js';
import { authClient } from '#lib/server/grpc.js';
import { idSchema } from '#lib/server/schemas.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { command, form, query } from '$app/server';

const keyNameSchema = v.pipe(
	v.string(),
	v.trim(),
	v.minLength(1, 'api_key_name_required'),
	v.maxLength(64, 'api_key_name_too_long')
);

/** `ListApiKeys` only ever returns the caller's own keys — they are self-service. */
export const listApiKeys = query(async (): Promise<ApiKeyRow[]> => {
	const metadata = sessionMetadata(requireSessionId());
	const { keys } = await callGrpc(() => authClient().listApiKeys({}, { metadata }));
	return keys.map(key => ({ id: key.id, name: key.name, createdAt: key.createdAt }));
});

export const createApiKey = form(v.object({ name: keyNameSchema }), async ({ name }) => {
	const metadata = sessionMetadata(requireSessionId());
	const reply = await callGrpc(() => authClient().createApiKey({ name }, { metadata }));
	await listApiKeys().refresh();
	// The plaintext secret is unrecoverable after this response.
	return { ok: true as const, id: reply.id, secret: reply.secret };
});

export const revokeApiKey = command(v.object({ keyId: idSchema }), async ({ keyId }) => {
	const metadata = sessionMetadata(requireSessionId());
	await callGrpc(() => authClient().revokeApiKey({ id: keyId }, { metadata }));
	await listApiKeys().refresh();
	return { ok: true as const };
});
