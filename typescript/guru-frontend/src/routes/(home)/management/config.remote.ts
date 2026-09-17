import { error } from '@sveltejs/kit';
import type { ConfigDocument } from 'app-protobuf/base/config';
import * as v from 'valibot';
import type { ConfigDocumentDto, ConfigKeyName } from '#lib/dto/config.js';
import { callGrpc } from '#lib/server/errors.js';
import { authClient, orchestrationClient } from '#lib/server/grpc.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { command, query } from '$app/server';

const keySchema = v.picklist(['auth', 'orchestration'] as const, 'config_key_invalid');
/**
 * Only syntax is checked here — that answer needs no round trip. The document's
 * *shape* belongs to the control plane: it decodes the payload into the config
 * type, and its rejection message is what the operator has to read.
 */
const documentSchema = v.pipe(
	v.string(),
	v.trim(),
	v.minLength(1, 'config_json_required'),
	v.check(text => {
		try {
			JSON.parse(text);
			return true;
		} catch {
			return false;
		}
	}, 'config_json_invalid')
);

/**
 * A reply without a document is a protocol violation, not the "no row yet"
 * state — that one arrives as `stored: false` with the defaults in `json`.
 */
function toDto(key: ConfigKeyName, document: ConfigDocument | undefined): ConfigDocumentDto {
	if (!document) {
		error(500, {
			message: 'Internal Error',
			code: 'internal',
			detail: `Missing ConfigDocument for ${key}`
		});
	}
	return {
		key,
		stored: document.stored,
		json: document.json,
		defaultsJson: document.defaultsJson
	};
}

/**
 * Both stored documents, in a stable order. Each key lives on its own module's
 * service, so the two reads are independent and run concurrently; each gets its
 * own error boundary so neither hides the other's status code.
 */
export const listConfigDocuments = query(async (): Promise<ConfigDocumentDto[]> => {
	const metadata = sessionMetadata(requireSessionId());
	const [auth, orchestration] = await Promise.all([
		callGrpc(() => authClient().getAuthConfig({}, { metadata })),
		callGrpc(() => orchestrationClient().getOrchestrationConfig({}, { metadata }))
	]);
	return [toDto('auth', auth.config), toDto('orchestration', orchestration.config)];
});

/** Replaces the whole document; the reply is the control plane's fresh read. */
export const saveConfigDocument = command(
	v.object({ key: keySchema, json: documentSchema }),
	async ({ key, json }): Promise<ConfigDocumentDto> => {
		const metadata = sessionMetadata(requireSessionId());
		let saved: ConfigDocumentDto;
		switch (key) {
			case 'auth': {
				const reply = await callGrpc(() => authClient().setAuthConfig({ json }, { metadata }));
				saved = toDto('auth', reply.config);
				break;
			}
			case 'orchestration': {
				const reply = await callGrpc(() =>
					orchestrationClient().setOrchestrationConfig({ json }, { metadata })
				);
				saved = toDto('orchestration', reply.config);
				break;
			}
			default: {
				// Exhaustive on purpose: a third key fails to assign to `never`
				// here, so it cannot be added without wiring its typed RPC pair.
				const unhandled: never = key;
				error(500, {
					message: 'Internal Error',
					code: 'internal',
					detail: `Unknown config key ${String(unhandled)}`
				});
			}
		}
		await listConfigDocuments().refresh();
		return saved;
	}
);
