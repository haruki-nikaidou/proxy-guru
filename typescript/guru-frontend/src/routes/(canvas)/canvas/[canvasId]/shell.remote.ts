/**
 * The remote shell on a server's worker: Admin only, and only on a worker that
 * advertised `remote_shell`. The control plane relays each call to the worker
 * and stores nothing. The transcript is streamed by `/shell/[serverId]/[sessionId]`.
 */
import * as v from 'valibot';
import { MAX_SHELL_COMMAND_BYTES, type ShellSessionDto } from '#lib/dto/shell.js';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { idSchema } from '#lib/server/schemas.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { toShellSessionDto } from '#lib/server/shell.js';
import { command, query } from '$app/server';

/** What the worker accepts as one command line. */
const shellCommandSchema = v.pipe(
	v.string(),
	v.check(text => text.trim() !== '', 'shell_command_required'),
	v.check(text => !text.includes('\0'), 'shell_command_invalid'),
	v.maxBytes(MAX_SHELL_COMMAND_BYTES, 'shell_command_too_long')
);

export const listShellSessions = query(
	v.object({ serverId: idSchema }),
	async ({ serverId }): Promise<ShellSessionDto[]> => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			orchestrationClient().listShellSessions({ serverId }, { metadata })
		);
		return reply.sessions.map(toShellSessionDto);
	}
);

/** Starts a bash on the worker; RESOURCE_EXHAUSTED once its session cap is reached. */
export const openShellSession = command(
	v.object({ serverId: idSchema }),
	async ({ serverId }): Promise<ShellSessionDto> => {
		const metadata = sessionMetadata(requireSessionId());
		const reply = await callGrpc(() =>
			orchestrationClient().openShellSession({ serverId }, { metadata })
		);
		if (!reply.session) throw new Error('OpenShellSession answered without a session');
		await listShellSessions({ serverId }).refresh();
		return toShellSessionDto(reply.session);
	}
);

/**
 * Answers once the worker accepted the command, not when it finishes: the exit
 * status arrives on the transcript. Refused while another command runs.
 */
export const sendShellCommand = command(
	v.object({ serverId: idSchema, sessionId: idSchema, command: shellCommandSchema }),
	async ({ serverId, sessionId, command }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().sendShellCommand({ serverId, sessionId, command }, { metadata })
		);
		return { ok: true as const };
	}
);

/** Kills the session's whole process group; its viewers see it closed. */
export const closeShellSession = command(
	v.object({ serverId: idSchema, sessionId: idSchema }),
	async ({ serverId, sessionId }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().closeShellSession({ serverId, sessionId }, { metadata })
		);
		await listShellSessions({ serverId }).refresh();
		return { ok: true as const };
	}
);
