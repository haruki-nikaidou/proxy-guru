/**
 * Remote shell messages as the dashboard carries them: session rows for the
 * remote functions, transcript events as the frames of the SSE relay.
 */
import {
	ShellCloseReason,
	type ShellEvent,
	type ShellSession,
	ShellStream
} from 'app-protobuf/orchestration/orchestration';
import type { ShellCloseReasonName, ShellFrame, ShellSessionDto } from '#lib/dto/shell.js';

export const toShellSessionDto = (session: ShellSession): ShellSessionDto => ({
	sessionId: session.sessionId,
	openedAt: session.openedAt,
	running: session.running ?? null,
	viewers: session.viewers
});

function toCloseReason(reason: ShellCloseReason): ShellCloseReasonName {
	switch (reason) {
		case ShellCloseReason.CLOSED:
			return 'closed';
		case ShellCloseReason.IDLE:
			return 'idle';
		case ShellCloseReason.EXITED:
			return 'exited';
		case ShellCloseReason.SHUTDOWN:
			return 'shutdown';
		default:
			return 'unknown';
	}
}

/**
 * What one `ShellEvent` becomes on the wire: a frame with the transcript
 * positions it occupies (output: one per byte; started, finished: one), a
 * keep-alive, or nothing for an event this dashboard does not know.
 */
export type ShellWireEvent =
	| { kind: 'frame'; frame: ShellFrame; length: bigint }
	| { kind: 'keep-alive' }
	| { kind: 'unknown' };

export function toShellWireEvent(event: ShellEvent): ShellWireEvent {
	if (event.output) {
		return {
			kind: 'frame',
			frame: {
				type: 'output',
				stream: event.output.stream === ShellStream.STDERR ? 'stderr' : 'stdout',
				data: Buffer.from(
					event.output.data.buffer,
					event.output.data.byteOffset,
					event.output.data.byteLength
				).toString('base64')
			},
			length: BigInt(event.output.data.byteLength)
		};
	}
	if (event.started) {
		return {
			kind: 'frame',
			frame: { type: 'started', command: event.started.command },
			length: 1n
		};
	}
	if (event.finished) {
		return {
			kind: 'frame',
			frame: { type: 'finished', exitCode: event.finished.exitCode },
			length: 1n
		};
	}
	if (event.truncated) {
		return {
			kind: 'frame',
			frame: { type: 'truncated', dropped: Number(event.truncated.dropped) },
			length: 0n
		};
	}
	if (event.closed) {
		return {
			kind: 'frame',
			frame: { type: 'closed', reason: toCloseReason(event.closed.reason) },
			length: 0n
		};
	}
	if (event.keepAlive) return { kind: 'keep-alive' };
	return { kind: 'unknown' };
}
