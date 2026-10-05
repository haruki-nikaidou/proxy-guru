/**
 * A remote shell session's transcript as server-sent events, relaying
 * `WatchShellSession` for the browser's `EventSource`.
 *
 * Every frame that occupies transcript positions carries the position right
 * after it as its SSE `id` (a `truncated` frame: the oldest position kept), so
 * the reconnect `EventSource` makes on its own sends it back as
 * `Last-Event-ID` and resumes without a gap or a repeat. A first connect
 * starts at `?from=` (0: the session's start).
 *
 * A refusal before the control plane accepted the watch answers with an HTTP
 * status, which ends the `EventSource` for good (it only reconnects after a
 * stream that was open); the page then finds out why on its own. A failure
 * after that just ends the response, and the `EventSource` reconnects.
 */

import type { ShellEvent } from 'app-protobuf/orchestration/orchestration';
import { ClientError, Status } from 'nice-grpc';
import { errorId } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { clearSessionCookie, SESSION_COOKIE, sessionMetadata } from '#lib/server/session.js';
import { toShellWireEvent } from '#lib/server/shell.js';
import type { RequestHandler } from './$types.js';

/** How long the browser waits before it reconnects a stream that ended. */
const RETRY_MS = 2000;
const MAX_OFFSET = 0xffff_ffff_ffff_ffffn;

/** The statuses that end the `EventSource`; anything unlisted reads as a bad gateway. */
const HTTP_STATUS: Partial<Record<Status, number>> = {
	[Status.INVALID_ARGUMENT]: 400,
	[Status.UNAUTHENTICATED]: 401,
	[Status.PERMISSION_DENIED]: 403,
	[Status.NOT_FOUND]: 404,
	[Status.FAILED_PRECONDITION]: 412,
	[Status.RESOURCE_EXHAUSTED]: 429,
	[Status.UNAVAILABLE]: 503,
	[Status.DEADLINE_EXCEEDED]: 504
};

const plain = (status: number, text: string): Response =>
	new Response(text, {
		status,
		headers: { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' }
	});

/** A transcript position as the browser sent it, or `null` when it is not one. */
function parseOffset(value: string | null): bigint | null {
	if (value === null || value === '') return 0n;
	if (!/^\d{1,20}$/.test(value)) return null;
	const offset = BigInt(value);
	return offset > MAX_OFFSET ? null : offset;
}

function refusal(err: ClientError): Response {
	const detail = `${Status[err.code]}: ${err.details}`;
	const status = HTTP_STATUS[err.code];
	if (err.code === Status.UNAUTHENTICATED) clearSessionCookie();
	if (status !== undefined) return plain(status, detail);
	console.error(`[${errorId()}] shell watch refused: ${detail}`);
	return plain(502, detail);
}

/** One SSE message, or `null` for an event this dashboard does not know. */
function encodeEvent(event: ShellEvent): string | null {
	const wire = toShellWireEvent(event);
	switch (wire.kind) {
		case 'keep-alive':
			return ': keep-alive\n\n';
		case 'unknown':
			return null;
		case 'frame': {
			// A `truncated` frame occupies nothing but moves the resume point to
			// the oldest position kept, its own offset: without an id, a reconnect
			// would ask again from before the gap and be told about it twice.
			const resume = wire.length > 0n || wire.frame.type === 'truncated';
			const id = resume ? `id: ${event.offset + wire.length}\n` : '';
			return `${id}data: ${JSON.stringify(wire.frame)}\n\n`;
		}
	}
}

export const GET: RequestHandler = async ({ params, request, cookies, url }) => {
	const sessionId = cookies.get(SESSION_COOKIE);
	if (!sessionId) return plain(401, 'UNAUTHENTICATED');
	const fromOffset = parseOffset(
		request.headers.get('last-event-id') ?? url.searchParams.get('from')
	);
	if (fromOffset === null) return plain(400, 'invalid offset');

	const abort = new AbortController();
	request.signal.addEventListener('abort', () => abort.abort(), { once: true });

	// The control plane sends the response headers once it accepted the watch
	// (the worker answered the attach); a refusal comes before them, as a
	// trailers-only status. Waiting for those headers rather than for a first
	// event keeps an idle session from holding the response back until the
	// next keep-alive.
	const { promise: accepted, resolve: accept } = Promise.withResolvers<void>();
	const events = orchestrationClient()
		.watchShellSession(
			{ serverId: params.serverId, sessionId: params.sessionId, fromOffset },
			{ metadata: sessionMetadata(sessionId), signal: abort.signal, onHeader: () => accept() }
		)
		[Symbol.asyncIterator]();
	let pending: Promise<IteratorResult<ShellEvent>> | null = events.next();

	try {
		await Promise.race([accepted, pending]);
	} catch (err) {
		abort.abort();
		if (request.signal.aborted) return new Response(null, { status: 204 });
		if (err instanceof ClientError) return refusal(err);
		throw err;
	}

	const encoder = new TextEncoder();
	const body = new ReadableStream<Uint8Array>({
		start(controller) {
			controller.enqueue(encoder.encode(`retry: ${RETRY_MS}\n\n`));
		},
		// Each pull enqueues at least once or ends the stream: a pull that
		// resolves with nothing enqueued is not called again.
		async pull(controller) {
			try {
				while (true) {
					const result = await (pending ?? events.next());
					pending = null;
					if (result.done) {
						controller.close();
						return;
					}
					const message = encodeEvent(result.value);
					if (message !== null) {
						controller.enqueue(encoder.encode(message));
						return;
					}
				}
			} catch (err) {
				// The browser reconnects from its last id; only what is not a
				// transport or control-plane failure is worth a log line.
				if (!abort.signal.aborted && !(err instanceof ClientError)) {
					console.error(`[${errorId()}] shell watch relay failed`, err);
				}
				abort.abort();
				controller.close();
			}
		},
		cancel() {
			abort.abort();
		}
	});

	return new Response(body, {
		headers: {
			'content-type': 'text/event-stream',
			'cache-control': 'no-cache',
			'x-accel-buffering': 'no'
		}
	});
};
