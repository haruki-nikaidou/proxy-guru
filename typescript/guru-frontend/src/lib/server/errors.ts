import { error, redirect } from '@sveltejs/kit';
import { ClientError, Status } from 'nice-grpc';
import { clearSessionCookie } from './session.js';

/** A short reference that ties what the browser shows to a line in this server's log. */
export function errorId(): string {
	return crypto.randomUUID().slice(0, 8);
}

/**
 * The single gRPC error boundary: wraps one client call and turns transport /
 * authz failures into SvelteKit errors carrying a stable `App.Error` code, with
 * the status and the control plane's own text as `detail`.
 *
 * Only the client call itself belongs inside `fn` — redirects and errors thrown
 * by surrounding code must not be swallowed here. Anything that is not a gRPC
 * failure is rethrown for `handleError` in `hooks.server.ts`, which logs it.
 */
export async function callGrpc<T>(fn: () => Promise<T>): Promise<T> {
	try {
		return await fn();
	} catch (err) {
		if (!(err instanceof ClientError)) throw err;
		grpcFailure(err);
	}
}

/**
 * What a gRPC status means for the caller: a redirect for a dead session, else
 * a SvelteKit error with a stable code. Shared by the unary boundary above and
 * by the live streams, so a stream that dies reads exactly like a call that
 * failed.
 */
export function grpcFailure(err: ClientError): never {
	const detail = `${Status[err.code]}: ${err.details}`;
	switch (err.code) {
		case Status.UNAUTHENTICATED:
			// Remote `query` functions run with cookie writes disabled, so this
			// is best-effort: the redirect is what matters. A stale cookie is
			// harmless — every protected path re-validates against the control
			// plane, and the `(home)` layout load (where writes are allowed)
			// clears it on the next visit.
			try {
				clearSessionCookie();
			} catch {
				/* SvelteKit forbids cookie mutation in a remote query. */
			}
			throw redirect(303, '/auth');
		case Status.PERMISSION_DENIED:
			throw error(403, { message: 'Forbidden', code: 'forbidden', detail });
		case Status.NOT_FOUND:
			throw error(404, { message: 'Not found', code: 'not_found', detail });
		case Status.UNAVAILABLE:
			// The control plane could not reach its database, or could not judge the
			// session in time — or this server cannot reach the control plane at
			// all; `detail` tells which. Deliberately NOT a redirect: the session is
			// still valid, and clearing the cookie here would turn a database blip
			// into a logout, which is exactly the bug this status exists to prevent.
			throw error(503, {
				message: 'The control plane is briefly unreachable. Try again in a moment.',
				code: 'unavailable',
				detail
			});
		case Status.INVALID_ARGUMENT:
		case Status.FAILED_PRECONDITION:
		case Status.ALREADY_EXISTS:
			// These carry actionable English text from the control plane.
			throw error(400, { message: err.details, code: 'server_message' });
		case Status.ABORTED:
			// Another write won the race for the same rows; nothing was written.
			throw error(409, { message: 'Conflict', code: 'conflict', detail });
		case Status.DEADLINE_EXCEEDED:
			throw error(504, { message: 'Timed out', code: 'timeout', detail });
		case Status.UNIMPLEMENTED:
			throw error(501, { message: 'Not implemented', code: 'unimplemented', detail });
		default: {
			const id = errorId();
			console.error(`[${id}] control plane call failed: ${detail}`);
			throw error(502, { message: 'Control plane error', code: 'control_plane', detail, id });
		}
	}
}
