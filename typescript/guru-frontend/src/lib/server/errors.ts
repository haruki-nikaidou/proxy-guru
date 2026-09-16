import { error, redirect } from '@sveltejs/kit';
import { ClientError, Status } from 'nice-grpc';
import { clearSessionCookie } from './session.js';

/**
 * The single gRPC error boundary: wraps one client call and turns transport /
 * authz failures into SvelteKit errors carrying a stable `App.Error` code.
 *
 * Only the client call itself belongs inside `fn` — redirects and errors thrown
 * by surrounding code must not be swallowed here.
 */
export async function callGrpc<T>(fn: () => Promise<T>): Promise<T> {
	try {
		return await fn();
	} catch (err) {
		if (err instanceof ClientError) {
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
					throw error(403, { message: 'Forbidden', code: 'forbidden' });
				case Status.NOT_FOUND:
					throw error(404, { message: 'Not found', code: 'not_found' });
				case Status.UNAVAILABLE:
					// The control plane could not reach its database, or could not judge the
					// session in time. Deliberately NOT a redirect: the session is still
					// valid, and clearing the cookie here would turn a database blip into a
					// logout, which is exactly the bug this status exists to prevent.
					throw error(503, {
						message: 'The control plane is briefly unreachable. Try again in a moment.',
						code: 'unavailable'
					});
				case Status.INVALID_ARGUMENT:
				case Status.FAILED_PRECONDITION:
					// These carry actionable English text from the control plane.
					throw error(400, { message: err.details, code: 'server_message' });
			}
		}
		console.error(err);
		throw error(500, { message: 'Internal server error', code: 'internal' });
	}
}
