/**
 * Every failure the dashboard shows is an `App.Error` with a `code`, so each
 * screen explains a failure the same way and nothing is shown as something it
 * is not:
 *
 * - a remote call rejects with the body the server sent (`#lib/server/errors.ts`
 *   and `handleError` in `hooks.server.ts` give every one a code);
 * - a `<svelte:boundary>` receives what `handleError` in `hooks.client.ts`
 *   returned for the error it caught;
 * - anything else a handler catches — a request that never reached the server,
 *   an exception thrown in the page — goes through `toAppError`.
 *
 * Server codes: `forbidden`, `not_found`, `page_not_found`, `unavailable`,
 * `conflict`, `timeout`, `unimplemented`, `control_plane`, `internal`,
 * `invalid_request`, `server_message`, `http`. Browser codes: `network`,
 * `stale_app`, `client_error`.
 */

/** Codes of failures that happened in the browser, where no HTTP status exists. */
const LOCAL_CODES = new Set(['network', 'stale_app', 'client_error']);
const DETAIL_LIMIT = 4000;
const STACK_FRAMES = 8;

function isAppError(value: unknown): value is App.Error {
	return (
		typeof value === 'object' &&
		value !== null &&
		typeof (value as { message?: unknown }).message === 'string' &&
		typeof (value as { status?: unknown }).status === 'number'
	);
}

const truncate = (text: string): string =>
	text.length > DETAIL_LIMIT ? `${text.slice(0, DETAIL_LIMIT)}…` : text;

/**
 * A body without a code did not come from this app: a proxy in front of a
 * dashboard that is down or restarting answers 502, 503 or 504 with a page of
 * its own, which the browser cannot tell from the network being down.
 */
export function withCode(error: App.Error): App.Error {
	if (error.code !== undefined) return error;
	const unreachable = error.status === 502 || error.status === 503 || error.status === 504;
	return {
		...error,
		code: unreachable ? 'network' : 'http',
		detail: error.detail ?? `HTTP ${error.status} ${error.message}`.trim()
	};
}

/** `fetch` rejects with a `TypeError` that each engine words its own way. */
function isNetworkFailure(err: unknown): boolean {
	return (
		err instanceof TypeError &&
		/failed to fetch|networkerror|load failed|network request failed/i.test(err.message)
	);
}

/**
 * A navigation whose code chunk did not load: the deployment that served the
 * page has been replaced, or the connection dropped. Reloading fixes either.
 */
function isStaleChunk(err: unknown): boolean {
	const message = err instanceof Error ? err.message : String(err);
	return /dynamically imported module|importing a module script failed/i.test(message);
}

/**
 * An exception, for the details section: its name and message, and — when
 * `stack` is set — the top of its stack. What is not an `Error` is shown as it is.
 */
export function describeException(err: unknown, stack = true): string {
	if (err instanceof Error) {
		const head = `${err.name}: ${err.message}`;
		if (!stack || !err.stack) return truncate(head);
		// V8 repeats the head as the first line of the stack; Gecko does not.
		const body = err.stack.startsWith(head) ? err.stack.slice(head.length) : err.stack;
		const frames = body
			.split('\n')
			.map(line => line.trim())
			.filter(line => line !== '')
			.slice(0, STACK_FRAMES);
		return truncate([head, ...frames].join('\n'));
	}
	if (typeof err === 'string') return truncate(err);
	try {
		return truncate(JSON.stringify(err) ?? String(err));
	} catch {
		return truncate(String(err));
	}
}

/** Something thrown in the browser, as the error a screen can show. */
export function exceptionError(err: unknown): App.Error {
	const detail = describeException(err);
	// First: V8 words a chunk that did not load "Failed to fetch dynamically
	// imported module", which would otherwise read as any failed request.
	if (isStaleChunk(err)) {
		return { status: 500, message: 'Stale App', code: 'stale_app', detail };
	}
	if (isNetworkFailure(err)) {
		return { status: 503, message: 'Network Error', code: 'network', detail };
	}
	return { status: 500, message: 'Internal Error', code: 'client_error', detail };
}

/** Whatever was thrown or rejected, as an `App.Error` with a code. */
export function toAppError(err: unknown): App.Error {
	// A rejected remote call: `HttpError` carries the body the server sent.
	const body = (err as { body?: unknown } | null | undefined)?.body;
	if (isAppError(body)) return withCode(body);
	// What a boundary receives has been through `handleError` already.
	if (isAppError(err) && !(err instanceof Error)) return withCode(err);
	return exceptionError(err);
}

/**
 * The details to show under an error and to copy: the status and code, the
 * reference of the server's log line, then what failed.
 */
export function errorDetails(error: App.Error): string {
	const head = [
		LOCAL_CODES.has(error.code ?? '') ? null : `HTTP ${error.status}`,
		error.code,
		error.id ? `ref ${error.id}` : null
	]
		.filter(part => part !== null && part !== undefined && part !== '')
		.join(' · ');
	return error.detail ? `${head}\n${error.detail}` : head;
}

/**
 * What the page should not report when nothing handled it: a request cancelled
 * by navigating away, a redirect SvelteKit follows itself, the benign
 * `ResizeObserver` loop warning browsers raise as an error event, and the bare
 * "Script error." a browser reports for a script of another origin — an
 * extension's, never the dashboard's.
 */
export function isIgnorable(err: unknown): boolean {
	if ((err as { name?: unknown } | null | undefined)?.name === 'AbortError') return true;
	const redirect = err as { status?: unknown; location?: unknown } | null | undefined;
	if (
		typeof redirect?.location === 'string' &&
		typeof redirect.status === 'number' &&
		redirect.status >= 300 &&
		redirect.status < 400
	) {
		return true;
	}
	const message = err instanceof Error ? err.message : typeof err === 'string' ? err : '';
	return /ResizeObserver loop/i.test(message) || /^Script error\.?$/.test(message);
}
