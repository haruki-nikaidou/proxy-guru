import type { Handle, HandleServerError } from '@sveltejs/kit/hooks';
import { describeException } from '#lib/errors.js';
import { getTextDirection } from '#lib/paraglide/runtime.js';
import { paraglideMiddleware } from '#lib/paraglide/server.js';
import { errorId } from '#lib/server/errors.js';

// No URL-based locale strategy is configured, so paraglide never rewrites the
// request; the locale comes from the `guru_locale` cookie or `accept-language`.
const handleParaglide: Handle = ({ event, resolve }) =>
	paraglideMiddleware(event.request, ({ locale }) =>
		resolve(event, {
			transformPageChunk: ({ html }) =>
				html
					.replace('%paraglide.lang%', locale)
					.replace('%paraglide.dir%', getTextDirection(locale))
		})
	);

export const handle: Handle = handleParaglide;

/**
 * Every error a request ends in reaches the browser with a code (see
 * `#lib/errors.ts`). One thrown with `error(...)` has its code already. An
 * unexpected exception is logged under a reference the browser shows, so what an
 * operator reports can be found in this server's log; its stack stays here.
 */
export const handleError: HandleServerError = ({ kind, error, event, issues }) => {
	switch (kind) {
		case 'app':
			return;
		case 'framework':
			return {
				code: error.status === 404 ? 'page_not_found' : 'http',
				detail: `HTTP ${error.status} ${error.message}`
			};
		case 'validation':
			// Arguments the remote function's schema refused: the page sent
			// something it should not have, which is a bug in the page.
			return {
				code: 'invalid_request',
				detail: issues
					.map(issue => {
						const path = (issue.path ?? [])
							.map(segment => String(typeof segment === 'object' ? segment.key : segment))
							.join('.');
						return `${path || '(arguments)'}: ${issue.message}`;
					})
					.join('\n')
			};
		case 'unknown': {
			const id = errorId();
			console.error(`[${id}] ${event.request.method} ${event.url.pathname}`, error);
			return {
				status: 500,
				message: 'Internal Error',
				code: 'internal',
				detail: describeException(error, false),
				id
			};
		}
	}
};
