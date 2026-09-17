import type { HandleClientError } from '@sveltejs/kit/hooks';
import { exceptionError, withCode } from '#lib/errors.js';

/**
 * Runs for every error while navigating or rendering — including each one a
 * `<svelte:boundary>` catches: its `failed` snippet receives what this returns.
 * A failed remote query comes through here too, before the query rejects.
 */
export const handleError: HandleClientError = ({ kind, error }) => {
	switch (kind) {
		case 'app':
			// A body the server sent has its code; a proxy's error page has none.
			return error.code === undefined ? withCode(error) : undefined;
		case 'framework':
			return {
				code: error.status === 404 ? 'page_not_found' : 'http',
				detail: `HTTP ${error.status} ${error.message}`
			};
		case 'unknown':
			console.error(error);
			return exceptionError(error);
	}
};
