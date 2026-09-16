import * as v from 'valibot';

/**
 * Validation every route's remote functions share. Anything here is about the
 * shape of a request rather than about one feature's rules — those live next to
 * the feature, like `./topology/schemas.js`.
 *
 * The messages are codes, resolved by `#lib/i18n/codes.js`.
 */

/** A record id on the wire: opaque to the dashboard, but never empty. */
export const idSchema = v.pipe(v.string(), v.minLength(1, 'id_required'));
