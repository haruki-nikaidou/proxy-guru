import { m } from '#lib/paraglide/messages.js';
import { getLocale } from '#lib/paraglide/runtime.js';

/** Backend timestamps are RFC3339 with nanoseconds, or `''` when absent. */
export function formatTimestamp(rfc3339: string): string {
	if (rfc3339 === '') return m.common_never();
	const parsed = new Date(rfc3339);
	return Number.isNaN(parsed.getTime()) ? rfc3339 : parsed.toLocaleString(getLocale());
}
