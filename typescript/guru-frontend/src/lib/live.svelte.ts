import { toAppError } from '#lib/errors.js';
import type { RemoteLiveQuery } from '$app/server';

/**
 * The codes of a failure that time fixes: the proxy answering 502/503/504
 * while this server restarts (`network`), and the control plane not up yet or
 * too slow (`unavailable`, `timeout`).
 */
const TRANSIENT = new Set(['network', 'unavailable', 'timeout']);

const MAX_DELAY_MS = 30_000;

/**
 * Reconnects a live query that failed for a reason time fixes. SvelteKit ends a
 * live query for good on any HTTP error, and a deploy produces exactly those:
 * the proxy answers 502 while the dashboard restarts, the control plane 503
 * while it starts. Without this, every open page stops following the streams
 * after a deploy until someone presses Try again. Backs off from 1 s to 30 s;
 * a value arriving clears the error and resets the backoff.
 */
export function reconnectWhenTransient(
	query: () => Pick<RemoteLiveQuery<unknown>, 'error' | 'reconnect'>
): void {
	// Not `$state`: only the effect reads and writes it.
	let attempt = 0;
	$effect(() => {
		const live = query();
		const error = live.error;
		if (!error) {
			attempt = 0;
			return;
		}
		if (!TRANSIENT.has(toAppError(error).code ?? '')) return;
		const delay = Math.min(1000 * 2 ** attempt, MAX_DELAY_MS);
		attempt += 1;
		// A reconnect that fails again sets a new error, which runs this again.
		const timer = setTimeout(() => void live.reconnect().catch(() => {}), delay);
		return () => clearTimeout(timer);
	});
}
