/**
 * Which TCP port a new pod should listen on. Nothing to do with the flow
 * mirror's ports — those are handles an edge attaches to; this is a number the
 * worker binds. It lives here because the panel that offers it is a canvas
 * panel, and because the range has to agree with the control plane's.
 */

/** Where suggested pod ports come from: the same high range the control plane
 * draws generated landing pods from. */
export const POD_PORT_RANGE: readonly [number, number] = [40000, 59999];

/**
 * A random port in [`POD_PORT_RANGE`] that no pod in `used` holds. Collisions
 * with anything else on the host surface as an apply error on the pod, which is
 * what the re-roll button is for.
 */
export function randomFreePort(used: Iterable<number>): number {
	const taken = new Set(used);
	const [low, high] = POD_PORT_RANGE;
	const span = high - low + 1;
	const buffer = new Uint32Array(1);
	for (let attempt = 0; attempt < 64; attempt += 1) {
		crypto.getRandomValues(buffer);
		const port = low + ((buffer[0] ?? 0) % span);
		if (!taken.has(port)) return port;
	}
	return low;
}
