import { untrack } from 'svelte';

/**
 * Fill a panel's local state from the thing it is editing, once per thing.
 *
 * A panel's inputs are not a view of the node: the operator types into them and
 * then saves. So they are filled when the panel first shows a subject and left
 * alone afterwards — an effect that simply copied the stored values across would
 * write them back over every keystroke.
 *
 * `key` names the subject; `seed` fills the state. `seed` runs untracked, so
 * nothing it reads becomes a reason to run it again, and nothing it writes has
 * to be guarded against.
 *
 * A `null` key means there is nothing to seed *and* forgets what was seeded, so
 * a dialog that closes and reopens on the same subject seeds it again:
 *
 * ```ts
 * seedOn(() => (open && provider ? provider.id : null), () => { ... });
 * ```
 */
export function seedOn(key: () => string | null, seed: () => void): void {
	// Deliberately not `$state`: nothing reads this but the effect that writes
	// it, and making it reactive would only schedule a second run for the guard
	// below to return from.
	let seeded: string | null = null;
	$effect(() => {
		const next = key();
		if (next === null) {
			seeded = null;
			return;
		}
		if (seeded === next) return;
		seeded = next;
		untrack(seed);
	});
}
