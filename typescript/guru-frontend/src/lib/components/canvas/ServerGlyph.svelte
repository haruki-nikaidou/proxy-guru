<script lang="ts">
import Icon, { iconLoaded, loadIcon } from '@iconify/svelte';
import ServerIcon from '@lucide/svelte/icons/server';
import { browser } from '$app/env';

/**
 * The server icon, rendered on demand through Iconify.
 *
 * Only two icon sets are offered, each behind a short prefix, so the field stays
 * a small vocabulary instead of the whole Iconify index:
 *
 * - `flag:us`     → `circle-flags:us`
 * - `logo:tauri`  → `thesvg-color:tauri`
 *
 * An empty field shows the flag of `country` — the country of the server's IPv4
 * address — so every server gets a flag without anyone typing one; whatever the
 * operator types overrides it. Anything that does not resolve — an unknown
 * prefix, a raw Iconify name, a name the set does not contain, an empty field
 * with no country — renders the default glyph, so a typo can never blank out a
 * node header. Both branches are 1.5rem tall, so the slot never resizes while
 * the operator types.
 */
const SETS: Record<string, string> = { flag: 'circle-flags', logo: 'thesvg-color' };

let {
	icon = '',
	country = '',
	class: className = ''
}: { icon?: string; country?: string; class?: string } = $props();

/** The icon to draw: the operator's, else the flag of a two-letter country. */
const chosen = $derived(
	icon.trim() || (/^[a-z]{2}$/i.test(country) ? `flag:${country.toLowerCase()}` : '')
);

/** The full Iconify name, or `null` when there is no icon or it is malformed. */
const resolved = $derived.by(() => {
	const match = /^([a-z]+):([a-z0-9-]+)$/.exec(chosen);
	const set = match ? SETS[match[1]] : undefined;
	return set ? `${set}:${match?.[2]}` : null;
});

/**
 * The name whose data Iconify actually holds. A missing icon renders as nothing,
 * so the default glyph stays up until the fetch confirms the name — and a fetch
 * that lands after the field moved on is dropped, both by the name check below
 * and by the teardown, so a stale load can never replace the current preview.
 *
 * Nothing is fetched during SSR: icon data only exists in the browser, and the
 * server must not make one request per rendered page.
 */
let available = $state<string | null>(null);

$effect(() => {
	const name = resolved;
	if (!name || !browser) return;
	if (iconLoaded(name)) {
		available = name;
		return;
	}
	let live = true;
	// A rejection means the set has no such icon: leave the fallback in place.
	loadIcon(name).then(
		() => {
			if (live) available = name;
		},
		() => {}
	);
	return () => {
		live = false;
	};
});

const shown = $derived(resolved && available === resolved ? resolved : null);
</script>

{#if shown}
	<Icon icon={shown} class="h-6 w-auto shrink-0 {className}" />
{:else}
	<ServerIcon class="size-6 shrink-0 {className}" />
{/if}
