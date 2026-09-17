<script lang="ts">
import { Badge } from '#lib/components/ui/badge/index.js';
import { m } from '#lib/paraglide/messages.js';
import type { RemoteLiveQuery } from '$app/server';

/**
 * Whether a live query's stream to this dashboard is up. A terminal error is
 * shown by the page's own error view, so the badge hides itself then.
 */
let { query }: { query: Pick<RemoteLiveQuery<unknown>, 'connected' | 'error'> } = $props();
</script>

{#if !query.error}
	<Badge variant="outline" class="gap-1.5 font-normal">
		<span
			class="size-2 rounded-full {query.connected ? 'bg-emerald-500' : 'animate-pulse bg-amber-500'}"
		></span>
		{query.connected ? m.live_connected() : m.live_reconnecting()}
	</Badge>
{/if}
