<script lang="ts">
import BoxIcon from '@lucide/svelte/icons/box';
import BoundaryError from '#lib/components/BoundaryError.svelte';
import * as Empty from '#lib/components/ui/empty/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import * as Table from '#lib/components/ui/table/index.js';
import type { HealthWindowMinutes } from '#lib/dto/health.js';
import { m } from '#lib/paraglide/messages.js';
import { watchPodEvents } from './health.remote.js';
import PodEventRows from './PodEventRows.svelte';

let {
	canvasId,
	serverId,
	windowMinutes
}: { canvasId: string; serverId: string; windowMinutes: HealthWindowMinutes } = $props();

// Mounted only once a server's feed is opened, so no pod stream is held on
// first paint. Every pod running on the server is listed, whichever canvas of
// the tree it is drawn on.
const feeds = $derived(watchPodEvents({ canvasId, serverId, windowMinutes }));
</script>

{#if feeds.current === undefined && feeds.error}
	<BoundaryError error={feeds.error} retry variant="inline" />
{:else if feeds.current === undefined}
	<Skeleton class="h-32 w-full" />
{:else if feeds.current.length === 0}
	<Empty.Root>
		<Empty.Header>
			<Empty.Media variant="icon"><BoxIcon /></Empty.Media>
			<Empty.Title>{m.health_no_pods_title()}</Empty.Title>
			<Empty.Description>{m.health_no_pods_description()}</Empty.Description>
		</Empty.Header>
	</Empty.Root>
{:else}
	<Table.Root>
		<Table.Header>
			<Table.Row>
				<Table.Head>{m.health_events_pod()}</Table.Head>
				<Table.Head>{m.health_events_status()}</Table.Head>
				<Table.Head>{m.health_events_time()}</Table.Head>
				<Table.Head>{m.health_events_message()}</Table.Head>
			</Table.Row>
		</Table.Header>
		<Table.Body>
			{#each feeds.current as feed (feed.podId)}
				<PodEventRows label={feed.podName} points={feed.points} />
			{/each}
		</Table.Body>
	</Table.Root>
{/if}
