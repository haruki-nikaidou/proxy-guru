<script lang="ts">
import BoxIcon from '@lucide/svelte/icons/box';
import BoundaryError from '#lib/components/BoundaryError.svelte';
import * as Empty from '#lib/components/ui/empty/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import * as Table from '#lib/components/ui/table/index.js';
import type { HealthWindowMinutes } from '#lib/dto/health.js';
import { m } from '#lib/paraglide/messages.js';
import { getCanvasGraph } from '../graph.remote.js';
import PodEventRows from './PodEventRows.svelte';

let {
	canvasId,
	serverId,
	windowMinutes
}: { canvasId: string; serverId: string; windowMinutes: HealthWindowMinutes } = $props();

// Mounted only once a server's feed is opened, so neither the graph nor the
// per-pod histories are fetched on first paint. Every pod running on the server
// is listed, whichever canvas of the tree it is drawn on.
const graph = $derived(getCanvasGraph({ canvasId }));
const pods = $derived(
	(graph.current?.pods ?? [])
		.filter(pod => pod.serverId === serverId)
		.sort((a, b) => a.name.localeCompare(b.name))
);
</script>

{#if graph.current === undefined && graph.error}
	<BoundaryError error={graph.error} retry variant="inline" />
{:else if graph.current === undefined}
	<Skeleton class="h-32 w-full" />
{:else if pods.length === 0}
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
			{#each pods as pod (pod.id)}
				<PodEventRows podId={pod.id} label={pod.name} {windowMinutes} />
			{/each}
		</Table.Body>
	</Table.Root>
{/if}
