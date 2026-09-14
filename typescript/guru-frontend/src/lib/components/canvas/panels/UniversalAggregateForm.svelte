<script lang="ts">
import { channelColor } from '#lib/components/canvas/graph.js';
import type { CanvasGraph, UniversalAggregateNodeDto } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * An aggregator has nothing to configure: it lists the channels its bundles
 * carry and where each one exits, which is the edge on its `chan:` port.
 */
let { node, graph }: { node: UniversalAggregateNodeDto; graph: CanvasGraph | undefined } = $props();

/** Exit node name by the port id an edge into this aggregator starts at. */
const exitOf = (portId: string): string | null => {
	if (!graph) return null;
	const edge = graph.edges.find(entry => entry.targetPortId === portId);
	if (!edge) return null;
	const owner = graph.nodes.find(entry => entry.ports.some(port => port.id === edge.sourcePortId));
	return owner?.name ?? null;
};
</script>

<p class="text-sm text-muted-foreground">{m.editor_universal_aggregate_hint()}</p>

<h3 class="mt-6 text-sm font-medium">{m.editor_universal_channels()}</h3>
{#if node.channels.length === 0}
	<p class="mt-2 text-sm text-muted-foreground">{m.editor_universal_no_channels()}</p>
{:else}
	<ul class="mt-2 grid gap-1">
		{#each node.channels as channel (channel.podId)}
			{@const exit = exitOf(channel.portId)}
			<li class="flex items-center gap-2 text-sm">
				<span class="size-3 shrink-0 rounded-full" style="background: {channelColor(channel)}"></span>
				<span class="truncate">{channel.podName}</span>
				<span class="ms-auto truncate text-xs text-muted-foreground">
					{exit ?? m.editor_universal_no_exit()}
				</span>
			</li>
		{/each}
	</ul>
{/if}
