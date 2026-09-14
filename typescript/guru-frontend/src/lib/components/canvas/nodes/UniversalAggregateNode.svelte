<script lang="ts">
import CombineIcon from '@lucide/svelte/icons/combine';
import type { NodeProps } from '@xyflow/svelte';
import { channelColor, type FlowNodeData } from '#lib/components/canvas/graph.js';
import { m } from '#lib/paraglide/messages.js';
import GroupHandle from './GroupHandle.svelte';
import NodeShell from './NodeShell.svelte';
import PortHandle from './PortHandle.svelte';

// Bundles come in on the left; every channel they carry gets its own coloured
// input on the right, which is where an exit is connected.
let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'universal_aggregate' }> } =
	$props();

const bundles = $derived(data.node.ports.filter(port => port.kind === 'bundle').length);
const portOf = (portId: string) => data.node.ports.find(port => port.id === portId);
</script>

<NodeShell
	{id}
	title={data.node.name}
	kindLabel={m.editor_kind_universal_aggregate()}
	comment={data.node.comment}
	problem={data.problem}
	background="bg-canvas-universal"
>
	{#snippet icon()}<CombineIcon class="size-4 shrink-0" />{/snippet}
	<div class="mt-1">
		<GroupHandle
			flowId={id}
			group="bundle_in"
			label={m.editor_universal_bundle_in()}
			side="left"
			count={bundles}
		/>
	</div>
	{#if data.node.channels.length === 0}
		<p class="px-3 py-1 text-end text-xs text-muted-foreground">{m.editor_universal_no_channels()}</p>
	{:else}
		{#each data.node.channels as channel (channel.podId)}
			{@const port = portOf(channel.portId)}
			{#if port}
				<PortHandle {port} label={channel.podName} color={channelColor(channel)} />
			{/if}
		{/each}
	{/if}
</NodeShell>
