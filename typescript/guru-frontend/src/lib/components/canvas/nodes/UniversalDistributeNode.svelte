<script lang="ts">
import WaypointsIcon from '@lucide/svelte/icons/waypoints';
import type { NodeProps } from '@xyflow/svelte';
import { channelColor, type FlowNodeData } from '#lib/components/canvas/graph.js';
import { m } from '#lib/paraglide/messages.js';
import GroupHandle from './GroupHandle.svelte';
import NodeShell from './NodeShell.svelte';

// One strategy and one relay protocol for every channel; the channels themselves
// are read off the graph, the ports behind them are the control plane's.
let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'universal_distribute' }> } =
	$props();

const balanceMode = $derived(
	data.node.balanceMode === 'random'
		? m.editor_balance_random()
		: data.node.balanceMode === 'ip_hash'
			? m.editor_balance_ip_hash()
			: data.node.balanceMode === 'fallback'
				? m.editor_balance_fallback()
				: m.editor_balance_round_robin()
);
const protocol = $derived(
	data.node.protocol === 'tcp_tls'
		? m.editor_relay_tcp_tls()
		: data.node.protocol === 'quic'
			? m.editor_relay_quic()
			: m.editor_relay_tcp_raw()
);
const bundles = $derived(data.node.ports.filter(port => port.kind === 'bundle').length);
</script>

<NodeShell
	{id}
	title={data.node.name}
	kindLabel={m.editor_kind_universal_distribute()}
	comment={data.node.comment}
	problem={data.problem}
	background="bg-canvas-universal"
>
	{#snippet icon()}<WaypointsIcon class="size-4 shrink-0" />{/snippet}
	<p class="px-3 pt-1 text-xs text-muted-foreground">{balanceMode} · {protocol}</p>
	<div class="mt-1 flex items-start justify-between">
		<GroupHandle
			flowId={id}
			group="channel_out"
			label={m.editor_universal_channels()}
			side="left"
			count={data.node.channels.length}
		/>
		<GroupHandle
			flowId={id}
			group="bundle_out"
			label={m.editor_universal_bundle_out()}
			side="right"
			count={bundles}
		/>
	</div>
	{#if data.node.channels.length > 0}
		<div class="flex flex-wrap gap-1 px-3 pb-1">
			{#each data.node.channels as channel (channel.podId)}
				<span
					class="inline-flex max-w-full items-center gap-1 rounded-full border px-1.5 text-[10px]"
					style="border-color: {channelColor(channel)}"
				>
					<span class="size-2 rounded-full" style="background: {channelColor(channel)}"></span>
					<span class="truncate">{channel.podName}</span>
				</span>
			{/each}
		</div>
	{/if}
</NodeShell>
