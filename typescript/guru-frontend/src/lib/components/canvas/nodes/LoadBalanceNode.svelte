<script lang="ts">
import MergeIcon from '@lucide/svelte/icons/merge';
import SplitIcon from '@lucide/svelte/icons/split';
import type { NodeProps } from '@xyflow/svelte';
import { channelColor, portLabel, type FlowNodeData } from '#lib/components/canvas/graph.js';
import { m } from '#lib/paraglide/messages.js';
import GroupHandle from './GroupHandle.svelte';
import NodeShell from './NodeShell.svelte';
import PortHandle from './PortHandle.svelte';

// Both load-balance variants render here: the backend refuses to change a node's
// spec kind, so the variant is fixed at creation and only ever displayed.
//
// Next to its hand-drawn ports a node adapts to bundles: a distribute node
// takes entry pods as coloured channels on the left and bundles them out on the
// right; an aggregate node takes bundles in on the left and grows one coloured
// input per channel on the right, where an exit is connected.
let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'load_balance' }> } = $props();

const distribute = $derived(data.node.mode === 'distribute');
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
const portOf = (portId: string | undefined) => data.node.ports.find(port => port.id === portId);
</script>

<NodeShell
	{id}
	title={data.node.name}
	kindLabel={distribute ? m.editor_kind_lb_distribute() : m.editor_kind_lb_aggregate()}
	comment={data.node.comment}
	problem={data.problem}
	background="bg-canvas-load-balance"
>
	{#snippet icon()}
		{#if distribute}
			<SplitIcon class="size-4 shrink-0" />
		{:else}
			<MergeIcon class="size-4 shrink-0" />
		{/if}
	{/snippet}
	<p class="px-3 pt-1 text-xs text-muted-foreground">
		{distribute ? `${balanceMode} · ${protocol} · ` : ''}{m.editor_member_count()}: {data.node.memberCount}
	</p>
	<div class="mt-1">
		{#each data.node.manualPorts as port (port.id)}
			<PortHandle {port} label={portLabel(port)} />
		{/each}
	</div>
	{#if distribute}
		<div class="flex items-start justify-between border-t pt-1">
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
				count={data.node.bundleCount}
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
	{:else}
		<div class="border-t pt-1">
			<GroupHandle
				flowId={id}
				group="bundle_in"
				label={m.editor_universal_bundle_in()}
				side="left"
				count={data.node.bundleCount}
			/>
			{#each data.node.channels as channel (channel.podId)}
				{@const port = portOf(channel.portId)}
				{#if port}
					<PortHandle {port} label={channel.podName} color={channelColor(channel)} />
				{/if}
			{/each}
		</div>
	{/if}
</NodeShell>
