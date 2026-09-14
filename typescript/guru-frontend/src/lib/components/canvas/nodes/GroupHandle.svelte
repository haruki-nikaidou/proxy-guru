<script lang="ts">
import { Handle, Position, useNodeConnections } from '@xyflow/svelte';
import { groupHandleId } from '#lib/components/canvas/graph.js';
import type { UniversalGroupName } from '#lib/dto/topology.js';

/**
 * One handle standing for a universal node's whole port group: the control
 * plane creates a port per edge behind it, so the handle takes any number of
 * edges (a group of one aside) and shows how many it carries. Bundles are the
 * thick handles; a channel group is the one that spawns coloured edges.
 */
let {
	flowId,
	group,
	label,
	side,
	single = false,
	count = 0
}: {
	flowId: string;
	group: UniversalGroupName;
	label: string;
	side: 'left' | 'right';
	/** A group of one: once wired it stops accepting connections. */
	single?: boolean;
	/** Shown next to the label; the number of edges on the group. */
	count?: number;
} = $props();

const id = $derived(groupHandleId(flowId, group));
const source = $derived(group !== 'bundle_in');
const bundle = $derived(group !== 'channel_out');
const connections = useNodeConnections();
const wired = $derived(
	connections.current.some(
		connection => (source ? connection.sourceHandle : connection.targetHandle) === id
	)
);
const connectable = $derived(!(single && wired));
</script>

<div
	class="relative flex items-center gap-1 px-3 py-1 text-xs"
	class:justify-end={side === 'right'}
>
	<Handle
		{id}
		type={source ? 'source' : 'target'}
		position={side === 'left' ? Position.Left : Position.Right}
		isConnectableStart={connectable}
		isConnectableEnd={connectable}
		style={bundle
			? 'background: var(--canvas-bundle); border-color: var(--canvas-bundle); width: 14px; height: 14px; border-radius: 3px;'
			: 'background: var(--canvas-port-destination); border-color: var(--canvas-port-destination); width: 12px; height: 12px;'}
	/>
	<span class="font-mono text-muted-foreground">{label}</span>
	{#if count > 0}
		<span class="rounded bg-muted px-1 font-mono text-[10px] text-muted-foreground">{count}</span>
	{/if}
</div>
