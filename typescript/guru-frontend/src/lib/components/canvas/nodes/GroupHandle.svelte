<script lang="ts">
import { Handle, Position } from '@xyflow/svelte';
import { groupHandleId } from '#lib/components/canvas/graph.js';
import type { UniversalGroupName } from '#lib/dto/topology.js';

/**
 * The "add" handle of a bundle-capable node's group: the one place a new
 * channel or bundle is drawn from (or to). Every connection made here becomes
 * a port of its own, rendered as an ordinary handle above, so this handle
 * itself is never wired and always takes another drag. Bundles are the thick
 * square handles; the channel handle spawns coloured edges.
 */
let {
	flowId,
	group,
	label,
	side
}: {
	flowId: string;
	group: UniversalGroupName;
	label: string;
	side: 'left' | 'right';
} = $props();

const id = $derived(groupHandleId(flowId, group));
const source = $derived(group !== 'bundle_in');
const bundle = $derived(group !== 'channel_out');
</script>

<div
	class="relative flex items-center gap-1 px-3 py-1 text-xs"
	class:justify-end={side === 'right'}
>
	<Handle
		{id}
		type={source ? 'source' : 'target'}
		position={side === 'left' ? Position.Left : Position.Right}
		style={bundle
			? 'background: var(--canvas-bundle); border-color: var(--canvas-bundle); width: 14px; height: 14px; border-radius: 3px; opacity: 0.55;'
			: 'background: var(--canvas-port-destination); border-color: var(--canvas-port-destination); width: 10px; height: 10px; opacity: 0.55;'}
	/>
	<span class="font-mono text-muted-foreground/70">{label}</span>
</div>
