<script lang="ts">
import { Handle, Position, useNodeConnections } from '@xyflow/svelte';
import type { CanvasPort } from '#lib/dto/topology.js';

// The one place a one-edge Svelte Flow handle is created. Output ports are
// handles on the left, input ports on the right — unless `side` says otherwise
// (a bundle flows left to right whatever its port direction) — and the dot takes
// the port *kind*'s colour, or the channel's, when the port belongs to one.
let {
	port,
	label,
	color,
	side
}: { port: CanvasPort; label: string; color?: string; side?: 'left' | 'right' } = $props();

const colour = $derived(
	color ??
		(port.kind === 'derive_listen'
			? 'var(--canvas-port-listen)'
			: port.kind === 'bundle'
				? 'var(--canvas-bundle)'
				: 'var(--canvas-port-destination)')
);
// A bundle port is a square, like the bundle edge is thick: not a traffic port.
const shape = $derived(
	port.kind === 'bundle'
		? 'width:14px; height:14px; border-radius:3px;'
		: 'width:10px; height:10px;'
);
const output = $derived(port.direction === 'output');
const left = $derived(side ? side === 'left' : output);

// A port carries at most one edge, so once it is wired the handle stops being a
// connection endpoint: no drag start, no drop target, no click-connect. Only the
// start/end flags are set — `isConnectable` would override the flow's
// `nodesConnectable`, re-enabling connections on a read-only canvas.
const connections = useNodeConnections();
const connected = $derived(
	connections.current.some(
		connection => (output ? connection.sourceHandle : connection.targetHandle) === port.id
	)
);
</script>

<!-- `relative` anchors the handle to this row, giving one handle per port line. -->
<div class="relative flex items-center px-3 py-1 text-xs" class:justify-end={!left}>
	<Handle
		id={port.id}
		type={output ? 'source' : 'target'}
		position={left ? Position.Left : Position.Right}
		isConnectableStart={!connected}
		isConnectableEnd={!connected}
		style="background:{colour}; border-color:{colour}; {shape}"
	/>
	<span class="font-mono text-muted-foreground">{label}</span>
</div>
