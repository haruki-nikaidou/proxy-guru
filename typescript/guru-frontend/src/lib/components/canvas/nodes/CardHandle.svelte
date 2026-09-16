<script lang="ts">
import { Handle, Position } from '@xyflow/svelte';

/**
 * The one place a handle is created. `role` says what a drag may do with it:
 * `start` begins a new connection, `end` accepts one, `bus` only anchors the
 * buses drawn between cards. Only the start/end flags are set — `isConnectable`
 * would override the flow's `nodesConnectable`, re-enabling connections on a
 * read-only canvas.
 */
let {
	id,
	side,
	role,
	color = 'var(--canvas-bundle)'
}: {
	id: string;
	side: 'left' | 'right';
	role: 'start' | 'end' | 'bus';
	color?: string;
} = $props();

const size = $derived(role === 'bus' ? 6 : 10);
</script>

<Handle
	{id}
	type={side === 'left' ? 'target' : 'source'}
	position={side === 'left' ? Position.Left : Position.Right}
	isConnectableStart={role === 'start'}
	isConnectableEnd={role === 'end'}
	style="background:{color}; border-color:{color}; width:{size}px; height:{size}px;"
/>
