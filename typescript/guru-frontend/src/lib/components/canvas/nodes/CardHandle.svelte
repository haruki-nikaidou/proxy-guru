<script lang="ts">
import { Handle, Position } from '@xyflow/svelte';

/**
 * The one place a handle is created. Its side is its direction: a line leaves a
 * card at a right-hand (source) handle, drawn blue, and lands at a left-hand
 * (target) one, drawn red — and a drag goes the same way, from blue to red.
 * `role` says what a drag may do with it: `start` begins a new connection, `end`
 * accepts one, `bus` only anchors lines. Only the start/end flags are set —
 * `isConnectable` would override the flow's `nodesConnectable`, re-enabling
 * connections on a read-only canvas.
 */
let { id, side, role }: { id: string; side: 'left' | 'right'; role: 'start' | 'end' | 'bus' } =
	$props();

const color = $derived(side === 'left' ? 'var(--canvas-handle-in)' : 'var(--canvas-handle-out)');
// The ring keeps a dot visible on a row that is highlighted in a similar colour.
const size = $derived(role === 'bus' ? 8 : 11);
</script>

<Handle
	{id}
	type={side === 'left' ? 'target' : 'source'}
	position={side === 'left' ? Position.Left : Position.Right}
	isConnectableStart={role === 'start'}
	isConnectableEnd={role === 'end'}
	style="background:{color}; border:1.5px solid var(--background); width:{size}px; height:{size}px;"
/>
