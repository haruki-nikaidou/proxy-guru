<script lang="ts">
import { BaseEdge, EdgeLabel, getBezierPath, type EdgeProps } from '@xyflow/svelte';
import type { BundleEdgeData } from '#lib/components/canvas/graph.js';
import { useEdgeOpener } from './open.svelte.js';

/** A bundle: one thick line carrying every channel of its source, counted. */
let {
	id,
	sourceX,
	sourceY,
	targetX,
	targetY,
	sourcePosition,
	targetPosition,
	data,
	selected
}: EdgeProps & { data?: BundleEdgeData } = $props();

const opener = useEdgeOpener();

const path = $derived(
	getBezierPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition })
);
const count = $derived(data?.count ?? 0);
</script>

<BaseEdge
	{id}
	path={path[0]}
	style="stroke: var(--canvas-bundle); stroke-width: {selected ? 8 : 6}; stroke-linecap: round; opacity: 0.85"
/>
<!-- The label lives outside the edge element, so the pill selects the edge on
     its own and opens the panel through the opener; the click bubbles to the
     label's selection handler. -->
<EdgeLabel x={path[1]} y={path[2]} selectEdgeOnClick>
	<button
		type="button"
		class="rounded-full border bg-background px-1.5 py-0.5 font-mono text-[10px] text-foreground shadow-sm hover:bg-accent"
		onclick={() => opener.open(id)}
	>
		{count}
	</button>
</EdgeLabel>
