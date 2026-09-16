<script lang="ts">
import { BaseEdge, EdgeLabel, type EdgeProps, getBezierPath } from '@xyflow/svelte';
import { useCanvasContext } from '#lib/components/canvas/context.svelte.js';
import type { BusEdgeData } from '#lib/components/canvas/flow/edges.js';

/**
 * A bus: one cable per way between two cards, with a thin line in each rule's
 * colour riding inside it. A bus standing for more than one edge says how many.
 */
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
}: EdgeProps & { data?: BusEdgeData } = $props();

const canvas = useCanvasContext();

/** Pixels between two rule lines. */
const GAP = 3;

const rules = $derived(data?.rules ?? []);
const center = $derived(
	getBezierPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition })
);
const lines = $derived(
	rules.map((rule, i) => {
		const offset = (i - (rules.length - 1) / 2) * GAP;
		const [path] = getBezierPath({
			sourceX,
			sourceY: sourceY + offset,
			targetX,
			targetY: targetY + offset,
			sourcePosition,
			targetPosition
		});
		return { rule, path };
	})
);
const width = $derived(Math.max(4, rules.length * GAP + 3));
const cable = $derived(
	data?.problem === 'error'
		? 'var(--destructive)'
		: data?.problem === 'warning'
			? 'oklch(0.77 0.16 70)'
			: 'var(--canvas-bundle)'
);
const faded = $derived(canvas.highlightedRule !== null && !rules.includes(canvas.highlightedRule));
</script>

<g style="opacity: {faded ? 0.15 : 1}">
	<BaseEdge
		{id}
		path={center[0]}
		interactionWidth={Math.max(20, width + 10)}
		style="stroke: {cable}; stroke-width: {width + (selected ? 3 : 0)}; stroke-linecap: round; opacity: {selected
			? 0.55
			: 0.3}"
	/>
	{#each lines as line (line.rule)}
		<path
			d={line.path}
			fill="none"
			style="stroke: {canvas.ruleColor(line.rule)}; stroke-width: 1.5; pointer-events: none"
		/>
	{/each}
</g>
{#if (data?.edges ?? 0) > 1}
	<!-- The label lives outside the edge element, so the pill opens the bus on
	     its own; the click also selects the edge. -->
	<EdgeLabel x={center[1]} y={center[2]} selectEdgeOnClick>
		<button
			type="button"
			class="rounded-full border bg-background px-1.5 py-0.5 font-mono text-[10px] text-foreground shadow-sm hover:bg-accent"
			style="opacity: {faded ? 0.3 : 1}"
			onclick={() => canvas.open({ kind: 'bus', id })}
		>
			{data?.edges}
		</button>
	</EdgeLabel>
{/if}
