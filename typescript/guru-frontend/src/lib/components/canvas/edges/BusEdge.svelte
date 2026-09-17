<script lang="ts">
import {
	BaseEdge,
	EdgeLabel,
	type EdgeProps,
	getBezierPath,
	Position,
	useNodes
} from '@xyflow/svelte';
import { useCanvasContext } from '#lib/components/canvas/context.svelte.js';
import type { BusEdgeData } from '#lib/components/canvas/flow/edges.js';

/**
 * A bus: one cable per way between two handles, with a thin line in each rule's
 * colour riding inside it. A bus standing for more than one edge says how many —
 * a plain count, since `×n` is how a splitter writes a weight.
 *
 * A bus between two rows of one card (a pod dialing a pod of its own server)
 * would run behind the card from its right side back to its left: it loops round
 * underneath instead.
 */
let {
	id,
	source,
	target,
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

/** How far a loop keeps from the card it goes round, and how round its corners are. */
const LOOP_CLEARANCE = 28;
const LOOP_RADIUS = 10;

const rules = $derived(data?.rules ?? []);
const loop = $derived(source === target);
const nodes = useNodes();
// Only a loop reads the nodes, so no other bus redraws while cards move.
const cardBottom = $derived.by(() => {
	if (!loop) return 0;
	const card = nodes.current.find(node => node.id === source);
	return card ? card.position.y + (card.measured?.height ?? 0) : Math.max(sourceY, targetY);
});

/** Out of the right-hand handle, down past the card, back in at the left. */
function loopPath(offset: number): string {
	const r = LOOP_RADIUS;
	const right = Math.max(sourceX, targetX) + LOOP_CLEARANCE + offset;
	const left = Math.min(sourceX, targetX) - LOOP_CLEARANCE - offset;
	const below = cardBottom + LOOP_CLEARANCE + offset;
	const from = sourceY + offset;
	const to = targetY + offset;
	return [
		`M ${sourceX} ${from} H ${right - r} Q ${right} ${from} ${right} ${from + r}`,
		`V ${below - r} Q ${right} ${below} ${right - r} ${below}`,
		`H ${left + r} Q ${left} ${below} ${left} ${below - r}`,
		`V ${to + r} Q ${left} ${to} ${left + r} ${to} H ${targetX}`
	].join(' ');
}

const pathAt = (offset: number): string =>
	loop
		? loopPath(offset)
		: getBezierPath({
				sourceX,
				sourceY: sourceY + offset,
				targetX,
				targetY: targetY + offset,
				sourcePosition,
				targetPosition
			})[0];
const center = $derived(pathAt(0));
const lines = $derived(
	rules.map((rule, i) => ({ rule, path: pathAt((i - (rules.length - 1) / 2) * GAP) }))
);
/**
 * A point along the drawn curve: `getBezierPath`'s control points (curvature
 * 0.25), evaluated at `t`. Its own label point is always the middle.
 */
function pointAt(t: number): [number, number] {
	const offset = (distance: number) =>
		distance >= 0 ? 0.5 * distance : 0.25 * 25 * Math.sqrt(-distance);
	const control = (
		position: Position,
		x1: number,
		y1: number,
		x2: number,
		y2: number
	): [number, number] => {
		switch (position) {
			case Position.Left:
				return [x1 - offset(x1 - x2), y1];
			case Position.Right:
				return [x1 + offset(x2 - x1), y1];
			case Position.Top:
				return [x1, y1 - offset(y1 - y2)];
			default:
				return [x1, y1 + offset(y2 - y1)];
		}
	};
	const [c1x, c1y] = control(sourcePosition, sourceX, sourceY, targetX, targetY);
	const [c2x, c2y] = control(targetPosition, targetX, targetY, sourceX, sourceY);
	const u = 1 - t;
	return [
		u * u * u * sourceX + 3 * u * u * t * c1x + 3 * u * t * t * c2x + t * t * t * targetX,
		u * u * u * sourceY + 3 * u * u * t * c1y + 3 * u * t * t * c2y + t * t * t * targetY
	];
}
const label = $derived(
	loop
		? ([(sourceX + targetX) / 2, cardBottom + LOOP_CLEARANCE] as [number, number])
		: pointAt(data?.labelAt ?? 0.5)
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
		path={center}
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
	<EdgeLabel x={label[0]} y={label[1]} selectEdgeOnClick>
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
