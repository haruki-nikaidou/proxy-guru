<script lang="ts">
import type { Snippet } from 'svelte';
import { useCanvasContext } from '#lib/components/canvas/context.svelte.js';
import type { ProblemLevel } from '#lib/components/canvas/flow/nodes.js';

// The frame every card renders inside: a header, then whatever the card lists.
let {
	id,
	title,
	kindLabel,
	comment = '',
	problem,
	background,
	width = 'w-[260px]',
	faded = false,
	icon,
	badge,
	handles,
	children
}: {
	/** Flow node id, matched against the panel's target to draw the focus ring. */
	id: string;
	title: string;
	kindLabel: string;
	comment?: string;
	problem: ProblemLevel;
	/** A `bg-canvas-*` utility backed by the theme tokens. */
	background: string;
	width?: string;
	/** Dimmed while the legend singles out a rule this card does not carry. */
	faded?: boolean;
	icon: Snippet;
	/** Rendered right after the title: a status the operator reads at a glance. */
	badge?: Snippet;
	/** The header's own handles, anchored to the header row. */
	handles?: Snippet;
	children?: Snippet;
} = $props();

const canvas = useCanvasContext();

// Being the panel's edit target outranks the problem ring: the operator needs to
// see which card the form on the right belongs to, warning or not.
const outline = $derived(
	canvas.focusedCard === id
		? 'border-ring ring-3 ring-ring/30'
		: problem === 'error'
			? 'ring-2 ring-destructive'
			: problem === 'warning'
				? 'ring-2 ring-amber-500'
				: ''
);
</script>

<div
	class="{width} {background} {outline} rounded-lg border py-2 text-foreground shadow-sm transition-[color,box-shadow,opacity]"
	class:opacity-35={faded}
>
	<div class="relative flex items-center gap-2 px-3">
		{@render handles?.()}
		{@render icon()}
		<span class="truncate text-sm font-medium">{title}</span>
		{#if badge}{@render badge()}{/if}
		<span class="ms-auto shrink-0 text-[10px] uppercase text-muted-foreground">{kindLabel}</span>
	</div>
	{#if comment}
		<p class="truncate px-3 pt-1 text-xs text-muted-foreground">{comment}</p>
	{/if}
	{@render children?.()}
</div>
