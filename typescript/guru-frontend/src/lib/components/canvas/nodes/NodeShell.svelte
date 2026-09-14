<script lang="ts">
import type { Snippet } from 'svelte';
import { useFocusedNode } from '#lib/components/canvas/focus.svelte.js';
import type { ProblemLevel } from '#lib/components/canvas/graph.js';

// The card every node kind renders inside: header, optional comment, port rows.
let {
	id,
	title,
	kindLabel,
	comment,
	problem,
	background,
	width = 'w-[260px]',
	icon,
	badge,
	children
}: {
	/** Flow node id, matched against the panel's target to draw the focus ring. */
	id: string;
	title: string;
	kindLabel: string;
	comment: string;
	problem: ProblemLevel;
	/** A `bg-canvas-*` utility backed by the theme tokens. */
	background: string;
	width?: string;
	icon: Snippet;
	/** Rendered right after the title: a status the operator reads at a glance. */
	badge?: Snippet;
	children: Snippet;
} = $props();

const focused = useFocusedNode();

// Being the panel's edit target outranks the problem ring: the operator needs to
// see which card the form on the right belongs to, warning or not.
const outline = $derived(
	focused.current === id
		? 'border-ring ring-3 ring-ring/30'
		: problem === 'error'
			? 'ring-2 ring-destructive'
			: problem === 'warning'
				? 'ring-2 ring-amber-500'
				: ''
);
</script>

<div
	class="{width} {background} {outline} rounded-lg border py-2 text-foreground shadow-sm transition-[color,box-shadow]"
>
	<div class="flex items-center gap-2 px-3">
		{@render icon()}
		<span class="truncate text-sm font-medium">{title}</span>
		{#if badge}{@render badge()}{/if}
		<span class="ms-auto text-[10px] uppercase text-muted-foreground">{kindLabel}</span>
	</div>
	{#if comment}
		<p class="truncate px-3 pt-1 text-xs text-muted-foreground">{comment}</p>
	{/if}
	{@render children()}
</div>
