<script lang="ts">
import ListOrderedIcon from '@lucide/svelte/icons/list-ordered';
import SplitIcon from '@lucide/svelte/icons/split';
import type { NodeProps } from '@xyflow/svelte';
import { useCanvasContext } from '#lib/components/canvas/context.svelte.js';
import type { FlowNodeData } from '#lib/components/canvas/flow/nodes.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { m } from '#lib/paraglide/messages.js';
import CardHandle from './CardHandle.svelte';
import CardShell from './CardShell.svelte';
import RuleDots from './RuleDots.svelte';

// A group of route nodes that choose the same way between the same cards: a
// balance by weight, or a failover in tiers. One way out per member; the faint
// handle at the bottom adds a member to every route the card stands for.
let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'splitter' }> } = $props();

const canvas = useCanvasContext();
const faded = $derived(
	canvas.highlightedRule !== null && !data.rules.includes(canvas.highlightedRule)
);
</script>

<CardShell
	{id}
	title={data.policy === 'failover' ? m.editor_policy_failover() : m.editor_policy_balance()}
	kindLabel={m.editor_kind_splitter()}
	problem={data.problem}
	background="bg-canvas-load-balance"
	width="w-[220px]"
	{faded}
>
	{#snippet handles()}
		<CardHandle id="in" side="left" role="end" />
	{/snippet}
	{#snippet icon()}
		{#if data.policy === 'failover'}
			<ListOrderedIcon class="size-4 shrink-0" />
		{:else}
			<SplitIcon class="size-4 shrink-0" />
		{/if}
	{/snippet}
	{#snippet badge()}
		{#if data.sticky}
			<Badge variant="outline" class="h-4 shrink-0 px-1 text-[10px]">{m.editor_sticky_short()}</Badge>
		{/if}
	{/snippet}
	<p class="flex items-center gap-2 px-3 pt-1 text-xs text-muted-foreground">
		<RuleDots rules={data.rules} />
		<span class="truncate">{m.editor_splitter_stands({ count: data.stands })}</span>
	</p>
	<div class="mt-1 border-t pt-1">
		{#each data.ways as way, index (way.handle)}
			<div class="relative flex items-center gap-2 px-3 py-0.5 text-xs">
				<span class="w-8 shrink-0 font-mono text-muted-foreground">
					{way.weight === null ? `#${index + 1}` : `×${way.weight}`}
				</span>
				<span class="min-w-0 truncate">{way.label}</span>
				<CardHandle id={way.handle} side="right" role="bus" />
			</div>
		{/each}
		<div class="relative flex items-center justify-end px-3 py-0.5 text-xs text-muted-foreground/70">
			{m.editor_splitter_add_member()}
			<CardHandle id="add" side="right" role="start" />
		</div>
	</div>
</CardShell>
