<script lang="ts">
import MergeIcon from '@lucide/svelte/icons/merge';
import type { NodeProps } from '@xyflow/svelte';
import { useCanvasContext } from '#lib/components/canvas/context.svelte.js';
import type { FlowNodeData } from '#lib/components/canvas/flow/nodes.js';
import { m } from '#lib/paraglide/messages.js';
import CardHandle from './CardHandle.svelte';
import CardShell from './CardShell.svelte';
import RuleDots from './RuleDots.svelte';

// Where buses from several servers meet before a splitter or an exit. Nothing
// about it is stored: it is how the drawing keeps many parallel lines readable.
// A drag from one of its ways out gives every pod on that way a way on.
let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'aggregator' }> } = $props();

const canvas = useCanvasContext();
const faded = $derived(
	canvas.highlightedRule !== null && !data.rules.includes(canvas.highlightedRule)
);
</script>

<CardShell
	{id}
	title={m.editor_aggregator_sources({ count: data.sources })}
	kindLabel={m.editor_kind_aggregator()}
	problem={data.problem}
	background="bg-canvas-universal"
	width="w-[200px]"
	{faded}
>
	{#snippet handles()}
		<CardHandle id="in" side="left" role="bus" />
	{/snippet}
	{#snippet icon()}<MergeIcon class="size-4 shrink-0" />{/snippet}
	<p class="px-3 pt-1"><RuleDots rules={data.rules} /></p>
	<div class="mt-1 border-t pt-1">
		{#each data.ways as way (way.handle)}
			<div class="relative flex items-center px-3 py-0.5 text-xs">
				<span class="min-w-0 truncate">{way.label}</span>
				<CardHandle id={way.handle} side="right" role="start" />
			</div>
		{/each}
	</div>
</CardShell>
