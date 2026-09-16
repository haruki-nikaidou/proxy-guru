<script lang="ts">
import ExternalLinkIcon from '@lucide/svelte/icons/external-link';
import type { NodeProps } from '@xyflow/svelte';
import type { FlowNodeData } from '#lib/components/canvas/flow/nodes.js';
import { m } from '#lib/paraglide/messages.js';
import CardHandle from './CardHandle.svelte';
import CardShell from './CardShell.svelte';

// Another canvas of the tree, outside this one, that edges of this canvas lead
// into or come from: what of it they touch is listed.
let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'portal' }> } = $props();
</script>

<CardShell
	{id}
	title={data.canvas?.name ?? m.editor_portal_unknown()}
	kindLabel={m.editor_kind_portal()}
	problem={data.problem}
	background="bg-canvas-export"
	width="w-[220px]"
>
	{#snippet handles()}
		<CardHandle id="in" side="left" role="end" />
		<CardHandle id="out" side="right" role="bus" />
	{/snippet}
	{#snippet icon()}<ExternalLinkIcon class="size-4 shrink-0" />{/snippet}
	<ul class="px-3 pt-1 text-xs text-muted-foreground">
		{#each data.pods as pod (pod.id)}
			<li class="truncate">{pod.name}</li>
		{/each}
		{#each data.exits as exit (exit.id)}
			<li class="truncate">→ {exit.name}</li>
		{/each}
	</ul>
</CardShell>
