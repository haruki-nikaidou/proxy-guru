<script lang="ts">
import LayersIcon from '@lucide/svelte/icons/layers';
import type { NodeProps } from '@xyflow/svelte';
import type { FlowNodeData } from '#lib/components/canvas/flow/nodes.js';
import { m } from '#lib/paraglide/messages.js';
import CardHandle from './CardHandle.svelte';
import CardShell from './CardShell.svelte';

// A canvas drawn inside this one. Edges cross into it freely: a drop on the card
// asks which pod or exit inside is meant. Double-click goes inside.
let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'canvas' }> } = $props();
</script>

<CardShell
	{id}
	title={data.canvas.name}
	kindLabel={m.editor_kind_subcanvas()}
	comment={data.canvas.description}
	problem={data.problem}
	background="bg-canvas-import"
>
	{#snippet handles()}
		<CardHandle id="in" side="left" role="end" />
		<CardHandle id="out" side="right" role="bus" />
	{/snippet}
	{#snippet icon()}<LayersIcon class="size-4 shrink-0" />{/snippet}
	<p class="px-3 pt-1 text-xs text-muted-foreground">
		{m.canvas_servers_count({ count: data.servers })} · {m.canvas_pods_count({ count: data.pods })}
	</p>
</CardShell>
