<script lang="ts">
import ShareIcon from '@lucide/svelte/icons/share-2';
import type { NodeProps } from '@xyflow/svelte';
import { portLabel, type FlowNodeData } from '#lib/components/canvas/graph.js';
import { m } from '#lib/paraglide/messages.js';
import { exportAsLabel, exportKindLabel } from '#lib/i18n/labels.js';
import NodeShell from './NodeShell.svelte';
import PortHandle from './PortHandle.svelte';

// One boundary port of this canvas. The parent sees it as a port on the import
// node that embeds this canvas, keyed by this node's id and named after it.
let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'canvas_export' }> } = $props();

const kindLabel = $derived(exportKindLabel(data.node.portKind));
const directionLabel = $derived(exportAsLabel(data.node.exportAs));
</script>

<NodeShell
	{id}
	title={data.node.name}
	kindLabel={m.editor_kind_export()}
	comment={data.node.comment}
	problem={data.problem}
	background="bg-canvas-export"
>
	{#snippet icon()}<ShareIcon class="size-4 shrink-0" />{/snippet}
	<p class="truncate px-3 pt-1 text-xs text-muted-foreground">
		{kindLabel} · {directionLabel}
	</p>
	<div class="mt-1">
		{#each data.node.ports as port (port.id)}
			<PortHandle {port} label={portLabel(port)} />
		{/each}
	</div>
</NodeShell>
