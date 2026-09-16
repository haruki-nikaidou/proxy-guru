<script lang="ts">
import LogOutIcon from '@lucide/svelte/icons/log-out';
import type { NodeProps } from '@xyflow/svelte';
import { useCanvasContext } from '#lib/components/canvas/context.svelte.js';
import type { FlowNodeData } from '#lib/components/canvas/flow/nodes.js';
import { proxyLabel } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import CardHandle from './CardHandle.svelte';
import CardShell from './CardShell.svelte';
import RuleDots from './RuleDots.svelte';

// Where traffic leaves the fabric. Any number of pods may lead here.
let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'exit' }> } = $props();

const canvas = useCanvasContext();
const faded = $derived(
	canvas.highlightedRule !== null && !data.rules.includes(canvas.highlightedRule)
);
</script>

<CardShell
	{id}
	title={data.exit.name}
	kindLabel={m.editor_kind_exit()}
	comment={data.exit.comment}
	problem={data.problem}
	background="bg-canvas-exit"
	{faded}
>
	{#snippet handles()}
		<CardHandle id="in" side="left" role="end" />
	{/snippet}
	{#snippet icon()}<LogOutIcon class="size-4 shrink-0" />{/snippet}
	<p
		class="truncate px-3 pt-1 font-mono text-xs"
		class:text-muted-foreground={!data.exit.destination}
	>
		{data.exit.destination || m.editor_destination_empty()}
	</p>
	<p class="flex items-center gap-2 px-3 pt-1 text-xs text-muted-foreground">
		<RuleDots rules={data.rules} />
		<span>{m.editor_exit_dialed_by({ count: data.dialedBy })}</span>
		{#if data.exit.sendProxyProtocol}
			<span class="ms-auto">{proxyLabel(data.exit.sendProxyProtocol)}</span>
		{/if}
	</p>
</CardShell>
