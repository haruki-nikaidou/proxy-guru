<script lang="ts">
import XIcon from '@lucide/svelte/icons/x';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import { type PanelTarget, refind } from '#lib/components/canvas/flow/nodes.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import { policyLabel } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import AggregatorForm from './AggregatorForm.svelte';
import BusForm from './BusForm.svelte';
import ExitForm from './ExitForm.svelte';
import PodForm from './PodForm.svelte';
import PortalForm from './PortalForm.svelte';
import ServerForm from './ServerForm.svelte';
import SplitterForm from './SplitterForm.svelte';
import SubcanvasForm from './SubcanvasForm.svelte';

/**
 * The side panel: whatever the operator clicked, resolved against the graph on
 * every render, so a refresh reflects straight into it. Something that is gone
 * closes its panel; a splitter, aggregator or bus an edit renamed is found again
 * by its anchor.
 */
let { target = $bindable(null) }: { target?: PanelTarget | null } = $props();

const editor = useEditor();
const graph = $derived(editor.graph);
const drawing = $derived(editor.drawing);

const server = $derived(
	target?.kind === 'server' ? graph.servers.find(entry => entry.id === target?.id) : undefined
);
const pod = $derived(
	target?.kind === 'pod' ? graph.pods.find(entry => entry.id === target?.id) : undefined
);
const exit = $derived(
	target?.kind === 'exit' ? graph.exits.find(entry => entry.id === target?.id) : undefined
);
const canvas = $derived(
	target?.kind === 'canvas' ? graph.canvases.find(entry => entry.id === target?.id) : undefined
);
const card = $derived(
	target && ['splitter', 'aggregator', 'portal'].includes(target.kind)
		? drawing.cards.find(entry => entry.id === target?.id)
		: undefined
);
const bus = $derived(
	target?.kind === 'bus' ? drawing.buses.find(entry => entry.id === target?.id) : undefined
);

$effect(() => {
	if (!target || server || pod || exit || canvas || card || bus) return;
	target = refind(drawing, target);
});

const title = $derived(
	server?.name ??
		pod?.name ??
		exit?.name ??
		canvas?.name ??
		(card?.kind === 'splitter'
			? policyLabel(card.policy)
			: card?.kind === 'portal'
				? (card.canvas?.name ?? '')
				: card?.kind === 'aggregator'
					? m.editor_kind_aggregator()
					: bus
						? m.editor_kind_bus()
						: '')
);
const kindLabel = $derived(
	server
		? m.editor_kind_server()
		: pod
			? m.editor_kind_pod()
			: exit
				? m.editor_kind_exit()
				: canvas
					? m.editor_kind_subcanvas()
					: card?.kind === 'splitter'
						? m.editor_kind_splitter()
						: card?.kind === 'aggregator'
							? m.editor_kind_aggregator()
							: card?.kind === 'portal'
								? m.editor_kind_portal()
								: m.editor_kind_bus()
);
</script>

<div class="flex h-full flex-col">
	<div class="flex items-center gap-2 border-b px-4 py-3">
		<div class="flex min-w-0 flex-1 flex-col gap-1">
			<h2 class="truncate text-sm font-semibold">{title}</h2>
			<Badge variant="secondary" class="w-fit">{kindLabel}</Badge>
		</div>
		<Button
			variant="ghost"
			size="icon-sm"
			aria-label={m.common_close()}
			onclick={() => (target = null)}
		>
			<XIcon />
		</Button>
	</div>

	<div class="min-h-0 flex-1 overflow-y-auto px-4 py-4">
		{#if server}
			<ServerForm {server} />
		{:else if pod}
			<PodForm {pod} />
		{:else if exit}
			<ExitForm {exit} />
		{:else if canvas}
			<SubcanvasForm {canvas} />
		{:else if card?.kind === 'splitter'}
			<SplitterForm {card} />
		{:else if card?.kind === 'aggregator'}
			<AggregatorForm {card} />
		{:else if card?.kind === 'portal'}
			<PortalForm {card} />
		{:else if bus}
			<BusForm {bus} />
		{/if}
	</div>
</div>
