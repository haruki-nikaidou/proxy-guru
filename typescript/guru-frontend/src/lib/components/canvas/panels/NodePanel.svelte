<script lang="ts">
import XIcon from '@lucide/svelte/icons/x';
import type { PanelTarget } from '#lib/components/canvas/graph.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import type { CanvasGraph } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import CanvasExportForm from './CanvasExportForm.svelte';
import CanvasImportForm from './CanvasImportForm.svelte';
import EntryForm from './EntryForm.svelte';
import ExitForm from './ExitForm.svelte';
import LoadBalanceForm from './LoadBalanceForm.svelte';
import NodeDeleteButton from './NodeDeleteButton.svelte';
import RelayForm from './RelayForm.svelte';
import ServerForm from './ServerForm.svelte';
import UniversalAggregateForm from './UniversalAggregateForm.svelte';
import UniversalDistributeForm from './UniversalDistributeForm.svelte';

let {
	target = $bindable(null),
	canvasId,
	editable,
	admin,
	graph
}: {
	target?: PanelTarget | null;
	canvasId: string;
	editable: boolean;
	/** Admin-only affordances (forgetting an applied config, listing DNS providers). */
	admin: boolean;
	graph: CanvasGraph | undefined;
} = $props();

// The target is resolved against the graph on every render, so a refresh reflects
// straight into the open panel.
const server = $derived(
	target?.kind === 'server' ? graph?.servers.find(entry => entry.id === target?.id) : undefined
);
const node = $derived(
	target?.kind === 'node' ? graph?.nodes.find(entry => entry.id === target?.id) : undefined
);

// A rollout names the servers it waits for by id; only this canvas's servers can
// be resolved to a name here, which is what the panel shows.
const serverNames = $derived(new Map((graph?.servers ?? []).map(entry => [entry.id, entry.name])));

// A deleted entity closes its own panel.
$effect(() => {
	if (target && graph && !server && !node) target = null;
});

const title = $derived(server?.name ?? node?.name ?? '');
const kindLabel = $derived(
	server
		? m.editor_kind_server()
		: node?.kind === 'entry'
			? m.editor_kind_entry()
			: node?.kind === 'relay'
				? m.editor_kind_relay()
				: node?.kind === 'exit'
					? m.editor_kind_exit()
					: node?.kind === 'canvas_import'
						? m.editor_kind_subcanvas()
						: node?.kind === 'canvas_export'
							? m.editor_kind_export()
							: node?.kind === 'load_balance'
								? node.mode === 'distribute'
									? m.editor_kind_lb_distribute()
									: m.editor_kind_lb_aggregate()
								: node?.kind === 'universal_distribute'
									? m.editor_kind_universal_distribute()
									: node?.kind === 'universal_aggregate'
										? m.editor_kind_universal_aggregate()
										: ''
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
			<ServerForm {canvasId} {server} {editable} {admin} {serverNames} />
		{:else if node?.kind === 'entry'}
			<EntryForm {canvasId} {node} {editable} {admin} />
		{:else if node?.kind === 'relay'}
			<RelayForm {canvasId} {node} {editable} />
		{:else if node?.kind === 'exit'}
			<ExitForm {canvasId} {node} {editable} />
		{:else if node?.kind === 'load_balance'}
			<LoadBalanceForm {canvasId} {node} {editable} />
		{:else if node?.kind === 'canvas_import'}
			<CanvasImportForm {canvasId} {node} {editable} />
		{:else if node?.kind === 'canvas_export'}
			<CanvasExportForm {canvasId} {node} {editable} />
		{:else if node?.kind === 'universal_distribute'}
			<UniversalDistributeForm {canvasId} {node} {editable} />
		{:else if node?.kind === 'universal_aggregate'}
			<UniversalAggregateForm {node} graph={graph} />
		{/if}
		{#if node}
			<NodeDeleteButton {canvasId} {node} {editable} />
		{/if}
	</div>
</div>
