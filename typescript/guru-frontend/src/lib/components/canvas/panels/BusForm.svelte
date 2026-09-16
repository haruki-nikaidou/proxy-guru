<script lang="ts">
import PencilIcon from '@lucide/svelte/icons/pencil';
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import { type Bus, type Edge, removeAll } from 'guru-graph';
import DialAddressDialog from '#lib/components/canvas/dialogs/DialAddressDialog.svelte';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import { cardLabel } from '#lib/components/canvas/flow/nodes.js';
import RuleDots from '#lib/components/canvas/nodes/RuleDots.svelte';
import { Button } from '#lib/components/ui/button/index.js';
import * as Table from '#lib/components/ui/table/index.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * A bus: every edge that takes the same way between two cards. The panel lists
 * the edges it stands for, each from its pod to where it leads, and how it dials.
 */
let { bus }: { bus: Bus } = $props();

const editor = useEditor();
const graph = $derived(editor.graph);
let dialing = $state<Edge | null>(null);

const edges = $derived(
	bus.edges.flatMap(id => {
		const edge = graph.edges.find(entry => entry.id === id);
		if (!edge) return [];
		const source = graph.pods.find(pod => pod.id === edge.sourcePodId);
		const target =
			'pod' in edge.target
				? graph.pods.find(pod => 'pod' in edge.target && pod.id === edge.target.pod)?.name
				: graph.exits.find(exit => 'exit' in edge.target && exit.id === edge.target.exit)?.name;
		return [{ edge, source: source?.name ?? edge.sourcePodId, target: target ?? '' }];
	})
);

function remove() {
	const current = editor.graph;
	editor.review({
		title: m.editor_bus_delete(),
		description: m.editor_bus_delete_description({ count: bus.edges.length }),
		prunable: true,
		build: prune => removeAll(current, editor.drawing, { edgeIds: bus.edges }, prune),
		success: m.editor_deleted()
	});
}
</script>

<dl class="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-sm">
	<dt class="text-muted-foreground">{m.editor_edge_from()}</dt>
	<dd class="min-w-0 truncate">{cardLabel(graph, editor.drawing, bus.source.node)}</dd>
	<dt class="text-muted-foreground">{m.editor_edge_to()}</dt>
	<dd class="min-w-0 truncate">{cardLabel(graph, editor.drawing, bus.target.node)}</dd>
</dl>
<p class="mt-2"><RuleDots rules={bus.rules} max={16} /></p>

<h3 class="mt-6 text-sm font-medium">{m.editor_bus_edges({ count: bus.edges.length })}</h3>
<Table.Root class="mt-2">
	<Table.Body>
		{#each edges as { edge, source, target } (edge.id)}
			<Table.Row>
				<Table.Cell class="max-w-0 truncate">
					<button
						type="button"
						class="truncate hover:underline"
						onclick={() => editor.open({ kind: 'pod', id: edge.sourcePodId })}
					>
						{source}
					</button>
				</Table.Cell>
				<Table.Cell class="max-w-0 truncate text-muted-foreground">
					→ {target}
					{#if edge.overrideIp || edge.overridePort}
						<span class="font-mono text-xs">
							({edge.overrideIp ?? ''}{edge.overridePort ? `:${edge.overridePort}` : ''})
						</span>
					{/if}
				</Table.Cell>
				<Table.Cell class="w-8">
					{#if editor.editable && 'pod' in edge.target}
						<Button size="icon-xs" variant="ghost" aria-label={m.editor_dial_title()} onclick={() => (dialing = edge)}>
							<PencilIcon />
						</Button>
					{/if}
				</Table.Cell>
			</Table.Row>
		{/each}
	</Table.Body>
</Table.Root>

{#if editor.editable}
	<Button class="mt-6 w-full" variant="outline" onclick={remove}>
		<Trash2Icon />
		{m.editor_bus_delete()}
	</Button>
{/if}

<DialAddressDialog bind:edge={dialing} />
