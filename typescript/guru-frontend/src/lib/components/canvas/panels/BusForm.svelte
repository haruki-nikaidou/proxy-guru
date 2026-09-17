<script lang="ts">
import PencilIcon from '@lucide/svelte/icons/pencil';
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import { type Bus, type Edge, removeAll } from 'guru-graph';
import CopyButton from '#lib/components/CopyButton.svelte';
import { useCanvasContext } from '#lib/components/canvas/context.svelte.js';
import DialAddressDialog from '#lib/components/canvas/dialogs/DialAddressDialog.svelte';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import { handleLabel } from '#lib/components/canvas/flow/nodes.js';
import { Button } from '#lib/components/ui/button/index.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * A bus: every edge that takes the same way between two handles. The panel says
 * where it runs, which rules ride it — each in the colour of its thin line on
 * the canvas, with how many of the edges carry it — and lists the edges: their
 * ids, the rules each carries, from which pod to where, and how it dials.
 */
let { bus }: { bus: Bus } = $props();

const editor = useEditor();
const canvas = useCanvasContext();
const graph = $derived(editor.graph);
let dialing = $state<Edge | null>(null);

const rulesOf = (edgeId: string) => editor.drawing.rules.edges.get(edgeId) ?? [];
/** A pod as the panel names it: relay pods of one rule share a name across servers. */
const podLabel = (podId: string) => {
	const pod = graph.pods.find(entry => entry.id === podId);
	if (!pod) return { name: podId, server: '' };
	return {
		name: pod.name,
		server: graph.servers.find(server => server.id === pod.serverId)?.name ?? ''
	};
};

const lines = $derived(
	bus.rules.map(rule => ({
		rule,
		server: podLabel(rule).server,
		edges: bus.edges.filter(id => rulesOf(id).includes(rule)).length
	}))
);

const edges = $derived(
	bus.edges.flatMap(id => {
		const edge = graph.edges.find(entry => entry.id === id);
		if (!edge) return [];
		const target =
			'pod' in edge.target
				? podLabel(edge.target.pod)
				: {
						name:
							graph.exits.find(exit => 'exit' in edge.target && exit.id === edge.target.exit)
								?.name ?? edge.target.exit,
						server: ''
					};
		return [{ edge, rules: rulesOf(id), source: podLabel(edge.sourcePodId), target }];
	})
);

function remove() {
	editor.review({
		title: m.editor_bus_delete(),
		description: m.editor_bus_delete_description({ count: bus.edges.length }),
		prunable: true,
		build: prune => removeAll(editor.graph, editor.drawing, { edgeIds: bus.edges }, prune),
		success: m.editor_deleted()
	});
}
</script>

{#snippet swatch(rule: string, width: string)}
	<span
		class="block h-[3px] shrink-0 rounded-full {width}"
		style="background: {canvas.ruleColor(rule)}"
		title={canvas.ruleName(rule)}
	></span>
{/snippet}

<dl class="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-sm">
	<dt class="text-muted-foreground">{m.editor_edge_from()}</dt>
	<dd class="min-w-0 truncate">{handleLabel(graph, editor.drawing, bus.source)}</dd>
	<dt class="text-muted-foreground">{m.editor_edge_to()}</dt>
	<dd class="min-w-0 truncate">{handleLabel(graph, editor.drawing, bus.target)}</dd>
</dl>

<h3 class="mt-6 text-sm font-medium">{m.editor_bus_lines()}</h3>
<p class="mt-1 text-xs text-muted-foreground">{m.editor_bus_lines_hint()}</p>
<ul class="mt-2 grid gap-1.5">
	{#each lines as line (line.rule)}
		<li class="flex items-center gap-2 text-sm">
			{@render swatch(line.rule, 'w-6')}
			<span class="min-w-0 truncate">{canvas.ruleName(line.rule)}</span>
			<span class="min-w-0 shrink truncate text-xs text-muted-foreground">{line.server}</span>
			<span class="ms-auto shrink-0 text-xs text-muted-foreground">
				{m.editor_bus_edges({ count: line.edges })}
			</span>
		</li>
	{/each}
</ul>

<h3 class="mt-6 text-sm font-medium">{m.editor_bus_edges({ count: bus.edges.length })}</h3>
<ul class="mt-2 grid gap-2">
	{#each edges as { edge, rules, source, target } (edge.id)}
		<li class="rounded-lg border px-3 py-2 text-sm">
			<div class="flex items-center gap-2">
				<span class="flex w-4 shrink-0 flex-col gap-0.5">
					{#each rules as rule (rule)}
						{@render swatch(rule, 'w-4')}
					{/each}
				</span>
				<div class="min-w-0 flex-1">
					<button
						type="button"
						class="max-w-full truncate text-start hover:underline"
						onclick={() => editor.open({ kind: 'pod', id: edge.sourcePodId })}
					>
						{source.name}
						{#if source.server}<span class="text-xs text-muted-foreground">· {source.server}</span>{/if}
					</button>
					<p class="truncate text-muted-foreground">
						→ {target.name}
						{#if target.server}<span class="text-xs">· {target.server}</span>{/if}
						{#if edge.overrideIp || edge.overridePort}
							<span class="font-mono text-xs">
								({edge.overrideIp ?? ''}{edge.overridePort ? `:${edge.overridePort}` : ''})
							</span>
						{/if}
					</p>
				</div>
				{#if editor.editable && 'pod' in edge.target}
					<Button
						size="icon-xs"
						variant="ghost"
						class="shrink-0"
						aria-label={m.editor_dial_title()}
						onclick={() => (dialing = edge)}
					>
						<PencilIcon />
					</Button>
				{/if}
			</div>
			<div class="mt-1 flex items-center gap-1 ps-6 text-xs text-muted-foreground">
				<span class="shrink-0">{m.editor_edge_id()}</span>
				<span class="min-w-0 truncate font-mono">{edge.id}</span>
				<CopyButton value={edge.id} label={m.editor_edge_id_copy()} size="icon-xs" class="shrink-0" />
			</div>
		</li>
	{/each}
</ul>

{#if editor.editable}
	<Button class="mt-6 w-full" variant="outline" onclick={remove}>
		<Trash2Icon />
		{m.editor_bus_delete()}
	</Button>
{/if}

<DialAddressDialog bind:edge={dialing} />
