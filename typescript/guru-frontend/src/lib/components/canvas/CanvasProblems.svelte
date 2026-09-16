<script lang="ts">
import type { Drawing, Subject } from 'guru-graph';
import { diagnosticsFor } from '#lib/components/canvas/flow/nodes.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import type { CanvasGraph, DiagnosticDto, ServerDto } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * What the control plane finds wrong with the graph of this tree, in the corner
 * of the flow: what concerns this canvas first, the rest of the tree after. A
 * short list is shown outright; a long one collapses to a count until the
 * operator asks. Clicking a problem opens what it is about — which may live on
 * another canvas of the tree.
 */
let {
	graph,
	drawing,
	onopen
}: {
	graph: CanvasGraph;
	drawing: Drawing<ServerDto>;
	onopen: (subject: Subject) => void;
} = $props();

let open = $state(false);
const split = $derived(diagnosticsFor(graph, drawing));
const all = $derived([...split.here, ...split.elsewhere]);
const errors = $derived(all.filter(diagnostic => diagnostic.error).length);
</script>

{#snippet row(diagnostic: DiagnosticDto)}
	{@const [subject] = diagnostic.subjects}
	<button
		type="button"
		class="flex items-start gap-2 text-start text-xs hover:underline disabled:no-underline"
		disabled={!subject}
		onclick={() => subject && onopen(subject)}
	>
		<Badge variant={diagnostic.error ? 'destructive' : 'outline'} class="shrink-0">
			{diagnostic.error ? m.editor_severity_error() : m.editor_severity_warning()}
		</Badge>
		<span>{diagnostic.message}</span>
	</button>
{/snippet}

{#if all.length === 0}
	<Badge variant="secondary">{m.canvas_health_ok()}</Badge>
{:else if !open && (all.length > 3 || split.here.length === 0)}
	<Button size="sm" variant={errors > 0 ? 'destructive' : 'outline'} onclick={() => (open = true)}>
		{m.editor_problems({ count: all.length })}
	</Button>
{:else}
	<Card.Root class="max-h-80 max-w-md gap-2 overflow-y-auto py-3">
		<Card.Content class="grid gap-2 px-3">
			{#each split.here as diagnostic, index (index)}
				{@render row(diagnostic)}
			{/each}
			{#if split.elsewhere.length > 0}
				<p class="pt-1 text-xs font-medium text-muted-foreground">{m.editor_problems_elsewhere()}</p>
				{#each split.elsewhere as diagnostic, index (index)}
					{@render row(diagnostic)}
				{/each}
			{/if}
			{#if all.length > 3}
				<Button size="sm" variant="ghost" class="justify-self-start" onclick={() => (open = false)}>
					{m.common_close()}
				</Button>
			{/if}
		</Card.Content>
	</Card.Root>
{/if}
