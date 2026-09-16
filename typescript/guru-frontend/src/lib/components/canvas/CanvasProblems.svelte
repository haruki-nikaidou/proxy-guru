<script lang="ts">
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import type { CanvasGraph } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * What the control plane thinks is wrong with this canvas, in the corner of the
 * flow. A short list is shown outright; a long one collapses to a count until
 * the operator asks, so the grid stays readable. Clicking a problem jumps to
 * the node it concerns, which the flow does — it may live in another canvas of
 * the tree.
 */
let { graph, onopen }: { graph: CanvasGraph; onopen: (nodeIds: string[]) => void } = $props();

let open = $state(false);
const problems = $derived(graph.problems);
</script>

{#if problems.length === 0}
	<Badge variant="secondary">{m.canvas_health_ok()}</Badge>
{:else if problems.length > 3 && !open}
	<Button size="sm" variant="outline" onclick={() => (open = true)}>
		{m.editor_problems({ count: problems.length })}
	</Button>
{:else}
	<Card.Root class="max-w-md gap-2 py-3">
		<Card.Content class="grid gap-2 px-3">
			{#if graph.orphanPods.length > 0}
				<p class="text-xs text-muted-foreground">
					{m.editor_pod_orphan({ count: graph.orphanPods.length })}
				</p>
			{/if}
			{#each problems as problem (problem.message)}
				<button
					type="button"
					class="flex items-start gap-2 text-start text-xs hover:underline"
					onclick={() => onopen(problem.nodeIds)}
				>
					<Badge
						variant={problem.severity === 'error' ? 'destructive' : 'outline'}
					>
						{problem.severity === 'error'
							? m.editor_severity_error()
							: m.editor_severity_warning()}
					</Badge>
					<span>{problem.message}</span>
				</button>
			{/each}
		</Card.Content>
	</Card.Root>
{/if}
