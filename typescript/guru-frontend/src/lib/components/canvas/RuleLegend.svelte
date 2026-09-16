<script lang="ts">
import type { Drawing } from 'guru-graph';
import { useCanvasContext } from '#lib/components/canvas/context.svelte.js';
import * as Card from '#lib/components/ui/card/index.js';
import type { CanvasGraph, ServerDto } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * The rules of this canvas: every client pod whose traffic passes through
 * something drawn here, in its colour. Clicking one singles it out: whatever
 * does not carry it fades.
 */
let {
	graph,
	drawing,
	highlighted = $bindable(null)
}: {
	graph: CanvasGraph;
	drawing: Drawing<ServerDto>;
	highlighted?: string | null;
} = $props();

const canvas = useCanvasContext();

const shown = $derived.by(() => {
	const here = new Set<string>();
	for (const bus of drawing.buses) for (const rule of bus.rules) here.add(rule);
	for (const card of drawing.cards) {
		if (card.kind === 'server') for (const rule of card.rules) here.add(rule);
	}
	return drawing.rules.rules.filter(rule => here.has(rule));
});
const serverOf = (rule: string) => {
	const pod = graph.pods.find(entry => entry.id === rule);
	return graph.servers.find(server => server.id === pod?.serverId)?.name ?? '';
};
</script>

{#if shown.length > 0}
	<Card.Root class="max-h-72 max-w-56 gap-1 overflow-y-auto py-2">
		<Card.Content class="grid gap-0.5 px-2">
			<p class="px-1 text-[10px] uppercase text-muted-foreground">{m.editor_rules()}</p>
			{#each shown as rule (rule)}
				<button
					type="button"
					class="flex items-center gap-2 rounded px-1 py-0.5 text-start text-xs hover:bg-accent"
					class:bg-accent={highlighted === rule}
					onclick={() => (highlighted = highlighted === rule ? null : rule)}
				>
					<span class="size-2.5 shrink-0 rounded-full" style="background: {canvas.ruleColor(rule)}"></span>
					<span class="min-w-0 truncate">{canvas.ruleName(rule)}</span>
					<span class="ms-auto shrink-0 truncate text-[10px] text-muted-foreground">{serverOf(rule)}</span>
				</button>
			{/each}
		</Card.Content>
	</Card.Root>
{/if}
