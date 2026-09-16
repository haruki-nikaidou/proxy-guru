<script lang="ts">
import type { AggregatorCard } from 'guru-graph';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import { cardLabel } from '#lib/components/canvas/flow/nodes.js';
import RuleDots from '#lib/components/canvas/nodes/RuleDots.svelte';
import { m } from '#lib/paraglide/messages.js';

/**
 * An aggregator is only how the drawing gathers buses: nothing about it is
 * stored or edited. The panel says what it gathers and where it leads.
 */
let { card }: { card: AggregatorCard } = $props();

const editor = useEditor();
const label = (node: string) => cardLabel(editor.graph, editor.drawing, node);
</script>

<p class="text-xs text-muted-foreground">{m.editor_aggregator_description()}</p>
<p class="mt-2"><RuleDots rules={card.rules} max={16} /></p>

<h3 class="mt-6 text-sm font-medium">{m.editor_aggregator_from()}</h3>
<ul class="mt-2 grid gap-1 text-sm">
	{#each card.sources as source (source)}
		<li>
			<button
				type="button"
				class="w-full truncate rounded px-2 py-1 text-start hover:bg-accent"
				onclick={() =>
					source.startsWith('server:') &&
					editor.open({ kind: 'server', id: source.slice('server:'.length) })}
			>
				{label(source)}
			</button>
		</li>
	{/each}
</ul>

<h3 class="mt-6 text-sm font-medium">{m.editor_aggregator_to()}</h3>
<ul class="mt-2 grid gap-1 text-sm">
	{#each card.targets as target (target)}
		<li class="truncate px-2 py-1">{label(target)}</li>
	{/each}
</ul>
