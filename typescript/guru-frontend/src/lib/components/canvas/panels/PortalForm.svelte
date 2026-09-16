<script lang="ts">
import ArrowRightIcon from '@lucide/svelte/icons/arrow-right';
import type { PortalCard } from 'guru-graph';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import { Button } from '#lib/components/ui/button/index.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * A canvas outside this one that edges of this canvas lead into or come from:
 * what of it they touch, each a link to where it is drawn.
 */
let { card }: { card: PortalCard } = $props();

const editor = useEditor();
const serverName = (id: string) =>
	editor.graph.servers.find(server => server.id === id)?.name ?? id;
</script>

{#if card.canvas}
	<Button class="w-full" variant="secondary" href="/canvas/{card.canvas.id}">
		{m.editor_portal_open({ name: card.canvas.name })}
		<ArrowRightIcon />
	</Button>
{/if}
<p class="mt-2 text-xs text-muted-foreground">{m.editor_portal_description()}</p>

{#if card.pods.length > 0}
	<h3 class="mt-6 text-sm font-medium">{m.editor_pods()}</h3>
	<ul class="mt-2 grid gap-1 text-sm">
		{#each card.pods as pod (pod.id)}
			<li>
				<a class="flex items-center gap-2 rounded px-2 py-1 hover:bg-accent" href="/canvas/{pod.canvasId}?focus=pod:{pod.id}">
					<span class="min-w-0 truncate">{pod.name}</span>
					<span class="ms-auto shrink-0 text-xs text-muted-foreground">{serverName(pod.serverId)}</span>
				</a>
			</li>
		{/each}
	</ul>
{/if}
{#if card.exits.length > 0}
	<h3 class="mt-6 text-sm font-medium">{m.editor_exits()}</h3>
	<ul class="mt-2 grid gap-1 text-sm">
		{#each card.exits as exit (exit.id)}
			<li>
				<a class="flex items-center gap-2 rounded px-2 py-1 hover:bg-accent" href="/canvas/{exit.canvasId}?focus=exit:{exit.id}">
					<span class="min-w-0 truncate">{exit.name}</span>
					<span class="ms-auto shrink-0 font-mono text-xs text-muted-foreground">{exit.destination}</span>
				</a>
			</li>
		{/each}
	</ul>
{/if}
