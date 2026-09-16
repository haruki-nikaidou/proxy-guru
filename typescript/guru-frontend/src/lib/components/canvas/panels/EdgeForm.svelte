<script lang="ts">
import { channelColor, type EdgeDetail } from '#lib/components/canvas/graph.js';
import { m } from '#lib/paraglide/messages.js';
import EdgeDeleteButton from './EdgeDeleteButton.svelte';

/**
 * The edge panel: nothing on an edge is editable, so it reads the two ends and
 * the rules riding it — every channel of a bus, the one channel of a thin
 * edge — each under the colour the canvas draws it in.
 */
let { canvasId, detail, editable }: { canvasId: string; detail: EdgeDetail; editable: boolean } =
	$props();
</script>

<dl class="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-sm">
	<dt class="text-muted-foreground">{m.editor_edge_from()}</dt>
	<dd class="min-w-0 truncate">
		{detail.source.node}
		<span class="font-mono text-xs text-muted-foreground">{detail.source.port}</span>
	</dd>
	<dt class="text-muted-foreground">{m.editor_edge_to()}</dt>
	<dd class="min-w-0 truncate">
		{detail.target.node}
		<span class="font-mono text-xs text-muted-foreground">{detail.target.port}</span>
	</dd>
</dl>

<h3 class="mt-6 text-sm font-medium">{m.editor_universal_channels()}</h3>
{#if detail.channels.length === 0}
	<p class="mt-2 text-sm text-muted-foreground">{m.editor_edge_no_channels()}</p>
{:else}
	<ul class="mt-2 grid gap-1">
		{#each detail.channels as channel (channel.podId)}
			<li class="flex items-center gap-2 text-sm">
				<span class="size-3 shrink-0 rounded-full" style="background: {channelColor(channel)}"></span>
				<span class="truncate">{channel.podName}</span>
			</li>
		{/each}
	</ul>
{/if}

<EdgeDeleteButton {canvasId} {detail} {editable} />
