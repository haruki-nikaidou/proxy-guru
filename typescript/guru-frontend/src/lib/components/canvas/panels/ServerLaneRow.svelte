<script lang="ts">
import TriangleAlertIcon from '@lucide/svelte/icons/triangle-alert';
import { untrack } from 'svelte';
import { replacePodSpec } from '#lib/components/canvas/commands.js';
import { channelColor } from '#lib/components/canvas/graph.js';
import { Button } from '#lib/components/ui/button/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import type { LaneDto } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';

/**
 * One generated landing pod. The row belongs to the bundle that made it; only
 * the port is the operator's (the addresses stay as generated: every address,
 * advertised as the server).
 */
let { canvasId, lane, editable }: { canvasId: string; lane: LaneDto; editable: boolean } = $props();

let port = $state('1');
const writes = panelWrites();
let seededFor = $state('');
$effect(() => {
	if (seededFor === lane.nodeId) return;
	seededFor = lane.nodeId;
	const snapshot = lane;
	untrack(() => {
		port = String(snapshot.port);
	});
});

async function save() {
	await writes.run(async () => {
		await replacePodSpec({
			canvasId,
			nodeId: lane.nodeId,
			serverId: lane.serverId,
			port: Number(port),
			bindIp: lane.bindIp ?? '',
			advertiseIp: lane.advertiseIp ?? ''
		});
	}, m.editor_saved());
}
</script>

<div class="flex items-center gap-2">
	<span class="size-3 shrink-0 rounded-full" style="background: {channelColor(lane.channel)}"></span>
	<div class="min-w-0 flex-1">
		<p class="truncate text-sm">{lane.channel.podName}</p>
		<p class="truncate text-xs text-muted-foreground">
			{m.editor_lane_via({ source: lane.sourceName })}
			{#if !lane.hasExit}
				<span class="inline-flex items-center gap-1 text-amber-600 dark:text-amber-400">
					<TriangleAlertIcon class="size-3" />
					{m.editor_lane_no_exit()}
				</span>
			{/if}
		</p>
	</div>
	<Input
		type="number"
		min={1}
		max={65535}
		class="w-24"
		bind:value={port}
		disabled={!editable}
		aria-label={m.editor_pod_port()}
	/>
	<Button
		size="sm"
		variant="secondary"
		disabled={!editable || writes.pending || Number(port) === lane.port}
		onclick={save}
	>
		{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
		{m.common_save()}
	</Button>
</div>
