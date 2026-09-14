<script lang="ts">
import ServerIcon from '@lucide/svelte/icons/server';
import type { NodeProps } from '@xyflow/svelte';
import {
	channelColor,
	portLabel,
	serverHealthBadge,
	serverHealthLabel,
	type FlowNodeData
} from '#lib/components/canvas/graph.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import GroupHandle from './GroupHandle.svelte';
import NodeShell from './NodeShell.svelte';
import PortHandle from './PortHandle.svelte';

// A server renders as one node containing all of its pods; pods are never placed
// on the canvas themselves.
let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'server' }> } = $props();

const universal = $derived(data.server.universal);

const ipv6 = $derived(
	data.server.ipv6Resolve === 'required'
		? m.editor_ipv6_required()
		: data.server.ipv6Resolve === 'preferred'
			? m.editor_ipv6_preferred()
			: data.server.ipv6Resolve === 'forbidden'
				? m.editor_ipv6_forbidden()
				: m.editor_ipv6_tolerated()
);

const health = $derived(serverHealthBadge(data.server.healthStatus));
/**
 * A worker that is offline — or has never reported — must not be read as merely
 * stale, so the last-seen line says so in words next to the timestamp.
 */
const silent = $derived(
	data.server.healthStatus === 'offline'
		? m.editor_server_health_offline_hint()
		: data.server.healthStatus === 'unknown'
			? m.editor_server_health_unknown_hint()
			: ''
);

/** `[::]:port` for a wildcard bind, `[v6]:port` for a literal IPv6. */
const listenOf = (bindIp: string | null, port: number): string =>
	bindIp === null ? `[::]:${port}` : bindIp.includes(':') ? `[${bindIp}]:${port}` : `${bindIp}:${port}`;

/** The channel an entry pod starts, if it is connected to a distributor. */
const channelOf = (podId: string) => data.channels[podId];
</script>

<NodeShell
	{id}
	title={data.server.name}
	kindLabel={m.editor_kind_server()}
	comment={data.server.comment}
	problem={data.problem}
	background="bg-canvas-server"
	width="w-[300px]"
>
	<!-- `icon` is a free-form operator string, shown verbatim next to the glyph. -->
	{#snippet icon()}
		<ServerIcon class="size-4 shrink-0" />
		{#if data.server.icon}
			<span class="text-xs text-muted-foreground">{data.server.icon}</span>
		{/if}
	{/snippet}
	{#snippet badge()}
		<Badge variant={health.variant} class="shrink-0 {health.class}">
			{serverHealthLabel(data.server.healthStatus)}
		</Badge>
	{/snippet}
	<p class="truncate px-3 pt-1 text-xs text-muted-foreground">
		{data.server.logLevel} · {ipv6} ·
		<span class="font-mono">
			{data.server.addresses.effectiveAddress || m.editor_server_address_none_short()}
		</span>
		{#if data.server.addresses.reportedCountry}
			· <span class="font-mono">{data.server.addresses.reportedCountry}</span>
		{/if}
	</p>
	<p class="px-3 text-xs text-muted-foreground">
		{m.editor_server_last_seen()}: {formatTimestamp(data.server.lastSeenAt)}{silent
			? ` · ${silent}`
			: ''}
	</p>

	{#if universal}
		<!-- The universal pod: bundles in on the left, the one bundle out on the
		     right, and a dot per channel landing here. -->
		<div class="mt-2 border-t pt-1">
			<p class="flex items-center gap-1 truncate px-3 text-xs font-medium">
				{m.editor_universal_pod()}
				{#each universal.lanes as lane (lane.nodeId)}
					<span
						class="size-2 shrink-0 rounded-full"
						style="background: {channelColor(lane.channel)}"
						title={lane.channel.podName}
					></span>
				{/each}
				{#if universal.lanes.length > 0}
					<span class="font-mono text-muted-foreground">{universal.lanes.length}</span>
				{/if}
			</p>
			<div class="flex items-start justify-between">
				<GroupHandle
					flowId={id}
					group="bundle_in"
					label={m.editor_universal_bundle_in()}
					side="left"
					count={universal.bundleIn.length}
				/>
				<GroupHandle
					flowId={id}
					group="bundle_out"
					label={m.editor_universal_bundle_out()}
					side="right"
					single
				/>
			</div>
		</div>
	{/if}
	{#if data.server.pods.length === 0}
		<p class="px-3 py-2 text-xs text-muted-foreground">{m.editor_pod_none()}</p>
	{:else}
		{#each data.server.pods as pod (pod.id)}
			{@const channel = channelOf(pod.id)}
			<div class="mt-2 border-t pt-1">
				<p class="flex items-center gap-1 truncate px-3 text-xs font-medium">
					{#if channel}
						<span
							class="size-2 shrink-0 rounded-full"
							style="background: {channelColor(channel)}"
						></span>
					{/if}
					<span class="truncate">{pod.name}</span>
					<span class="font-mono text-muted-foreground">
						{listenOf(pod.bindIp, pod.port)}
					</span>
				</p>
				{#each pod.ports as port (port.id)}
					<PortHandle
						{port}
						label={portLabel(port)}
						color={channel ? channelColor(channel) : undefined}
					/>
				{/each}
			</div>
		{/each}
	{/if}
</NodeShell>
