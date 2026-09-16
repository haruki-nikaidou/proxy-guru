<script lang="ts">
import type { NodeProps } from '@xyflow/svelte';
import ServerGlyph from '#lib/components/canvas/ServerGlyph.svelte';
import { channelColor, portLabel, type FlowNodeData } from '#lib/components/canvas/graph.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import { serverHealthBadge, serverHealthLabel } from '#lib/i18n/labels.js';
import type { ServerAddressesDto } from '#lib/dto/topology.js';
import GroupHandle from './GroupHandle.svelte';
import NodeShell from './NodeShell.svelte';
import PortHandle from './PortHandle.svelte';

// A server renders as one node containing all of its pods; pods are never placed
// on the canvas themselves.
let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'server' }> } = $props();

const universal = $derived(data.server.universal);

/**
 * The address of one family, walked the way `ServerEntity::effective_address`
 * walks each family on the control plane: the pinned address, else the one the
 * worker reported, else the address it registered from when that is of this
 * family. Empty when the family has none.
 */
const addressOf = (addresses: ServerAddressesDto, family: 'v4' | 'v6'): string => {
	const observed = addresses.observedAddress;
	const observedFamily = observed === '' ? null : observed.includes(':') ? 'v6' : 'v4';
	return (
		addresses[family].pinned ||
		addresses[family].reported ||
		(observedFamily === family ? observed : '')
	);
};
const v4 = $derived(addressOf(data.server.addresses, 'v4'));
const v6 = $derived(addressOf(data.server.addresses, 'v6'));

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
	bindIp === null
		? `[::]:${port}`
		: bindIp.includes(':')
			? `[${bindIp}]:${port}`
			: `${bindIp}:${port}`;

/** The channel an entry pod starts, if it is connected to a distributor. */
const channelOf = (podId: string) => data.channels[podId];
</script>

{#snippet addressLine(label: string, address: string)}
	<p class="truncate px-3 text-xs text-muted-foreground">
		{label}
		{#if address}
			<span class="font-mono">{address}</span>
		{:else}
			{m.editor_server_address_none_short()}
		{/if}
	</p>
{/snippet}

<NodeShell
	{id}
	title={data.server.name}
	kindLabel={m.editor_kind_server()}
	comment={data.server.comment}
	problem={data.problem}
	background="bg-canvas-server"
	width="w-[300px]"
>
	<!-- `icon` names an Iconify icon (`flag:*` / `logo:*`); invalid or empty
	     falls back to the default server glyph. -->
	{#snippet icon()}<ServerGlyph icon={data.server.icon} />{/snippet}
	{#snippet badge()}
		<Badge variant={health.variant} class="shrink-0 {health.class}">
			{serverHealthLabel(data.server.healthStatus)}
		</Badge>
	{/snippet}
	<!-- One line per address family; the server's settings (log level, IPv6
	     policy, QUIC) live in the panel, not on the card. -->
	<div class="pt-1">
		{@render addressLine(m.editor_server_address_v4(), v4)}
		{@render addressLine(m.editor_server_address_v6(), v6)}
	</div>
	<!-- The health report time, not `lastSeenAt`: the latter also moves with the
	     config stream's heartbeat, which the master keeps up on its own timer, so
	     it can read "5 s ago" on a server that is offline. -->
	<p class="px-3 text-xs text-muted-foreground">
		{m.editor_server_last_seen()}: {formatTimestamp(data.server.lastHealthReportAt)}{silent
			? ` · ${silent}`
			: ''}{data.server.agentVersion ? ` · v${data.server.agentVersion}` : ''}
	</p>

	{#if universal}
		{@const landed = [...new Map(universal.lanes.map(lane => [lane.channel.podId, lane.channel])).values()]}
		<!-- The universal pod: bundles in on the left, the one bundle out on the
		     right, and a dot per channel (rule) landing here. A rule arriving by
		     two paths lands twice, so the counts say both: channels and pods. -->
		<div class="mt-2 border-t pt-1">
			<p class="flex items-center gap-1 truncate px-3 text-xs font-medium">
				{m.editor_universal_pod()}
				{#each landed as channel (channel.podId)}
					<span
						class="size-2 shrink-0 rounded-full"
						style="background: {channelColor(channel)}"
						title={channel.podName}
					></span>
				{/each}
				{#if universal.lanes.length > 0}
					<span class="truncate font-mono text-muted-foreground">
						{m.editor_universal_summary({ channels: landed.length, lanes: universal.lanes.length })}
					</span>
				{/if}
			</p>
			<div class="grid grid-cols-2">
				<div>
					{#each universal.bundleIn as port (port.id)}
						<PortHandle {port} label={port.peerName} side="left" />
					{/each}
					{#each universal.channels as channel (channel.podId)}
						<PortHandle
							port={{
								id: channel.portId,
								kind: 'derive_destination',
								direction: 'output',
								key: `chan:${channel.podId}`,
								position: channel.ordinal,
								label: null
							}}
							label={channel.podName}
							color={channelColor(channel)}
							side="left"
						/>
					{/each}
					<GroupHandle
						flowId={id}
						group="bundle_in"
						label={m.editor_universal_add_bundle()}
						side="left"
					/>
					<GroupHandle
						flowId={id}
						group="channel_out"
						label={m.editor_universal_add_channel()}
						side="left"
					/>
				</div>
				<div>
					{#if universal.bundleOut}
						<PortHandle
							port={universal.bundleOut}
							label={m.editor_universal_bundle_out()}
							side="right"
						/>
					{/if}
				</div>
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
