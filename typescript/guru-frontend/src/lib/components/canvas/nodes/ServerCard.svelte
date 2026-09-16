<script lang="ts">
import LogInIcon from '@lucide/svelte/icons/log-in';
import type { NodeProps } from '@xyflow/svelte';
import { useCanvasContext } from '#lib/components/canvas/context.svelte.js';
import type { FlowNodeData } from '#lib/components/canvas/flow/nodes.js';
import ServerGlyph from '#lib/components/canvas/ServerGlyph.svelte';
import { Badge } from '#lib/components/ui/badge/index.js';
import type { ServerAddressesDto } from '#lib/dto/topology.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import {
	ingressLabel,
	ingressProtocolLabel,
	isClientIngress,
	listenLabel,
	serverHealthBadge,
	serverHealthLabel
} from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import CardHandle from './CardHandle.svelte';
import CardShell from './CardShell.svelte';
import RuleDots from './RuleDots.svelte';

// A server is the set of pods that run on it: the card lists the pods drawn on
// this canvas, each with the handles a new connection starts from or lands on.
// Buses between cards attach to the header.
let { id, data }: NodeProps & { data: Extract<FlowNodeData, { kind: 'server' }> } = $props();

const canvas = useCanvasContext();

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
const faded = $derived(
	canvas.highlightedRule !== null && !data.rules.includes(canvas.highlightedRule)
);
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

<CardShell
	{id}
	title={data.server.name}
	kindLabel={data.ghost ? m.editor_server_ghost() : m.editor_kind_server()}
	comment={data.server.comment}
	problem={data.problem}
	background={data.ghost ? 'bg-canvas-server/50 border-dashed' : 'bg-canvas-server'}
	width="w-[320px]"
	{faded}
>
	{#snippet handles()}
		<CardHandle id="in" side="left" role="end" />
		<CardHandle id="out" side="right" role="bus" />
	{/snippet}
	<!-- `icon` names an Iconify icon (`flag:*` / `logo:*`); empty shows the flag
	     of the IPv4 address's country, and anything unresolvable the default
	     server glyph. -->
	{#snippet icon()}
		<ServerGlyph icon={data.server.icon} country={data.server.addresses.country} />
	{/snippet}
	{#snippet badge()}
		<Badge variant={health.variant} class="shrink-0 {health.class}">
			{serverHealthLabel(data.server.healthStatus)}
		</Badge>
	{/snippet}
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

	<div class="mt-2 border-t pt-1">
		{#if data.pods.length === 0}
			<p class="px-3 py-1 text-xs text-muted-foreground">{m.editor_pod_none()}</p>
		{/if}
		{#each data.pods as drawn (drawn.pod.id)}
			{@const client = isClientIngress(drawn.pod.ingress.kind)}
			{@const focused = canvas.focusedPod === drawn.pod.id}
			<!-- The row opens the pod, the rest of the card the server: the canvas
			     tells the two apart by `data-pod-id` on the click's target. -->
			<div
				data-pod-id={drawn.pod.id}
				class="relative flex cursor-pointer items-center gap-1.5 px-3 py-1 text-xs hover:bg-accent/50"
				class:bg-accent={focused}
				class:text-destructive={drawn.problem === 'error'}
				class:text-amber-600={drawn.problem === 'warning'}
			>
				{#if !client}
					<CardHandle id="pod-in:{drawn.pod.id}" side="left" role="end" />
				{/if}
				<RuleDots rules={drawn.rules} max={4} />
				<span class="min-w-0 truncate font-medium">{drawn.pod.name}</span>
				<Badge
					variant={client ? 'default' : 'outline'}
					class="h-4 shrink-0 gap-0.5 px-1 text-[10px]"
					title={ingressLabel(drawn.pod.ingress.kind)}
				>
					{#if client}<LogInIcon class="size-2.5" />{/if}
					{ingressProtocolLabel(drawn.pod.ingress.kind)}
				</Badge>
				<span class="ms-auto shrink-0 font-mono text-muted-foreground">
					{drawn.pod.port === 0
						? m.editor_pod_port_auto()
						: listenLabel(drawn.pod.bindIp, drawn.pod.port)}
				</span>
				<CardHandle id="pod-out:{drawn.pod.id}" side="right" role="start" />
			</div>
		{/each}
	</div>
</CardShell>
