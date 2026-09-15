<script lang="ts">
import { untrack } from 'svelte';
import { toast } from 'svelte-sonner';
import { replaceLoadBalanceSpec, updateNodeText } from '#lib/components/canvas/commands.js';
import { channelColor } from '#lib/components/canvas/graph.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import type {
	CanvasGraph,
	LoadBalanceModeName,
	LoadBalanceNodeDto,
	RelayProtocolName
} from '#lib/dto/topology.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';

let {
	canvasId,
	node,
	editable,
	graph
}: {
	canvasId: string;
	node: LoadBalanceNodeDto;
	editable: boolean;
	/** Resolves the exit an aggregate node's channel input is connected to. */
	graph: CanvasGraph | undefined;
} = $props();

const MODES: LoadBalanceModeName[] = ['round_robin', 'random', 'ip_hash', 'fallback'];
const modeLabel = (value: LoadBalanceModeName): string =>
	value === 'random'
		? m.editor_balance_random()
		: value === 'ip_hash'
			? m.editor_balance_ip_hash()
			: value === 'fallback'
				? m.editor_balance_fallback()
				: m.editor_balance_round_robin();
const PROTOCOLS: RelayProtocolName[] = ['tcp_raw', 'tcp_tls', 'quic'];
const protocolLabel = (value: RelayProtocolName): string =>
	value === 'tcp_tls'
		? m.editor_relay_tcp_tls()
		: value === 'quic'
			? m.editor_relay_quic()
			: m.editor_relay_tcp_raw();

let name = $state('');
let comment = $state('');
let balanceMode = $state<LoadBalanceModeName>('round_robin');
let protocol = $state<RelayProtocolName>('tcp_raw');
// `Input` renders a dynamic `type`, so Svelte never coerces: number fields are
// strings here and are converted exactly once, at the call.
let memberCount = $state('2');
let pending = $state(false);

let seededFor = $state('');
$effect(() => {
	if (seededFor === node.id) return;
	seededFor = node.id;
	const snapshot = node;
	untrack(() => {
		name = snapshot.name;
		comment = snapshot.comment;
		balanceMode = snapshot.balanceMode;
		protocol = snapshot.protocol;
		memberCount = String(snapshot.memberCount);
	});
});

async function save() {
	pending = true;
	try {
		await updateNodeText({ canvasId, nodeId: node.id, name, comment });
		// Shrinking is refused by the control plane while a removed port still
		// carries an edge; that `Conflict` text surfaces in the toast below.
		await replaceLoadBalanceSpec({
			canvasId,
			nodeId: node.id,
			mode: node.mode,
			balanceMode,
			protocol,
			memberCount: Number(memberCount)
		});
		toast.success(m.editor_saved());
	} catch (err) {
		const body = (err as { body?: App.Error }).body;
		toast.error(errorMessage(body?.code, body?.message ?? ''));
	} finally {
		pending = false;
	}
}

/** Exit node name by the `chan:` port id an edge into this node ends at. */
const exitOf = (portId: string | undefined): string | null => {
	if (!graph || !portId) return null;
	const edge = graph.edges.find(entry => entry.targetPortId === portId);
	if (!edge) return null;
	const owner = graph.nodes.find(entry => entry.ports.some(port => port.id === edge.sourcePortId));
	return owner?.name ?? null;
};
</script>

<Field.FieldGroup>
	<Field.Field>
		<Field.FieldLabel for="lb-name">{m.editor_node_name()}</Field.FieldLabel>
		<Input id="lb-name" bind:value={name} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="lb-comment">{m.editor_node_comment()}</Field.FieldLabel>
		<Textarea id="lb-comment" bind:value={comment} disabled={!editable} />
	</Field.Field>

	{#if node.mode === 'distribute'}
		<Field.Field>
			<Field.FieldLabel for="lb-mode">{m.editor_balance_mode()}</Field.FieldLabel>
			<Select.Root
				type="single"
				value={balanceMode}
				disabled={!editable}
				onValueChange={next => (balanceMode = next as LoadBalanceModeName)}
			>
				<Select.Trigger id="lb-mode">{modeLabel(balanceMode)}</Select.Trigger>
				<Select.Content>
					<Select.Group>
						{#each MODES as option (option)}
							<Select.Item value={option} label={modeLabel(option)}>{modeLabel(option)}</Select.Item>
						{/each}
					</Select.Group>
				</Select.Content>
			</Select.Root>
		</Field.Field>

		<Field.Field>
			<Field.FieldLabel for="lb-protocol">{m.editor_relay_protocol()}</Field.FieldLabel>
			<Select.Root
				type="single"
				value={protocol}
				disabled={!editable}
				onValueChange={next => (protocol = next as RelayProtocolName)}
			>
				<Select.Trigger id="lb-protocol">{protocolLabel(protocol)}</Select.Trigger>
				<Select.Content>
					<Select.Group>
						{#each PROTOCOLS as option (option)}
							<Select.Item value={option} label={protocolLabel(option)}>
								{protocolLabel(option)}
							</Select.Item>
						{/each}
					</Select.Group>
				</Select.Content>
			</Select.Root>
			<Field.FieldDescription>{m.editor_universal_protocol_hint()}</Field.FieldDescription>
		</Field.Field>
	{/if}

	<Field.Field>
		<Field.FieldLabel for="lb-members">{m.editor_member_count()}</Field.FieldLabel>
		<Input
			id="lb-members"
			type="number"
			min={0}
			max={256}
			bind:value={memberCount}
			disabled={!editable}
		/>
		<Field.FieldDescription>{m.editor_member_count_hint()}</Field.FieldDescription>
	</Field.Field>
</Field.FieldGroup>

<Button class="mt-6 w-full" disabled={!editable || pending} onclick={save}>
	{#if pending}<Spinner data-icon="inline-start" />{/if}
	{m.common_save()}
</Button>

<h3 class="mt-6 text-sm font-medium">{m.editor_universal_channels()}</h3>
{#if node.channels.length === 0}
	<p class="mt-2 text-sm text-muted-foreground">
		{node.mode === 'distribute' ? m.editor_universal_channels_hint() : m.editor_universal_no_channels()}
	</p>
{:else}
	<ul class="mt-2 grid gap-1">
		{#each node.channels as channel (channel.podId)}
			<li class="flex items-center gap-2 text-sm">
				<span class="size-3 shrink-0 rounded-full" style="background: {channelColor(channel)}"></span>
				<span class="truncate">{channel.podName}</span>
				{#if node.mode === 'aggregate'}
					<span class="ms-auto truncate text-xs text-muted-foreground">
						{exitOf(channel.portId) ?? m.editor_universal_no_exit()}
					</span>
				{/if}
			</li>
		{/each}
	</ul>
{/if}
