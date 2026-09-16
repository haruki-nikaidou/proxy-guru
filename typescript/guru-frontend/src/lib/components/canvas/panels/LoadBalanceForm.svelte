<script lang="ts">
import PlusIcon from '@lucide/svelte/icons/plus';
import Trash2Icon from '@lucide/svelte/icons/trash-2';
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
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';

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
/**
 * The member list as edited: the operator's rule. A slot is the stable number
 * behind a member's bundle port, so renaming keeps the bundle; a new member
 * takes the next free slot; a removed member takes its port with it, which
 * the control plane refuses while a bundle is still drawn on it.
 */
let members = $state<{ slot: number; name: string }[]>([]);
const writes = panelWrites();

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
		members = snapshot.members.map(member => ({ slot: member.slot, name: member.name }));
	});
});

/** The far end of a member's bundle as stored, or nothing while unwired. */
const peerOf = (slot: number): string | null =>
	node.members.find(member => member.slot === slot)?.peerName ?? null;

function addMember() {
	const next = members.reduce((max, member) => Math.max(max, member.slot), 0) + 1;
	members = [...members, { slot: next, name: String(next) }];
}

function removeMember(slot: number) {
	members = members.filter(member => member.slot !== slot);
}

const valid = $derived(
	members.length > 0 &&
		members.every(member => member.name.trim() !== '') &&
		new Set(members.map(member => member.name.trim())).size === members.length
);

async function save() {
	await writes.run(async () => {
		await updateNodeText({ canvasId, nodeId: node.id, name, comment });
		// Dropping a wired member is refused by the control plane; that
		// `Conflict` text surfaces in the toast below.
		await replaceLoadBalanceSpec({
			canvasId,
			nodeId: node.id,
			mode: node.mode,
			balanceMode,
			protocol,
			members: members.map(member => ({ slot: member.slot, name: member.name.trim() }))
		});
	}, m.editor_saved());
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
</Field.FieldGroup>

<h3 class="mt-6 text-sm font-medium">{m.editor_members()} · {members.length}</h3>
<p class="mt-1 text-xs text-muted-foreground">
	{node.mode === 'distribute' ? m.editor_members_hint_distribute() : m.editor_members_hint_aggregate()}
</p>
<ul class="mt-2 grid gap-2">
	{#each members as member, i (member.slot)}
		{@const peer = peerOf(member.slot)}
		<li class="flex items-center gap-2">
			<Input
				id="lb-member-{member.slot}"
				bind:value={members[i].name}
				disabled={!editable}
				placeholder={m.editor_member_name()}
				aria-label={m.editor_member_name()}
			/>
			<span class="w-24 shrink-0 truncate text-xs text-muted-foreground" title={peer ?? ''}>
				{peer ?? m.editor_member_unwired()}
			</span>
			<Button
				size="sm"
				variant="ghost"
				disabled={!editable || writes.pending || members.length === 1}
				onclick={() => removeMember(member.slot)}
				aria-label={m.common_delete()}
			>
				<Trash2Icon />
			</Button>
		</li>
	{/each}
</ul>
<Button
	class="mt-2"
	size="sm"
	variant="outline"
	disabled={!editable || writes.pending || members.length >= 256}
	onclick={addMember}
>
	<PlusIcon />
	{m.editor_member_add()}
</Button>

<Button class="mt-6 w-full" disabled={!editable || writes.pending || !valid} onclick={save}>
	{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
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
