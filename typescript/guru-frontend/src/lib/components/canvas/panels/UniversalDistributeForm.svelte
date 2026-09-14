<script lang="ts">
import { untrack } from 'svelte';
import { toast } from 'svelte-sonner';
import { replaceUniversalDistributeSpec, updateNodeText } from '#lib/components/canvas/commands.js';
import { channelColor } from '#lib/components/canvas/graph.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import type {
	LoadBalanceModeName,
	RelayProtocolName,
	UniversalDistributeNodeDto
} from '#lib/dto/topology.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';

let {
	canvasId,
	node,
	editable
}: { canvasId: string; node: UniversalDistributeNodeDto; editable: boolean } = $props();

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
	});
});

async function save() {
	pending = true;
	try {
		await updateNodeText({ canvasId, nodeId: node.id, name, comment });
		await replaceUniversalDistributeSpec({ canvasId, nodeId: node.id, balanceMode, protocol });
		toast.success(m.editor_saved());
	} catch (err) {
		const body = (err as { body?: App.Error }).body;
		toast.error(errorMessage(body?.code, body?.message ?? ''));
	} finally {
		pending = false;
	}
}
</script>

<Field.FieldGroup>
	<Field.Field>
		<Field.FieldLabel for="ud-name">{m.editor_node_name()}</Field.FieldLabel>
		<Input id="ud-name" bind:value={name} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="ud-comment">{m.editor_node_comment()}</Field.FieldLabel>
		<Textarea id="ud-comment" bind:value={comment} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="ud-mode">{m.editor_balance_mode()}</Field.FieldLabel>
		<Select.Root
			type="single"
			value={balanceMode}
			disabled={!editable}
			onValueChange={next => (balanceMode = next as LoadBalanceModeName)}
		>
			<Select.Trigger id="ud-mode">{modeLabel(balanceMode)}</Select.Trigger>
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
		<Field.FieldLabel for="ud-protocol">{m.editor_relay_protocol()}</Field.FieldLabel>
		<Select.Root
			type="single"
			value={protocol}
			disabled={!editable}
			onValueChange={next => (protocol = next as RelayProtocolName)}
		>
			<Select.Trigger id="ud-protocol">{protocolLabel(protocol)}</Select.Trigger>
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
</Field.FieldGroup>

<Button class="mt-6 w-full" disabled={!editable || pending} onclick={save}>
	{#if pending}<Spinner data-icon="inline-start" />{/if}
	{m.common_save()}
</Button>

<h3 class="mt-6 text-sm font-medium">{m.editor_universal_channels()}</h3>
{#if node.channels.length === 0}
	<p class="mt-2 text-sm text-muted-foreground">{m.editor_universal_channels_hint()}</p>
{:else}
	<ul class="mt-2 grid gap-1">
		{#each node.channels as channel (channel.podId)}
			<li class="flex items-center gap-2 text-sm">
				<span class="size-3 shrink-0 rounded-full" style="background: {channelColor(channel)}"></span>
				<span class="truncate">{channel.podName}</span>
			</li>
		{/each}
	</ul>
{/if}
