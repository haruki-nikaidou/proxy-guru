<script lang="ts">
import { untrack } from 'svelte';
import { replaceRelaySpec, updateNodeText } from '#lib/components/canvas/commands.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import type { RelayNodeDto, RelayProtocolName } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';

let { canvasId, node, editable }: { canvasId: string; node: RelayNodeDto; editable: boolean } =
	$props();

const PROTOCOLS: RelayProtocolName[] = ['tcp_raw', 'tcp_tls', 'quic'];
const protocolLabel = (value: RelayProtocolName): string =>
	value === 'tcp_tls'
		? m.editor_relay_tcp_tls()
		: value === 'quic'
			? m.editor_relay_quic()
			: m.editor_relay_tcp_raw();

let name = $state('');
let comment = $state('');
let protocol = $state<RelayProtocolName>('tcp_raw');
let overrideIpAddress = $state('');
// `Input` renders a dynamic `type`, so Svelte never coerces this to a number.
let overridePort = $state('0');
const writes = panelWrites();

let seededFor = $state('');
$effect(() => {
	if (seededFor === node.id) return;
	seededFor = node.id;
	const snapshot = node;
	untrack(() => {
		name = snapshot.name;
		comment = snapshot.comment;
		protocol = snapshot.protocol;
		overrideIpAddress = snapshot.overrideIpAddress;
		overridePort = String(snapshot.overridePort);
	});
});

async function save() {
	await writes.run(async () => {
		await updateNodeText({ canvasId, nodeId: node.id, name, comment });
		await replaceRelaySpec({
			canvasId,
			nodeId: node.id,
			protocol,
			overrideIpAddress,
			overridePort: Number(overridePort)
		});
	}, m.editor_saved());
}
</script>

<Field.FieldGroup>
	<Field.Field>
		<Field.FieldLabel for="relay-name">{m.editor_node_name()}</Field.FieldLabel>
		<Input id="relay-name" bind:value={name} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="relay-comment">{m.editor_node_comment()}</Field.FieldLabel>
		<Textarea id="relay-comment" bind:value={comment} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="relay-protocol">{m.editor_relay_protocol()}</Field.FieldLabel>
		<Select.Root
			type="single"
			value={protocol}
			disabled={!editable}
			onValueChange={next => (protocol = next as RelayProtocolName)}
		>
			<Select.Trigger id="relay-protocol">{protocolLabel(protocol)}</Select.Trigger>
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
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="relay-ip">{m.editor_override_ip()}</Field.FieldLabel>
		<Input id="relay-ip" bind:value={overrideIpAddress} disabled={!editable} />
		<Field.FieldDescription>{m.editor_override_none()}</Field.FieldDescription>
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="relay-port">{m.editor_override_port()}</Field.FieldLabel>
		<Input
			id="relay-port"
			type="number"
			min={0}
			max={65535}
			bind:value={overridePort}
			disabled={!editable}
		/>
	</Field.Field>
</Field.FieldGroup>

<Button class="mt-6 w-full" disabled={!editable || writes.pending} onclick={save}>
	{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
	{m.common_save()}
</Button>
