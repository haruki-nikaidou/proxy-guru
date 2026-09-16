<script lang="ts">
import { seedOn } from '#lib/seed.svelte.js';
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
import { RELAY_PROTOCOLS, relayProtocolLabel } from '#lib/i18n/labels.js';

let { canvasId, node, editable }: { canvasId: string; node: RelayNodeDto; editable: boolean } =
	$props();

let name = $state('');
let comment = $state('');
let protocol = $state<RelayProtocolName>('tcp_raw');
let overrideIpAddress = $state('');
// `Input` renders a dynamic `type`, so Svelte never coerces this to a number.
let overridePort = $state('0');
const writes = panelWrites();

seedOn(
	() => node.id,
	() => {
		name = node.name;
		comment = node.comment;
		protocol = node.protocol;
		overrideIpAddress = node.overrideIpAddress;
		overridePort = String(node.overridePort);
	}
);

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
			<Select.Trigger id="relay-protocol">{relayProtocolLabel(protocol)}</Select.Trigger>
			<Select.Content>
				<Select.Group>
					{#each RELAY_PROTOCOLS as option (option)}
						<Select.Item value={option} label={relayProtocolLabel(option)}>
							{relayProtocolLabel(option)}
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
