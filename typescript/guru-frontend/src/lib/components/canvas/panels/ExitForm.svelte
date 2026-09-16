<script lang="ts">
import { untrack } from 'svelte';
import { replaceExitSpec, updateNodeText } from '#lib/components/canvas/commands.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import type { ExitNodeDto, ProxyProtocolName } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';
import { PROXY_OPTIONS, proxyLabel } from '#lib/i18n/labels.js';

let { canvasId, node, editable }: { canvasId: string; node: ExitNodeDto; editable: boolean } =
	$props();

let name = $state('');
let comment = $state('');
let destination = $state('');
let proxy = $state<ProxyProtocolName>('none');
const writes = panelWrites();

let seededFor = $state('');
$effect(() => {
	if (seededFor === node.id) return;
	seededFor = node.id;
	const snapshot = node;
	untrack(() => {
		name = snapshot.name;
		comment = snapshot.comment;
		destination = snapshot.destination;
		proxy = snapshot.passProxyProtocol;
	});
});

async function save() {
	await writes.run(async () => {
		await updateNodeText({ canvasId, nodeId: node.id, name, comment });
		await replaceExitSpec({
			canvasId,
			nodeId: node.id,
			destination,
			passProxyProtocol: proxy
		});
	}, m.editor_saved());
}
</script>

<Field.FieldGroup>
	<Field.Field>
		<Field.FieldLabel for="exit-name">{m.editor_node_name()}</Field.FieldLabel>
		<Input id="exit-name" bind:value={name} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="exit-comment">{m.editor_node_comment()}</Field.FieldLabel>
		<Textarea id="exit-comment" bind:value={comment} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="exit-destination">{m.editor_destination()}</Field.FieldLabel>
		<Input id="exit-destination" bind:value={destination} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="exit-proxy">{m.editor_pass_proxy()}</Field.FieldLabel>
		<Select.Root
			type="single"
			value={proxy}
			disabled={!editable}
			onValueChange={next => (proxy = next as ProxyProtocolName)}
		>
			<Select.Trigger id="exit-proxy">{proxyLabel(proxy)}</Select.Trigger>
			<Select.Content>
				<Select.Group>
					{#each PROXY_OPTIONS as option (option)}
						<Select.Item value={option} label={proxyLabel(option)}>{proxyLabel(option)}</Select.Item>
					{/each}
				</Select.Group>
			</Select.Content>
		</Select.Root>
	</Field.Field>
</Field.FieldGroup>

<Button class="mt-6 w-full" disabled={!editable || writes.pending} onclick={save}>
	{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
	{m.common_save()}
</Button>
