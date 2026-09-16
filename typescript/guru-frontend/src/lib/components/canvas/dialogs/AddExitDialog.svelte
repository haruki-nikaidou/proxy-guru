<script lang="ts">
import { newExit, putExit } from 'guru-graph';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Dialog from '#lib/components/ui/dialog/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import type { ProxyProtocolName } from '#lib/dto/topology.js';
import { PROXY_OPTIONS, proxyLabel, proxyVersion } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * A new exit: where traffic leaves the fabric. The destination is asked for
 * up front, because an exit that goes nowhere is an error the control plane
 * refuses to store.
 */
let {
	open = $bindable(false),
	place,
	suggest
}: { open?: boolean; place: () => { x: number; y: number }; suggest: () => string } = $props();

const editor = useEditor();
let name = $state('');
let destination = $state('');
let proxy = $state<ProxyProtocolName>('none');
let pending = $state(false);

let seeded = false;
$effect(() => {
	if (!open) {
		seeded = false;
		return;
	}
	if (seeded) return;
	seeded = true;
	name = suggest();
	destination = '';
	proxy = 'none';
});

async function submit() {
	pending = true;
	const exit = {
		...newExit(editor.canvasId, name.trim(), destination.trim(), place()),
		sendProxyProtocol: proxyVersion(proxy)
	};
	const written = await editor.commit(() => putExit(editor.graph, exit), m.editor_exit_created());
	pending = false;
	if (written) open = false;
}
</script>

<Dialog.Root bind:open>
	<Dialog.Content>
		<Dialog.Header>
			<Dialog.Title>{m.editor_exit_create_title()}</Dialog.Title>
			<Dialog.Description>{m.editor_exit_create_description()}</Dialog.Description>
		</Dialog.Header>
		<Field.FieldGroup>
			<Field.Field>
				<Field.FieldLabel for="exit-name">{m.editor_node_name()}</Field.FieldLabel>
				<Input id="exit-name" bind:value={name} />
			</Field.Field>
			<Field.Field>
				<Field.FieldLabel for="exit-destination">{m.editor_destination()}</Field.FieldLabel>
				<Input
					id="exit-destination"
					class="font-mono"
					placeholder="example.com:443"
					bind:value={destination}
				/>
				<Field.FieldDescription>{m.editor_destination_hint()}</Field.FieldDescription>
			</Field.Field>
			<Field.Field>
				<Field.FieldLabel for="exit-proxy">{m.editor_pass_proxy()}</Field.FieldLabel>
				<Select.Root type="single" value={proxy} onValueChange={next => (proxy = next as ProxyProtocolName)}>
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
		<Dialog.Footer class="mt-6">
			<Button variant="outline" onclick={() => (open = false)}>{m.common_cancel()}</Button>
			<Button
				disabled={pending || name.trim() === '' || destination.trim() === ''}
				onclick={submit}
			>
				{#if pending}<Spinner data-icon="inline-start" />{/if}
				{m.common_create()}
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
