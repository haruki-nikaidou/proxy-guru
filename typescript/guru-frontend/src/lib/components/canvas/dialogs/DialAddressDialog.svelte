<script lang="ts">
import { type Edge, putEdge } from 'guru-graph';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Dialog from '#lib/components/ui/dialog/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { m } from '#lib/paraglide/messages.js';

/**
 * How one edge dials the pod it leads to: the pod's own address and port
 * unless overridden here — a second address of the same host, say, which is
 * what parallel edges between two pods are for. Exits are dialed as they say.
 */
let { edge = $bindable(null) }: { edge?: Edge | null } = $props();

const editor = useEditor();
let address = $state('');
// `Input` renders a dynamic `type`, so Svelte never coerces this to a number.
let port = $state('');
let pending = $state(false);

let seeded: Edge | null = null;
$effect(() => {
	if (edge === seeded) return;
	seeded = edge;
	if (!edge) return;
	address = edge.overrideIp ?? '';
	port = edge.overridePort === null ? '' : String(edge.overridePort);
});

const portValue = $derived(port.trim() === '' ? null : Number(port));
const portValid = $derived(
	portValue === null || (Number.isInteger(portValue) && portValue >= 1 && portValue <= 65535)
);

async function save() {
	const current = edge;
	if (!current) return;
	pending = true;
	const next: Edge = {
		...current,
		overrideIp: address.trim() === '' ? null : address.trim(),
		overridePort: portValue
	};
	const written = await editor.commit(() => putEdge(editor.graph, next), m.editor_saved());
	pending = false;
	if (written) edge = null;
}
</script>

<Dialog.Root
	open={edge !== null}
	onOpenChange={open => {
		if (!open) edge = null;
	}}
>
	<Dialog.Content>
		<Dialog.Header>
			<Dialog.Title>{m.editor_dial_title()}</Dialog.Title>
			<Dialog.Description>{m.editor_dial_description()}</Dialog.Description>
		</Dialog.Header>
		<Field.FieldGroup>
			<Field.Field>
				<Field.FieldLabel for="dial-address">{m.editor_override_ip()}</Field.FieldLabel>
				<Input id="dial-address" class="font-mono" bind:value={address} />
				<Field.FieldDescription>{m.editor_override_none()}</Field.FieldDescription>
			</Field.Field>
			<Field.Field>
				<Field.FieldLabel for="dial-port">{m.editor_override_port()}</Field.FieldLabel>
				<Input
					id="dial-port"
					type="number"
					min={1}
					max={65535}
					bind:value={port}
					aria-invalid={!portValid}
				/>
			</Field.Field>
		</Field.FieldGroup>
		<Dialog.Footer class="mt-6">
			<Button variant="outline" onclick={() => (edge = null)}>{m.common_cancel()}</Button>
			<Button disabled={pending || !portValid} onclick={save}>
				{#if pending}<Spinner data-icon="inline-start" />{/if}
				{m.common_save()}
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
