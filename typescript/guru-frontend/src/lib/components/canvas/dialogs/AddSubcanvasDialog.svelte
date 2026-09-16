<script lang="ts">
import { createSubcanvas } from '#lib/components/canvas/commands.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Dialog from '#lib/components/ui/dialog/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';

/**
 * A new canvas drawn inside this one. `place` and `suggest` are evaluated when
 * the dialog opens, so the card lands in the middle of the view the operator
 * was looking at.
 */
let {
	open = $bindable(false),
	canvasId,
	place,
	suggest
}: {
	open?: boolean;
	canvasId: string;
	place: () => { x: number; y: number };
	suggest: () => string;
} = $props();

let name = $state('');
const writes = panelWrites();

let seeded = false;
$effect(() => {
	if (!open) {
		seeded = false;
		return;
	}
	if (seeded) return;
	seeded = true;
	name = suggest();
});

const submit = () =>
	writes.run(async () => {
		const { x, y } = place();
		await createSubcanvas({ canvasId, name, x, y });
		open = false;
	}, m.editor_subcanvas_created());
</script>

<Dialog.Root bind:open>
	<Dialog.Content>
		<Dialog.Header>
			<Dialog.Title>{m.editor_subcanvas_create_title()}</Dialog.Title>
			<Dialog.Description>{m.editor_subcanvas_create_description()}</Dialog.Description>
		</Dialog.Header>
		<Field.Field>
			<Field.FieldLabel for="subcanvas-name">{m.editor_subcanvas_name()}</Field.FieldLabel>
			<Input id="subcanvas-name" bind:value={name} />
		</Field.Field>
		<Dialog.Footer class="mt-6">
			<Button variant="outline" onclick={() => (open = false)}>{m.common_cancel()}</Button>
			<Button disabled={writes.pending || name.trim() === ''} onclick={submit}>
				{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
				{m.common_create()}
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
