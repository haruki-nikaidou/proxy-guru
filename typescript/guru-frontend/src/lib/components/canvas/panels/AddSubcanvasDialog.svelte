<script lang="ts">
import {
	createSubcanvas,
	importCanvas,
	listImportableCanvases
} from '#lib/components/canvas/commands.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Dialog from '#lib/components/ui/dialog/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';

/**
 * Attaches a subcanvas: either a brand-new canvas created together with the
 * import node that embeds it, or an existing root canvas from outside this
 * tree. `place` and `suggest` are evaluated when the dialog opens, so the node
 * lands in the middle of the view the operator was looking at.
 */
let {
	open = $bindable(false),
	mode,
	canvasId,
	place,
	suggest
}: {
	open?: boolean;
	mode: 'create' | 'import';
	canvasId: string;
	place: () => { x: number; y: number };
	suggest: () => string;
} = $props();

const importable = $derived(
	mode === 'import' && open ? listImportableCanvases({ canvasId }) : undefined
);

let name = $state('');
/** The last name this dialog filled in; an operator edit makes it stale. */
let autoName = $state('');
let targetCanvasId = $state('');
const writes = panelWrites();

// Seeded on every open: the suggested name is drawn against the names already
// on the canvas, which change between openings.
let seeded = $state(false);
$effect(() => {
	if (!open) {
		seeded = false;
		return;
	}
	if (seeded) return;
	seeded = true;
	name = suggest();
	autoName = name;
	targetCanvasId = '';
});

const target = $derived(importable?.current?.find(option => option.id === targetCanvasId));

// Picking a canvas to import names the node after it, unless the operator
// already typed a name of their own.
function pickTarget(id: string) {
	targetCanvasId = id;
	const picked = importable?.current?.find(option => option.id === id);
	if (!picked || (name !== autoName && name !== '')) return;
	name = picked.name;
	autoName = picked.name;
}

async function submit() {
	await writes.run(
		async () => {
			const { x, y } = place();
			if (mode === 'create') await createSubcanvas({ canvasId, name, x, y });
			else await importCanvas({ canvasId, targetCanvasId, name, x, y });
			open = false;
		},
		mode === 'create' ? m.editor_subcanvas_created() : m.editor_subcanvas_imported()
	);
}
</script>

<Dialog.Root bind:open>
	<Dialog.Content>
		<Dialog.Header>
			<Dialog.Title>
				{mode === 'create' ? m.editor_subcanvas_create_title() : m.editor_subcanvas_import_title()}
			</Dialog.Title>
			<Dialog.Description>
				{mode === 'create'
					? m.editor_subcanvas_create_description()
					: m.editor_subcanvas_import_description()}
			</Dialog.Description>
		</Dialog.Header>

		<Field.FieldGroup>
			{#if mode === 'import'}
				<Field.Field>
					<Field.FieldLabel for="subcanvas-target">{m.editor_subcanvas_target()}</Field.FieldLabel>
					<Select.Root
						type="single"
						value={targetCanvasId}
						onValueChange={pickTarget}
						disabled={importable?.current === undefined}
					>
						<Select.Trigger id="subcanvas-target">
							{target?.name ?? m.editor_subcanvas_pick()}
						</Select.Trigger>
						<Select.Content>
							<Select.Group>
								{#each importable?.current ?? [] as option (option.id)}
									<Select.Item value={option.id} label={option.name}>{option.name}</Select.Item>
								{/each}
							</Select.Group>
						</Select.Content>
					</Select.Root>
					{#if importable?.current?.length === 0}
						<Field.FieldDescription>{m.editor_subcanvas_none_importable()}</Field.FieldDescription>
					{/if}
				</Field.Field>
			{/if}

			<Field.Field>
				<Field.FieldLabel for="subcanvas-name">
					{mode === 'create' ? m.editor_subcanvas_name() : m.editor_node_name()}
				</Field.FieldLabel>
				<Input id="subcanvas-name" bind:value={name} />
				{#if mode === 'create'}
					<Field.FieldDescription>{m.editor_subcanvas_name_hint()}</Field.FieldDescription>
				{/if}
			</Field.Field>
		</Field.FieldGroup>

		<Dialog.Footer class="mt-6">
			<Button variant="outline" onclick={() => (open = false)}>{m.common_cancel()}</Button>
			<Button
				disabled={writes.pending || name.trim() === '' || (mode === 'import' && targetCanvasId === '')}
				onclick={submit}
			>
				{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
				{m.common_create()}
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
