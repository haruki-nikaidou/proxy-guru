<script lang="ts">
import ArrowRightIcon from '@lucide/svelte/icons/arrow-right';
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import { type Canvas, subtreeOf } from 'guru-graph';
import ConfirmDeleteDialog from '#lib/components/ConfirmDeleteDialog.svelte';
import { deleteSubcanvas, updateSubcanvas } from '#lib/components/canvas/commands.js';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import { m } from '#lib/paraglide/messages.js';
import { seedOn } from '#lib/seed.svelte.js';
import { panelWrites } from '#lib/writes.svelte.js';

/** A canvas drawn inside this one: its name, what is in it, and the way in. */
let { canvas }: { canvas: Canvas } = $props();

const editor = useEditor();
const writes = panelWrites();
let name = $state('');
let description = $state('');
let deleteOpen = $state(false);

seedOn(
	() => canvas.id,
	() => {
		name = canvas.name;
		description = canvas.description;
	}
);

const inside = $derived(subtreeOf(editor.graph, canvas.id));
const counts = $derived({
	servers: editor.graph.servers.filter(server => inside.has(server.canvasId)).length,
	pods: editor.graph.pods.filter(pod => inside.has(pod.canvasId)).length
});

const save = () =>
	writes.run(
		() =>
			updateSubcanvas({
				canvasId: editor.canvasId,
				subcanvasId: canvas.id,
				name,
				description
			}),
		m.editor_saved()
	);

const remove = () =>
	writes.run(async () => {
		await deleteSubcanvas({ canvasId: editor.canvasId, subcanvasId: canvas.id });
		deleteOpen = false;
	}, m.editor_deleted());
</script>

<Button class="w-full" variant="secondary" href="/canvas/{canvas.id}">
	{m.editor_subcanvas_open()}
	<ArrowRightIcon />
</Button>
<p class="mt-2 text-xs text-muted-foreground">
	{m.canvas_servers_count({ count: counts.servers })} · {m.canvas_pods_count({ count: counts.pods })}
</p>

<Field.FieldGroup class="mt-6">
	<Field.Field>
		<Field.FieldLabel for="subcanvas-name">{m.canvas_name()}</Field.FieldLabel>
		<Input id="subcanvas-name" bind:value={name} disabled={!editor.editable} />
	</Field.Field>
	<Field.Field>
		<Field.FieldLabel for="subcanvas-description">{m.canvas_description()}</Field.FieldLabel>
		<Textarea id="subcanvas-description" bind:value={description} disabled={!editor.editable} />
	</Field.Field>
</Field.FieldGroup>

{#if editor.editable}
	<Button class="mt-6 w-full" disabled={writes.pending || name.trim() === ''} onclick={save}>
		{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
		{m.common_save()}
	</Button>
	<Button class="mt-2 w-full" variant="outline" disabled={writes.pending} onclick={() => (deleteOpen = true)}>
		<Trash2Icon />
		{m.canvas_delete_title()}
	</Button>
	<ConfirmDeleteDialog
		bind:open={deleteOpen}
		title={m.canvas_delete_title()}
		description={m.canvas_delete_warning({ name: canvas.name })}
		pending={writes.pending}
		onconfirm={remove}
	/>
{/if}
