<script lang="ts">
import { seedOn } from '#lib/seed.svelte.js';
import { toast } from 'svelte-sonner';
import { Button } from '#lib/components/ui/button/index.js';
import * as Dialog from '#lib/components/ui/dialog/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import type { CanvasSummary } from '#lib/dto/canvas.js';
import { issueMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';
import { createCanvas, updateCanvas } from './canvases.remote.js';

let {
	mode,
	canvas,
	open = $bindable(false)
}: { mode: 'create' | 'edit'; canvas?: CanvasSummary; open?: boolean } = $props();

type CanvasForm = typeof createCanvas | ReturnType<typeof updateCanvas.for>;

// `.for(id)` keeps every row-level edit form independent of the others.
const form: CanvasForm = $derived(
	mode === 'edit' && canvas ? updateCanvas.for(canvas.id) : createCanvas
);

// Seeded once per open, so a reopen on the same canvas seeds it again.
seedOn(
	() => (open && mode === 'edit' && canvas ? canvas.id : null),
	() => {
		form.fields.name.set(canvas?.name ?? '');
		form.fields.description.set(canvas?.description ?? '');
	}
);

$effect(() => {
	if (!form.result?.ok) return;
	open = false;
	toast.success(mode === 'edit' ? m.canvas_updated() : m.canvas_created());
});
</script>

{#snippet body(current: CanvasForm)}
	{@const nameIssues = current.fields.name.issues()}
	{@const descriptionIssues = current.fields.description.issues()}
	<Field.FieldGroup>
		<Field.Field data-invalid={nameIssues !== undefined}>
			<Field.FieldLabel for="canvas-name">{m.canvas_name()}</Field.FieldLabel>
			<Input
				id="canvas-name"
				{...current.fields.name.as('text')}
				aria-invalid={nameIssues !== undefined}
			/>
			{#each nameIssues ?? [] as issue (issue.message)}
				<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
			{/each}
		</Field.Field>

		<Field.Field data-invalid={descriptionIssues !== undefined}>
			<Field.FieldLabel for="canvas-description">{m.canvas_description()}</Field.FieldLabel>
			<Textarea
				id="canvas-description"
				{...current.fields.description.as('text')}
				aria-invalid={descriptionIssues !== undefined}
			/>
			{#each descriptionIssues ?? [] as issue (issue.message)}
				<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
			{/each}
		</Field.Field>
	</Field.FieldGroup>

	<Dialog.Footer class="mt-6">
		<Button type="button" variant="outline" onclick={() => (open = false)}>
			{m.common_cancel()}
		</Button>
		<Button type="submit" disabled={current.pending > 0}>
			{#if current.pending > 0}<Spinner data-icon="inline-start" />{/if}
			{mode === 'edit' ? m.common_save() : m.common_create()}
		</Button>
	</Dialog.Footer>
{/snippet}

<Dialog.Root bind:open>
	<Dialog.Content>
		<Dialog.Header>
			<Dialog.Title>{mode === 'edit' ? m.canvas_edit_title() : m.canvas_create_title()}</Dialog.Title>
			<Dialog.Description>
				{mode === 'edit' ? m.canvas_edit_description() : m.canvas_create_description()}
			</Dialog.Description>
		</Dialog.Header>

		{#if mode === 'edit' && canvas}
			{@const edit = updateCanvas.for(canvas.id)}
			<form {...edit}>
				<input {...edit.fields.canvasId.as('hidden', canvas.id)} />
				{@render body(edit)}
			</form>
		{:else}
			<form {...createCanvas}>
				{@render body(createCanvas)}
			</form>
		{/if}
	</Dialog.Content>
</Dialog.Root>
