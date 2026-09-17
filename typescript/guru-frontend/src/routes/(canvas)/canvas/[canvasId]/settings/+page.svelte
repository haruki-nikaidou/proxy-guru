<script lang="ts">
import { seedOn } from '#lib/seed.svelte.js';
import { toast } from 'svelte-sonner';
import { goto } from '$app/navigation';
import { page } from '$app/state';
import CopyButton from '#lib/components/CopyButton.svelte';
import { Button } from '#lib/components/ui/button/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import * as InputGroup from '#lib/components/ui/input-group/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import { errorMessage, issueMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';
import { canEditWorkspace } from '#lib/permissions.js';
import BoundaryError from '#lib/components/BoundaryError.svelte';
import DeleteCanvasDialog from '../../../../(home)/DeleteCanvasDialog.svelte';
import { listCanvasOptions, updateCanvas } from '../../../../(home)/canvases.remote.js';
import type { PageProps } from './$types.js';

let { data }: PageProps = $props();

const canvasId = $derived(page.params.canvasId ?? '');
const editable = $derived(canEditWorkspace(data.identity.role));
const options = listCanvasOptions();
const canvas = $derived(options.current?.find(option => option.id === canvasId));

const form = $derived(updateCanvas.for(canvasId));
let deleteOpen = $state(false);

seedOn(
	() => canvas?.id ?? null,
	() => {
		form.fields.name.set(canvas?.name ?? '');
		form.fields.description.set(canvas?.description ?? '');
	}
);

$effect(() => {
	if (form.result?.ok) toast.success(m.canvas_updated());
});
</script>

<div class="flex flex-1 flex-col gap-6 p-4 md:p-8">
	<div>
		<h1 class="text-2xl font-semibold">{m.settings_title()}</h1>
		<p class="text-sm text-muted-foreground">{m.settings_description()}</p>
	</div>

	<svelte:boundary>
		{#if options.current === undefined && options.error}
			<BoundaryError error={options.error} retry />
		{:else if options.current === undefined}
			<Skeleton class="h-64 w-full max-w-2xl rounded-xl" />
		{:else if !canvas}
			<Empty.Root>
				<Empty.Header>
					<Empty.Title>{m.error_title()}</Empty.Title>
					<Empty.Description>{m.error_not_found()}</Empty.Description>
				</Empty.Header>
			</Empty.Root>
		{:else}
			{@const nameIssues = form.fields.name.issues()}
			{@const descriptionIssues = form.fields.description.issues()}
			<Card.Root class="max-w-2xl">
				<Card.Header>
					<Card.Title>{m.canvas_edit_title()}</Card.Title>
					<Card.Description>{m.canvas_edit_description()}</Card.Description>
				</Card.Header>
				<form {...form}>
					<input {...form.fields.canvasId.as('hidden', canvasId)} />
					<Card.Content>
						<Field.FieldGroup>
							<Field.Field>
								<Field.FieldLabel for="canvas-id">{m.canvas_id()}</Field.FieldLabel>
								<InputGroup.Root>
									<InputGroup.Input id="canvas-id" readonly value={canvas.id} class="font-mono" />
									<InputGroup.Addon align="inline-end">
										<CopyButton value={canvas.id} label={m.canvas_id_copy()} size="icon-sm" />
									</InputGroup.Addon>
								</InputGroup.Root>
							</Field.Field>

							<Field.Field data-invalid={nameIssues !== undefined} data-disabled={!editable}>
								<Field.FieldLabel for="canvas-name">{m.canvas_name()}</Field.FieldLabel>
								<Input
									id="canvas-name"
									{...form.fields.name.as('text')}
									aria-invalid={nameIssues !== undefined}
									disabled={!editable}
								/>
								{#each nameIssues ?? [] as issue (issue.message)}
									<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
								{/each}
							</Field.Field>

							<Field.Field
								data-invalid={descriptionIssues !== undefined}
								data-disabled={!editable}
							>
								<Field.FieldLabel for="canvas-description">{m.canvas_description()}</Field.FieldLabel>
								<Textarea
									id="canvas-description"
									{...form.fields.description.as('text')}
									aria-invalid={descriptionIssues !== undefined}
									disabled={!editable}
								/>
								{#each descriptionIssues ?? [] as issue (issue.message)}
									<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
								{/each}
							</Field.Field>
						</Field.FieldGroup>
					</Card.Content>
					{#if editable}
						<Card.Footer class="justify-end">
							<Button type="submit" disabled={form.pending > 0}>
								{#if form.pending > 0}<Spinner data-icon="inline-start" />{/if}
								{m.common_save()}
							</Button>
						</Card.Footer>
					{/if}
				</form>
			</Card.Root>

			{#if editable}
				<Card.Root class="max-w-2xl border-destructive/40">
					<Card.Header>
						<Card.Title>{m.settings_danger_title()}</Card.Title>
						<Card.Description>{m.settings_danger_description()}</Card.Description>
					</Card.Header>
					<Card.Footer class="justify-end">
						<Button variant="destructive" onclick={() => (deleteOpen = true)}>
							{m.canvas_delete()}
						</Button>
					</Card.Footer>
				</Card.Root>
				<DeleteCanvasDialog {canvas} bind:open={deleteOpen} ondeleted={() => goto('/')} />
			{/if}
		{/if}

		{#snippet failed(error, reset)}
			<BoundaryError {error} {reset} />
		{/snippet}
	</svelte:boundary>
</div>
