<script lang="ts">
import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
import { buttonVariants } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import type { CanvasOption, CanvasSummary } from '#lib/dto/canvas.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';
import { deleteCanvas } from './canvases.remote.js';

let {
	canvas,
	stats = null,
	open = $bindable(false),
	ondeleted
}: {
	canvas: CanvasOption;
	/** Shown when the caller has them; the settings page does not. */
	stats?: CanvasSummary['stats'];
	open?: boolean;
	ondeleted?: () => void;
} = $props();

let confirmName = $state('');
const writes = panelWrites();

$effect(() => {
	if (!open) confirmName = '';
});

async function confirm() {
	await writes.run(async () => {
		await deleteCanvas({ canvasId: canvas.id });
		open = false;
		ondeleted?.();
	}, m.canvas_deleted());
}
</script>

<AlertDialog.Root bind:open>
	<AlertDialog.Content>
		<AlertDialog.Header>
			<AlertDialog.Title>{m.canvas_delete_title()}</AlertDialog.Title>
			<AlertDialog.Description>
				{m.canvas_delete_warning({ name: canvas.name })}
			</AlertDialog.Description>
		</AlertDialog.Header>

		{#if stats}
			<p class="text-sm text-muted-foreground">
				{m.canvas_servers_count({ count: stats.servers })} ·
				{m.canvas_pods_count({ count: stats.pods })} ·
				{m.canvas_edges_count({ count: stats.edges })}
			</p>
		{/if}

		<Field.Field>
			<Field.FieldDescription>
				{m.canvas_delete_confirm_hint({ name: canvas.name })}
			</Field.FieldDescription>
			<Input bind:value={confirmName} autocomplete="off" />
		</Field.Field>

		<AlertDialog.Footer>
			<AlertDialog.Cancel>{m.common_cancel()}</AlertDialog.Cancel>
			<AlertDialog.Action
				class={buttonVariants({ variant: 'destructive' })}
				disabled={confirmName.trim() !== canvas.name || writes.pending}
				onclick={confirm}
			>
				{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
				{m.common_delete()}
			</AlertDialog.Action>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>
