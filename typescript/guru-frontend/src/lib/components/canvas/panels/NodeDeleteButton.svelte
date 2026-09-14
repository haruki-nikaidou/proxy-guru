<script lang="ts">
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import { toast } from 'svelte-sonner';
import { deleteNode } from '#lib/components/canvas/commands.js';
import { Button } from '#lib/components/ui/button/index.js';
import type { StandaloneNode } from '#lib/dto/topology.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';
import ConfirmDeleteDialog from './ConfirmDeleteDialog.svelte';

/**
 * The delete affordance every standalone node panel shares. It asks first, then
 * retires the node the same way the Delete key does; the control plane still
 * refuses a delete that would break the topology, and the panel closes on its
 * own once the refreshed graph no longer holds the node.
 */
let { canvasId, node, editable }: { canvasId: string; node: StandaloneNode; editable: boolean } =
	$props();

let open = $state(false);
let pending = $state(false);

async function remove() {
	pending = true;
	try {
		await deleteNode({
			canvasId,
			nodeId: node.id,
			force: false,
			// Retiring an import hands its target back to the root listing; retiring
			// an export takes its mirrored port off the parent's import node.
			subcanvasTarget: node.kind === 'canvas_import' ? node.targetCanvasId : '',
			boundary: node.kind === 'canvas_export'
		});
		open = false;
		toast.success(m.editor_deleted());
	} catch (err) {
		const body = (err as { body?: App.Error }).body;
		toast.error(errorMessage(body?.code, body?.message ?? ''));
	} finally {
		pending = false;
	}
}
</script>

<Button class="mt-2 w-full" variant="outline" disabled={!editable || pending} onclick={() => (open = true)}>
	<Trash2Icon />
	{m.editor_node_delete()}
</Button>

<ConfirmDeleteDialog
	bind:open
	title={m.editor_node_delete()}
	description={m.editor_node_delete_description({ name: node.name })}
	{pending}
	onconfirm={remove}
/>
