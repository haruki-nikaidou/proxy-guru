<script lang="ts">
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import { toast } from 'svelte-sonner';
import { disconnectEdge } from '#lib/components/canvas/commands.js';
import type { EdgeDetail } from '#lib/components/canvas/graph.js';
import { Button } from '#lib/components/ui/button/index.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';
import ConfirmDeleteDialog from './ConfirmDeleteDialog.svelte';

/**
 * The delete affordance of the edge panel. It asks first, then disconnects the
 * edge the same way the Delete key does; the control plane still refuses a cut
 * that would break the topology, and the panel closes on its own once the
 * refreshed graph no longer holds the edge.
 */
let { canvasId, detail, editable }: { canvasId: string; detail: EdgeDetail; editable: boolean } =
	$props();

let open = $state(false);
let pending = $state(false);

async function remove() {
	pending = true;
	try {
		await disconnectEdge({ canvasId, edgeId: detail.id, force: false });
		open = false;
		toast.success(m.editor_disconnected());
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
	{m.editor_edge_delete()}
</Button>

<ConfirmDeleteDialog
	bind:open
	title={m.editor_edge_delete()}
	description={m.editor_edge_delete_description({ from: detail.source.node, to: detail.target.node })}
	{pending}
	onconfirm={remove}
/>
