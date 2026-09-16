<script lang="ts">
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import { disconnectEdge } from '#lib/components/canvas/commands.js';
import type { EdgeDetail } from '#lib/components/canvas/graph.js';
import { Button } from '#lib/components/ui/button/index.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';
import ConfirmDeleteDialog from '#lib/components/ConfirmDeleteDialog.svelte';

/**
 * The delete affordance of the edge panel. It asks first, then disconnects the
 * edge the same way the Delete key does; the control plane still refuses a cut
 * that would break the topology, and the panel closes on its own once the
 * refreshed graph no longer holds the edge.
 */
let { canvasId, detail, editable }: { canvasId: string; detail: EdgeDetail; editable: boolean } =
	$props();

let open = $state(false);
const writes = panelWrites();

async function remove() {
	await writes.run(async () => {
		await disconnectEdge({ canvasId, edgeId: detail.id, force: false });
		open = false;
	}, m.editor_disconnected());
}
</script>

<Button class="mt-2 w-full" variant="outline" disabled={!editable || writes.pending} onclick={() => (open = true)}>
	<Trash2Icon />
	{m.editor_edge_delete()}
</Button>

<ConfirmDeleteDialog
	bind:open
	title={m.editor_edge_delete()}
	description={m.editor_edge_delete_description({ from: detail.source.node, to: detail.target.node })}
	pending={writes.pending}
	onconfirm={remove}
/>
