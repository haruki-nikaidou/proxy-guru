<script lang="ts">
import CopyButton from '#lib/components/CopyButton.svelte';
import { Separator } from '#lib/components/ui/separator/index.js';
import type { ServerDto } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import ServerAgentSection from './ServerAgentSection.svelte';
import ServerPodsSection from './ServerPodsSection.svelte';
import ServerRolloutSection from './ServerRolloutSection.svelte';
import ServerSettingsForm from './ServerSettingsForm.svelte';

/**
 * The inspector for a server node. It is four panels stacked: what the server
 * is, what it carries, the worker running on it, and where it stands in a
 * rollout. Each section owns its own drafts and its own writes — they share
 * only the server they are pointed at.
 */
let {
	canvasId,
	server,
	editable,
	admin,
	serverNames
}: {
	canvasId: string;
	server: ServerDto;
	editable: boolean;
	/** `ForgetServerApplied` is admin-only in the control plane. */
	admin: boolean;
	/** Server id → name, for the ids a rollout says it is waiting for. */
	serverNames: ReadonlyMap<string, string>;
} = $props();
</script>

<div class="mb-4 flex items-center gap-1 text-xs text-muted-foreground">
	<span>{m.editor_server_id()}</span>
	<span class="truncate font-mono">{server.id}</span>
	<CopyButton
		value={server.id}
		label={m.editor_server_id_copy()}
		size="icon-xs"
		class="shrink-0"
	/>
</div>

<ServerSettingsForm {canvasId} {server} {editable} />

<Separator class="my-6" />

<ServerPodsSection {canvasId} {server} {editable} />

<Separator class="my-6" />

<ServerAgentSection {canvasId} {server} {editable} />

<Separator class="my-6" />

<ServerRolloutSection {canvasId} {server} {admin} {serverNames} />
