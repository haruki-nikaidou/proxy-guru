<script lang="ts">
import CopyButton from '#lib/components/CopyButton.svelte';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import { Separator } from '#lib/components/ui/separator/index.js';
import type { ServerDto } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import ServerAgentSection from './ServerAgentSection.svelte';
import ServerPodsSection from './ServerPodsSection.svelte';
import ServerRolloutSection from './ServerRolloutSection.svelte';
import ServerSettingsForm from './ServerSettingsForm.svelte';

/**
 * The inspector for a server. It is four panels stacked: what the server is,
 * the pods that run on it, the worker running on it, and where it stands in a
 * rollout. Each section owns its own drafts and its own writes — they share
 * only the server they are pointed at.
 */
let { server }: { server: ServerDto } = $props();

const editor = useEditor();

// A rollout names the servers it waits for by id; the whole tree's servers can
// be resolved to a name here.
const serverNames = $derived(new Map(editor.graph.servers.map(entry => [entry.id, entry.name])));
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

<ServerSettingsForm {server} editable={editor.editable} />

<Separator class="my-6" />

<ServerPodsSection {server} />

<Separator class="my-6" />

<ServerAgentSection {server} editable={editor.editable} />

<Separator class="my-6" />

<ServerRolloutSection {server} admin={editor.admin} {serverNames} />
