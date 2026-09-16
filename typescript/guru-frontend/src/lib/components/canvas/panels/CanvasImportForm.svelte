<script lang="ts">
import ArrowRightIcon from '@lucide/svelte/icons/arrow-right';
import { seedOn } from '#lib/seed.svelte.js';
import { goto } from '$app/navigation';
import { updateNodeText } from '#lib/components/canvas/commands.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import type { CanvasImportNodeDto } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';

// The embedded canvas is immutable: the control plane refuses a spec replace on
// an import node, so the only edits here are the name and the comment. Changing
// what is embedded means retiring this node and importing again.
let {
	canvasId,
	node,
	editable
}: { canvasId: string; node: CanvasImportNodeDto; editable: boolean } = $props();

let name = $state('');
let comment = $state('');
const writes = panelWrites();

seedOn(
	() => node.id,
	() => {
		name = node.name;
		comment = node.comment;
	}
);

async function save() {
	await writes.run(async () => {
		await updateNodeText({ canvasId, nodeId: node.id, name, comment });
	}, m.editor_saved());
}
</script>

<Field.FieldGroup>
	<Field.Field>
		<Field.FieldLabel for="import-name">{m.editor_node_name()}</Field.FieldLabel>
		<Input id="import-name" bind:value={name} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="import-comment">{m.editor_node_comment()}</Field.FieldLabel>
		<Textarea id="import-comment" bind:value={comment} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="import-target">{m.editor_subcanvas_target()}</Field.FieldLabel>
		<Input
			id="import-target"
			readonly
			value={node.targetName || m.editor_subcanvas_missing()}
		/>
		<Field.FieldDescription>{m.editor_subcanvas_target_fixed()}</Field.FieldDescription>
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="import-ports">{m.editor_subcanvas_ports()}</Field.FieldLabel>
		{#if node.ports.length === 0}
			<p id="import-ports" class="text-sm text-muted-foreground">
				{m.editor_subcanvas_no_ports()}
			</p>
		{:else}
			<ul id="import-ports" class="grid gap-1 text-sm">
				{#each node.ports as port (port.id)}
					<li class="flex items-center justify-between gap-2">
						<span class="truncate">{port.label ?? port.key}</span>
						<span class="shrink-0 text-xs text-muted-foreground">
							{port.kind === 'derive_listen'
								? m.editor_port_listen()
								: m.editor_port_destination()} ·
							{port.direction === 'input' ? m.editor_port_input() : m.editor_port_output()}
						</span>
					</li>
				{/each}
			</ul>
		{/if}
		<Field.FieldDescription>{m.editor_subcanvas_ports_hint()}</Field.FieldDescription>
	</Field.Field>
</Field.FieldGroup>

<Button
	class="mt-6 w-full"
	variant="secondary"
	disabled={!node.targetCanvasId}
	onclick={() => goto(`/canvas/${node.targetCanvasId}`)}
>
	{m.editor_subcanvas_open()}
	<ArrowRightIcon />
</Button>

<Button class="mt-2 w-full" disabled={!editable || writes.pending} onclick={save}>
	{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
	{m.common_save()}
</Button>
