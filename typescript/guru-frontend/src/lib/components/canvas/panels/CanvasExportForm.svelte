<script lang="ts">
import { seedOn } from '#lib/seed.svelte.js';
import { replaceExportSpec, updateNodeText } from '#lib/components/canvas/commands.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import type {
	CanvasExportAsName,
	CanvasExportNodeDto,
	ExportPortKindName
} from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';
import {
	EXPORT_DIRECTIONS,
	EXPORT_PORT_KINDS,
	exportAsLabel,
	exportKindLabel
} from '#lib/i18n/labels.js';

let {
	canvasId,
	node,
	editable
}: { canvasId: string; node: CanvasExportNodeDto; editable: boolean } = $props();

let name = $state('');
let comment = $state('');
let portKind = $state<ExportPortKindName>('derive_listen');
let exportAs = $state<CanvasExportAsName>('input_into_canvas');
const writes = panelWrites();

seedOn(
	() => node.id,
	() => {
		name = node.name;
		comment = node.comment;
		portKind = node.portKind;
		exportAs = node.exportAs;
	}
);

const reshapes = $derived(portKind !== node.portKind || exportAs !== node.exportAs);

async function save() {
	await writes.run(async () => {
		// The name is what the parent labels the mirrored port with, so it is
		// saved even when the shape is unchanged.
		await updateNodeText({ canvasId, nodeId: node.id, name, comment, boundary: true });
		if (reshapes) await replaceExportSpec({ canvasId, nodeId: node.id, portKind, exportAs });
	}, m.editor_saved());
}
</script>

<Field.FieldGroup>
	<Field.Field>
		<Field.FieldLabel for="export-name">{m.editor_node_name()}</Field.FieldLabel>
		<Input id="export-name" bind:value={name} disabled={!editable} />
		<Field.FieldDescription>{m.editor_export_name_hint()}</Field.FieldDescription>
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="export-comment">{m.editor_node_comment()}</Field.FieldLabel>
		<Textarea id="export-comment" bind:value={comment} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="export-kind">{m.editor_export_kind()}</Field.FieldLabel>
		<Select.Root
			type="single"
			value={portKind}
			disabled={!editable}
			onValueChange={next => (portKind = next as ExportPortKindName)}
		>
			<Select.Trigger id="export-kind">{exportKindLabel(portKind)}</Select.Trigger>
			<Select.Content>
				<Select.Group>
					{#each EXPORT_PORT_KINDS as option (option)}
						<Select.Item value={option} label={exportKindLabel(option)}>{exportKindLabel(option)}</Select.Item>
					{/each}
				</Select.Group>
			</Select.Content>
		</Select.Root>
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="export-direction">{m.editor_export_direction()}</Field.FieldLabel>
		<Select.Root
			type="single"
			value={exportAs}
			disabled={!editable}
			onValueChange={next => (exportAs = next as CanvasExportAsName)}
		>
			<Select.Trigger id="export-direction">{exportAsLabel(exportAs)}</Select.Trigger>
			<Select.Content>
				<Select.Group>
					{#each EXPORT_DIRECTIONS as option (option)}
						<Select.Item value={option} label={exportAsLabel(option)}>
							{exportAsLabel(option)}
						</Select.Item>
					{/each}
				</Select.Group>
			</Select.Content>
		</Select.Root>
		<Field.FieldDescription>{m.editor_export_direction_hint()}</Field.FieldDescription>
	</Field.Field>

	{#if reshapes}
		<Field.FieldDescription class="text-destructive">
			{m.editor_export_reshape_warning()}
		</Field.FieldDescription>
	{/if}
</Field.FieldGroup>

<Button class="mt-6 w-full" disabled={!editable || writes.pending} onclick={save}>
	{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
	{m.common_save()}
</Button>
