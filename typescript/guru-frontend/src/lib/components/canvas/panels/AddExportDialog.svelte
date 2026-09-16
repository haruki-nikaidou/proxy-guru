<script lang="ts">
import { createExportNode } from '#lib/components/canvas/commands.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Dialog from '#lib/components/ui/dialog/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import type { CanvasExportAsName, ExportPortKindName } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';
import {
	EXPORT_DIRECTIONS,
	EXPORT_PORT_KINDS,
	exportAsLabel,
	exportKindLabel
} from '#lib/i18n/labels.js';

/**
 * Adds a boundary port to this canvas. Kind and direction are asked up front
 * because changing them later reshapes the mirrored port on the parent's import
 * node, which drops whatever edge was attached to it.
 */
let {
	open = $bindable(false),
	canvasId,
	place,
	suggest
}: {
	open?: boolean;
	canvasId: string;
	place: () => { x: number; y: number };
	suggest: () => string;
} = $props();

let name = $state('');
let portKind = $state<ExportPortKindName>('derive_listen');
let exportAs = $state<CanvasExportAsName>('input_into_canvas');
const writes = panelWrites();

let seeded = $state(false);
$effect(() => {
	if (!open) {
		seeded = false;
		return;
	}
	if (seeded) return;
	seeded = true;
	name = suggest();
});

async function submit() {
	await writes.run(async () => {
		const { x, y } = place();
		await createExportNode({ canvasId, name, portKind, exportAs, x, y });
		open = false;
	}, m.editor_export_created());
}
</script>

<Dialog.Root bind:open>
	<Dialog.Content>
		<Dialog.Header>
			<Dialog.Title>{m.editor_export_create_title()}</Dialog.Title>
			<Dialog.Description>{m.editor_export_create_description()}</Dialog.Description>
		</Dialog.Header>

		<Field.FieldGroup>
			<Field.Field>
				<Field.FieldLabel for="add-export-name">{m.editor_node_name()}</Field.FieldLabel>
				<Input id="add-export-name" bind:value={name} />
				<Field.FieldDescription>{m.editor_export_name_hint()}</Field.FieldDescription>
			</Field.Field>

			<Field.Field>
				<Field.FieldLabel for="add-export-kind">{m.editor_export_kind()}</Field.FieldLabel>
				<Select.Root
					type="single"
					value={portKind}
					onValueChange={next => (portKind = next as ExportPortKindName)}
				>
					<Select.Trigger id="add-export-kind">{exportKindLabel(portKind)}</Select.Trigger>
					<Select.Content>
						<Select.Group>
							{#each EXPORT_PORT_KINDS as option (option)}
								<Select.Item value={option} label={exportKindLabel(option)}>
									{exportKindLabel(option)}
								</Select.Item>
							{/each}
						</Select.Group>
					</Select.Content>
				</Select.Root>
			</Field.Field>

			<Field.Field>
				<Field.FieldLabel for="add-export-direction">
					{m.editor_export_direction()}
				</Field.FieldLabel>
				<Select.Root
					type="single"
					value={exportAs}
					onValueChange={next => (exportAs = next as CanvasExportAsName)}
				>
					<Select.Trigger id="add-export-direction">{exportAsLabel(exportAs)}</Select.Trigger>
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
		</Field.FieldGroup>

		<Dialog.Footer class="mt-6">
			<Button variant="outline" onclick={() => (open = false)}>{m.common_cancel()}</Button>
			<Button disabled={writes.pending || name.trim() === ''} onclick={submit}>
				{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
				{m.common_create()}
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
