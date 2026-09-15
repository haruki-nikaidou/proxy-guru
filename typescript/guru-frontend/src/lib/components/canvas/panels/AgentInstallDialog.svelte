<script lang="ts">
import { toast } from 'svelte-sonner';
import CopyButton from '#lib/components/CopyButton.svelte';
import { issueServerAgentInstall } from '#lib/components/canvas/commands.js';
import * as Alert from '#lib/components/ui/alert/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Dialog from '#lib/components/ui/dialog/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import type { AgentInstallDto } from '#lib/dto/topology.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';

let {
	open = $bindable(false),
	canvasId,
	serverId,
	unit: initialUnit,
	replacing
}: {
	open?: boolean;
	canvasId: string;
	serverId: string;
	/** The stored instance name, or the one suggested from the server name. */
	unit: string;
	/** A key already exists: issuing replaces it and the running worker loses it. */
	replacing: boolean;
} = $props();

/** What the control plane accepts as `guru-worker@<unit>`. */
const UNIT_PATTERN = /^[a-z0-9][a-z0-9-]{0,31}$/;

let unit = $state('');
let pending = $state(false);
// The command carries the key, which is returned exactly once; it lives only in
// this component's state and is dropped when the dialog closes.
let install = $state<AgentInstallDto | null>(null);

$effect(() => {
	if (open) {
		unit = initialUnit;
	} else {
		install = null;
	}
});

const unitValid = $derived(UNIT_PATTERN.test(unit));

async function issue() {
	pending = true;
	try {
		install = await issueServerAgentInstall({ canvasId, serverId, unit });
	} catch (err) {
		const body = (err as { body?: App.Error }).body;
		toast.error(errorMessage(body?.code, body?.message ?? ''));
	} finally {
		pending = false;
	}
}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-2xl">
		<Dialog.Header>
			<Dialog.Title>{m.editor_agent_install_title()}</Dialog.Title>
			<Dialog.Description>{m.editor_agent_install_description()}</Dialog.Description>
		</Dialog.Header>

		{#if install}
			<Alert.Root variant="destructive">
				<Alert.Description>{m.editor_agent_install_once()}</Alert.Description>
			</Alert.Root>
			<div class="flex items-center gap-2 text-xs text-muted-foreground">
				<span class="font-mono">guru-worker@{install.unit} · v{install.version}</span>
				<CopyButton
					value={install.command}
					label={m.editor_agent_install_copy()}
					size="icon-xs"
					class="ms-auto shrink-0"
				/>
			</div>
			<pre
				class="max-h-72 overflow-auto rounded-md border bg-muted/40 p-3 font-mono text-xs">{install.command}</pre>
			<p class="text-xs text-muted-foreground">{m.editor_agent_install_after()}</p>
			<Dialog.Footer>
				<Button onclick={() => (open = false)}>{m.common_close()}</Button>
			</Dialog.Footer>
		{:else}
			{#if replacing}
				<Alert.Root variant="destructive">
					<Alert.Description>{m.editor_agent_install_replaces()}</Alert.Description>
				</Alert.Root>
			{/if}
			<Field.FieldGroup>
				<Field.Field data-invalid={!unitValid}>
					<Field.FieldLabel for="agent-unit">{m.editor_agent_unit()}</Field.FieldLabel>
					<Input id="agent-unit" bind:value={unit} aria-invalid={!unitValid} class="font-mono" />
					<Field.FieldDescription>{m.editor_agent_unit_hint()}</Field.FieldDescription>
					{#if !unitValid}
						<Field.FieldError>{m.issue_agent_unit_invalid()}</Field.FieldError>
					{/if}
				</Field.Field>
			</Field.FieldGroup>
			<Dialog.Footer class="mt-6">
				<Button type="button" variant="outline" onclick={() => (open = false)}>
					{m.common_cancel()}
				</Button>
				<Button type="button" disabled={pending || !unitValid} onclick={issue}>
					{#if pending}<Spinner data-icon="inline-start" />{/if}
					{m.editor_agent_install_generate()}
				</Button>
			</Dialog.Footer>
		{/if}
	</Dialog.Content>
</Dialog.Root>
