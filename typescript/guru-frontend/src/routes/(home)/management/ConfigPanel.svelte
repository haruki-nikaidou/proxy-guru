<script lang="ts">
import InfoIcon from '@lucide/svelte/icons/info';
import RotateCcwIcon from '@lucide/svelte/icons/rotate-ccw';
import SaveIcon from '@lucide/svelte/icons/save';
import Undo2Icon from '@lucide/svelte/icons/undo-2';
import { toast } from 'svelte-sonner';
import CopyButton from '#lib/components/CopyButton.svelte';
import JsonDocumentEditor from '#lib/components/JsonDocumentEditor.svelte';
import * as Alert from '#lib/components/ui/alert/index.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import * as ToggleGroup from '#lib/components/ui/toggle-group/index.js';
import type { ConfigKeyName } from '#lib/dto/config.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';
import { listConfigDocuments, saveConfigDocument } from './config.remote.js';

const documents = listConfigDocuments();

// A two-item `ToggleGroup` rather than nested `Tabs`: this panel already sits
// inside the management tab strip, so a second strip would read as another
// level of page navigation, and each document needs the full width and a tall
// editor — stacking both cards would bury the footer actions and mount two
// editors for a document nobody is looking at.
let selected = $state<ConfigKeyName>('auth');

/**
 * Drafts and rejections live here, keyed by config key, rather than inside the
 * editor: only the selected document is mounted, so an unsaved edit would be
 * lost on every switch otherwise.
 */
let drafts = $state<Partial<Record<ConfigKeyName, string>>>({});
let rejections = $state<Partial<Record<ConfigKeyName, string>>>({});
/** The key whose save is in flight, so the spinner cannot follow a switch. */
let saving = $state<ConfigKeyName | null>(null);

const docs = $derived(documents.current);
const active = $derived(docs?.find(entry => entry.key === selected));
const text = $derived(drafts[selected] ?? active?.json ?? '');
/**
 * An unseeded key has no row, and its `json` is the defaults it would be
 * created with — identical text, so a plain text comparison would leave `Save`
 * disabled on exactly the document the panel says saving will create. Until the
 * row exists, saving is always available.
 */
const dirty = $derived(active !== undefined && (!active.stored || text !== active.json));
/** `undefined` while the text parses; otherwise the parser's own complaint. */
const syntaxError = $derived.by(() => {
	try {
		JSON.parse(text);
		return undefined;
	} catch (err) {
		return (err as Error).message;
	}
});

async function save() {
	if (!active) return;
	const key = active.key;
	saving = key;
	try {
		const fresh = await saveConfigDocument({ key, json: text });
		// The control plane's fresh read is authoritative — it is pretty-printed
		// from the row it just wrote, so the draft tracks it exactly.
		drafts[key] = fresh.json;
		rejections[key] = undefined;
		toast.success(m.config_saved());
	} catch (err) {
		// A rejected payload is shown next to the editor and the operator's text
		// is left alone: refetching here would throw away the edit they have to
		// fix. `server_message` carries the control plane's own English text.
		const body = (err as { body?: App.Error }).body;
		rejections[key] = errorMessage(body?.code, body?.message ?? '');
	} finally {
		saving = null;
	}
}
</script>

{#if docs === undefined}
	<Skeleton class="h-128 w-full" />
{:else if active !== undefined}
	<Card.Root>
		<Card.Header>
			<Card.Title class="flex items-center gap-2">
				<code>{active.key}</code>
				{#if dirty}
					<Badge variant="secondary">{m.config_unsaved()}</Badge>
				{/if}
			</Card.Title>
			<Card.Description>
				{active.key === 'auth' ? m.config_auth_description() : m.config_orchestration_description()}
			</Card.Description>
			<Card.Action class="flex items-center gap-2">
				<ToggleGroup.Root
					type="single"
					variant="outline"
					size="sm"
					value={selected}
					onValueChange={(next) => {
						if (next === 'auth' || next === 'orchestration') selected = next;
					}}
					aria-label={m.config_key_label()}
				>
					{#each docs as entry (entry.key)}
						<ToggleGroup.Item value={entry.key}>{entry.key}</ToggleGroup.Item>
					{/each}
				</ToggleGroup.Root>
				<CopyButton value={text} label={m.config_copy()} />
			</Card.Action>
		</Card.Header>

		<Card.Content class="flex flex-col gap-4">
			<Alert.Root>
				<InfoIcon />
				<Alert.Title>{m.config_restart_title()}</Alert.Title>
				<Alert.Description>{m.config_restart_description()}</Alert.Description>
			</Alert.Root>

			{#if !active.stored}
				<Alert.Root>
					<InfoIcon />
					<Alert.Title>{m.config_unseeded_title()}</Alert.Title>
					<Alert.Description>{m.config_unseeded_description()}</Alert.Description>
				</Alert.Root>
			{/if}

			{#if rejections[selected]}
				<Alert.Root variant="destructive">
					<Alert.Title>{m.config_rejected_title()}</Alert.Title>
					<Alert.Description>{rejections[selected]}</Alert.Description>
				</Alert.Root>
			{/if}

			{#if syntaxError}
				<Alert.Root variant="destructive">
					<Alert.Title>{m.config_invalid_json_title()}</Alert.Title>
					<Alert.Description>{syntaxError}</Alert.Description>
				</Alert.Root>
			{/if}

			<JsonDocumentEditor
				value={text}
				onChange={(next) => {
					drafts[selected] = next;
				}}
			/>
		</Card.Content>

		<Card.Footer class="flex flex-wrap gap-2">
			<Button onclick={save} disabled={saving !== null || !dirty || syntaxError !== undefined}>
				{#if saving === selected}
					<Spinner data-icon="inline-start" />
				{:else}
					<SaveIcon data-icon="inline-start" />
				{/if}
				{m.config_save()}
			</Button>
			<Button
				variant="outline"
				disabled={saving !== null || !dirty}
				onclick={() => {
					drafts[selected] = active?.json;
					rejections[selected] = undefined;
				}}
			>
				<Undo2Icon data-icon="inline-start" />
				{m.config_revert()}
			</Button>
			<!--
				Loading the defaults into the editor without saving: replacing a
				whole document is not something to confirm in a dialog and then
				apply unseen — the operator reviews the text and saves it
				deliberately, exactly like any other edit.
			-->
			<Button
				variant="outline"
				disabled={saving !== null}
				onclick={() => {
					drafts[selected] = active?.defaultsJson;
					rejections[selected] = undefined;
				}}
			>
				<RotateCcwIcon data-icon="inline-start" />
				{m.config_reset_defaults()}
			</Button>
		</Card.Footer>
	</Card.Root>
{/if}
