<script lang="ts">
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import { edgeTargetExit, type Exit, putExit, removeAll } from 'guru-graph';
import { useEditor } from '#lib/components/canvas/editor.svelte.js';
import RuleDots from '#lib/components/canvas/nodes/RuleDots.svelte';
import { Button } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Separator } from '#lib/components/ui/separator/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import type { ProxyProtocolName } from '#lib/dto/topology.js';
import { PROXY_OPTIONS, proxyLabel, proxyName, proxyVersion } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import { seedOn } from '#lib/seed.svelte.js';

/** An exit: a destination outside the fabric, and the pods that lead there. */
let { exit }: { exit: Exit } = $props();

const editor = useEditor();
const graph = $derived(editor.graph);

let name = $state('');
let comment = $state('');
let destination = $state('');
let proxy = $state<ProxyProtocolName>('none');
let pending = $state(false);

seedOn(
	() => exit.id,
	() => {
		name = exit.name;
		comment = exit.comment;
		destination = exit.destination;
		proxy = proxyName(exit.sendProxyProtocol);
	}
);

async function save() {
	pending = true;
	const next: Exit = {
		...exit,
		name: name.trim(),
		comment: comment.trim(),
		destination: destination.trim(),
		sendProxyProtocol: proxyVersion(proxy)
	};
	await editor.commit(() => putExit(editor.graph, next), m.editor_saved());
	pending = false;
}

const dialedBy = $derived(
	graph.edges
		.filter(edge => edgeTargetExit(edge) === exit.id)
		.flatMap(edge => graph.pods.filter(pod => pod.id === edge.sourcePodId))
		.sort((a, b) => a.name.localeCompare(b.name))
);
const serverName = (id: string) => graph.servers.find(entry => entry.id === id)?.name ?? id;
const rules = $derived(editor.drawing.rules.exits.get(exit.id) ?? []);

function remove() {
	const current = editor.graph;
	editor.review({
		title: m.editor_exit_delete(),
		description: m.editor_exit_delete_description({ name: exit.name, count: dialedBy.length }),
		prunable: true,
		build: prune => removeAll(current, editor.drawing, { exitIds: [exit.id] }, prune),
		success: m.editor_deleted()
	});
}
</script>

<Field.FieldGroup>
	<Field.Field>
		<Field.FieldLabel for="exit-name">{m.editor_node_name()}</Field.FieldLabel>
		<Input id="exit-name" bind:value={name} disabled={!editor.editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="exit-comment">{m.editor_node_comment()}</Field.FieldLabel>
		<Textarea id="exit-comment" bind:value={comment} disabled={!editor.editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="exit-destination">{m.editor_destination()}</Field.FieldLabel>
		<Input
			id="exit-destination"
			class="font-mono"
			bind:value={destination}
			disabled={!editor.editable}
		/>
		<Field.FieldDescription>{m.editor_destination_hint()}</Field.FieldDescription>
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="exit-proxy">{m.editor_pass_proxy()}</Field.FieldLabel>
		<Select.Root
			type="single"
			value={proxy}
			disabled={!editor.editable}
			onValueChange={next => (proxy = next as ProxyProtocolName)}
		>
			<Select.Trigger id="exit-proxy">{proxyLabel(proxy)}</Select.Trigger>
			<Select.Content>
				<Select.Group>
					{#each PROXY_OPTIONS as option (option)}
						<Select.Item value={option} label={proxyLabel(option)}>{proxyLabel(option)}</Select.Item>
					{/each}
				</Select.Group>
			</Select.Content>
		</Select.Root>
	</Field.Field>
</Field.FieldGroup>

<Button
	class="mt-6 w-full"
	disabled={!editor.editable || pending || name.trim() === '' || destination.trim() === ''}
	onclick={save}
>
	{#if pending}<Spinner data-icon="inline-start" />{/if}
	{m.common_save()}
</Button>

<Separator class="my-6" />

<div class="flex items-center gap-2">
	<h3 class="text-sm font-medium">{m.editor_exit_led_here()}</h3>
	<span class="ms-auto"><RuleDots {rules} /></span>
</div>
{#if dialedBy.length === 0}
	<p class="mt-1 text-xs text-muted-foreground">{m.editor_exit_led_here_none()}</p>
{:else}
	<ul class="mt-2 grid gap-1">
		{#each dialedBy as pod (pod.id)}
			<li>
				<button
					type="button"
					class="flex w-full items-center gap-2 rounded px-2 py-1 text-start text-sm hover:bg-accent"
					onclick={() => editor.open({ kind: 'pod', id: pod.id })}
				>
					<span class="min-w-0 truncate">{pod.name}</span>
					<span class="ms-auto shrink-0 text-xs text-muted-foreground">{serverName(pod.serverId)}</span>
				</button>
			</li>
		{/each}
	</ul>
{/if}

{#if editor.editable}
	<Button class="mt-6 w-full" variant="outline" onclick={remove}>
		<Trash2Icon />
		{m.editor_exit_delete()}
	</Button>
{/if}
