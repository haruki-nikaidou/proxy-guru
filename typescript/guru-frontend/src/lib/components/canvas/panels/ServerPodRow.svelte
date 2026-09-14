<script lang="ts">
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import { untrack } from 'svelte';
import { toast } from 'svelte-sonner';
import { deleteNode, replacePodSpec, updateNodeText } from '#lib/components/canvas/commands.js';
import { Button } from '#lib/components/ui/button/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import type { PodDto } from '#lib/dto/topology.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';
import ConfirmDeleteDialog from './ConfirmDeleteDialog.svelte';

let {
	canvasId,
	pod,
	bindChoices,
	advertiseChoices,
	editable
}: {
	canvasId: string;
	pod: PodDto;
	/** Local addresses the pod may bind besides the two wildcards. */
	bindChoices: string[];
	/** Addresses the pod may advertise besides the server's own. */
	advertiseChoices: string[];
	editable: boolean;
} = $props();

/** The two wildcard binds, keyed so the select can hold a non-address value. */
const BIND_ALL = '';
const BIND_V4 = '0.0.0.0';
const ADVERTISE_AUTO = '';

let name = $state('');
let bindIp = $state(BIND_ALL);
let advertiseIp = $state(ADVERTISE_AUTO);
// `Input` renders a dynamic `type`, so Svelte never coerces this to a number.
let port = $state('1');
let pending = $state(false);
let confirmOpen = $state(false);

let seededFor = $state('');
$effect(() => {
	if (seededFor === pod.id) return;
	seededFor = pod.id;
	const snapshot = pod;
	untrack(() => {
		name = snapshot.name;
		bindIp = snapshot.bindIp ?? BIND_ALL;
		advertiseIp = snapshot.advertiseIp ?? ADVERTISE_AUTO;
		port = String(snapshot.port);
	});
});

const bindLabel = (value: string): string =>
	value === BIND_ALL ? m.editor_pod_bind_all() : value === BIND_V4 ? m.editor_pod_bind_v4() : value;
const advertiseLabel = (value: string): string =>
	value === ADVERTISE_AUTO ? m.editor_pod_advertise_auto() : value;

// A stored address that is no longer among the server's candidates stays
// selectable, so an edit never silently rewrites it.
const bindOptions = $derived([
	BIND_ALL,
	BIND_V4,
	...new Set([...bindChoices, ...(bindIp !== BIND_ALL && bindIp !== BIND_V4 ? [bindIp] : [])])
]);
const advertiseOptions = $derived([
	ADVERTISE_AUTO,
	...new Set([...advertiseChoices, ...(advertiseIp !== ADVERTISE_AUTO ? [advertiseIp] : [])])
]);

async function save() {
	pending = true;
	try {
		await updateNodeText({ canvasId, nodeId: pod.id, name, comment: pod.comment });
		await replacePodSpec({
			canvasId,
			nodeId: pod.id,
			serverId: pod.serverId,
			port: Number(port),
			bindIp,
			advertiseIp
		});
		toast.success(m.editor_saved());
	} catch (err) {
		const body = (err as { body?: App.Error }).body;
		toast.error(errorMessage(body?.code, body?.message ?? ''));
	} finally {
		pending = false;
	}
}

async function remove() {
	pending = true;
	try {
		await deleteNode({ canvasId, nodeId: pod.id, force: false });
		confirmOpen = false;
		toast.success(m.editor_deleted());
	} catch (err) {
		const body = (err as { body?: App.Error }).body;
		toast.error(errorMessage(body?.code, body?.message ?? ''));
	} finally {
		pending = false;
	}
}
</script>

<div class="flex items-end gap-2">
	<div class="grid flex-1 gap-2">
		<div class="flex gap-2">
			<Input
				class="flex-1"
				bind:value={name}
				disabled={!editable}
				aria-label={m.editor_node_name()}
			/>
			<Input
				type="number"
				min={1}
				max={65535}
				class="w-24"
				bind:value={port}
				disabled={!editable}
				aria-label={m.editor_pod_port()}
			/>
		</div>
		<div class="flex gap-2">
			<Select.Root
				type="single"
				value={bindIp}
				disabled={!editable}
				onValueChange={next => (bindIp = next)}
			>
				<Select.Trigger class="flex-1" aria-label={m.editor_pod_bind()}>
					{bindLabel(bindIp)}
				</Select.Trigger>
				<Select.Content>
					<Select.Group>
						<Select.GroupHeading>{m.editor_pod_bind()}</Select.GroupHeading>
						{#each bindOptions as option (option)}
							<Select.Item value={option} label={bindLabel(option)}>{bindLabel(option)}</Select.Item>
						{/each}
					</Select.Group>
				</Select.Content>
			</Select.Root>
			<Select.Root
				type="single"
				value={advertiseIp}
				disabled={!editable}
				onValueChange={next => (advertiseIp = next)}
			>
				<Select.Trigger class="flex-1" aria-label={m.editor_pod_advertise()}>
					{advertiseLabel(advertiseIp)}
				</Select.Trigger>
				<Select.Content>
					<Select.Group>
						<Select.GroupHeading>{m.editor_pod_advertise()}</Select.GroupHeading>
						{#each advertiseOptions as option (option)}
							<Select.Item value={option} label={advertiseLabel(option)}>
								{advertiseLabel(option)}
							</Select.Item>
						{/each}
					</Select.Group>
				</Select.Content>
			</Select.Root>
		</div>
	</div>
	<div class="grid gap-2">
		<Button size="sm" variant="secondary" disabled={!editable || pending} onclick={save}>
			{#if pending}<Spinner data-icon="inline-start" />{/if}
			{m.common_save()}
		</Button>
		<Button
			size="sm"
			variant="ghost"
			disabled={!editable || pending}
			onclick={() => (confirmOpen = true)}
			aria-label={m.common_delete()}
		>
			<Trash2Icon />
		</Button>
	</div>
</div>

<ConfirmDeleteDialog
	bind:open={confirmOpen}
	title={m.editor_pod_delete_title()}
	description={m.editor_pod_delete_description({ name: pod.name, port: pod.port })}
	{pending}
	onconfirm={remove}
/>
