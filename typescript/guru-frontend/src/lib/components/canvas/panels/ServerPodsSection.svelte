<script lang="ts">
import DicesIcon from '@lucide/svelte/icons/dices';
import PlusIcon from '@lucide/svelte/icons/plus';
import { untrack } from 'svelte';
import { createPodNode } from '#lib/components/canvas/commands.js';
import { randomFreePort } from '#lib/components/canvas/graph.js';
import { Button } from '#lib/components/ui/button/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Separator } from '#lib/components/ui/separator/index.js';
import type { ServerDto } from '#lib/dto/topology.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';
import ServerLaneRow from './ServerLaneRow.svelte';
import ServerPodRow from './ServerPodRow.svelte';

/**
 * Everything this server carries: the pods an operator places here by hand, and
 * the lanes a universal node generated on it. A pod is placed on exactly one
 * server, so it is created from here; the draft row below is reseeded when the
 * panel is pointed at another server.
 */
let { canvasId, server, editable }: { canvasId: string; server: ServerDto; editable: boolean } =
	$props();

const writes = panelWrites();

// Draft row for the "add pod" form.
let newPodName = $state('');
let newPodBind = $state('');
let newPodAdvertise = $state('');
// `Input` renders a dynamic `type`, so Svelte never coerces this to a number.
let newPodPort = $state('');

const rerollPort = () => {
	newPodPort = String(
		randomFreePort([
			...server.pods.map(pod => pod.port),
			...(server.universal?.lanes ?? []).map(lane => lane.port)
		])
	);
};

let seededFor = $state('');
$effect(() => {
	if (seededFor === server.id) return;
	seededFor = server.id;
	untrack(() => {
		newPodName = '';
		newPodBind = '';
		newPodAdvertise = '';
		rerollPort();
	});
});

const addresses = $derived(server.addresses);
/** Every address the server is known by, pinned values first, deduplicated. */
const knownAddresses = $derived([
	...new Set(
		[
			addresses.v4.pinned,
			addresses.v4.reported,
			addresses.v6.pinned,
			addresses.v6.reported,
			...addresses.extra,
			addresses.observedAddress
		].filter(value => value !== '')
	)
]);
/** What a pod may bind: any address the host actually has, plus the extras. */
const bindChoices = $derived([...new Set([...addresses.reportedInterfaces, ...addresses.extra])]);
const bindLabel = (value: string): string =>
	value === '' ? m.editor_pod_bind_all() : value === '0.0.0.0' ? m.editor_pod_bind_v4() : value;
const advertiseLabel = (value: string): string =>
	value === '' ? m.editor_pod_advertise_auto() : value;

// A pod is placed on exactly one server, so pods are created here. Their stored
// position is unused: they render inside the server node.
const addPod = () =>
	writes.run(async () => {
		await createPodNode({
			canvasId,
			name: newPodName,
			serverId: server.id,
			port: Number(newPodPort),
			bindIp: newPodBind,
			advertiseIp: newPodAdvertise,
			x: server.x,
			y: server.y
		});
		newPodName = '';
		rerollPort();
	}, m.editor_saved());
</script>

<h3 class="text-sm font-medium">{m.editor_pods()}</h3>
{#if server.pods.length === 0}
	<p class="mt-2 text-sm text-muted-foreground">{m.editor_pod_none()}</p>
{:else}
	<div class="mt-2 grid gap-4">
		{#each server.pods as pod (pod.id)}
			<ServerPodRow
				{canvasId}
				{pod}
				{bindChoices}
				advertiseChoices={knownAddresses}
				{editable}
			/>
		{/each}
	</div>
{/if}

<div class="mt-4 grid gap-2">
	<div class="flex gap-2">
		<Input
			class="flex-1"
			placeholder={m.editor_node_name()}
			bind:value={newPodName}
			disabled={!editable}
		/>
		<Input
			type="number"
			min={1}
			max={65535}
			class="w-24"
			bind:value={newPodPort}
			disabled={!editable}
			aria-label={m.editor_pod_port()}
		/>
		<Button
			size="icon-sm"
			variant="ghost"
			disabled={!editable}
			onclick={rerollPort}
			aria-label={m.editor_pod_port_reroll()}
			title={m.editor_pod_port_reroll()}
		>
			<DicesIcon />
		</Button>
	</div>
	<div class="flex gap-2">
		<Select.Root
			type="single"
			value={newPodBind}
			disabled={!editable}
			onValueChange={next => (newPodBind = next)}
		>
			<Select.Trigger class="flex-1" aria-label={m.editor_pod_bind()}>
				{bindLabel(newPodBind)}
			</Select.Trigger>
			<Select.Content>
				<Select.Group>
					<Select.GroupHeading>{m.editor_pod_bind()}</Select.GroupHeading>
					{#each ['', '0.0.0.0', ...bindChoices] as option (option)}
						<Select.Item value={option} label={bindLabel(option)}>{bindLabel(option)}</Select.Item>
					{/each}
				</Select.Group>
			</Select.Content>
		</Select.Root>
		<Select.Root
			type="single"
			value={newPodAdvertise}
			disabled={!editable}
			onValueChange={next => (newPodAdvertise = next)}
		>
			<Select.Trigger class="flex-1" aria-label={m.editor_pod_advertise()}>
				{advertiseLabel(newPodAdvertise)}
			</Select.Trigger>
			<Select.Content>
				<Select.Group>
					<Select.GroupHeading>{m.editor_pod_advertise()}</Select.GroupHeading>
					{#each ['', ...knownAddresses] as option (option)}
						<Select.Item value={option} label={advertiseLabel(option)}>
							{advertiseLabel(option)}
						</Select.Item>
					{/each}
				</Select.Group>
			</Select.Content>
		</Select.Root>
		<Button
			size="sm"
			variant="secondary"
			disabled={!editable || writes.pending || newPodName.trim() === '' || newPodPort === ''}
			onclick={addPod}
		>
			<PlusIcon />
			{m.editor_pod_add()}
		</Button>
	</div>
</div>

{#if server.universal}
	<Separator class="my-6" />
	<h3 class="text-sm font-medium">{m.editor_lanes()}</h3>
	<p class="mt-1 text-xs text-muted-foreground">{m.editor_lanes_hint()}</p>
	{#if server.universal.lanes.length === 0}
		<p class="mt-2 text-sm text-muted-foreground">{m.editor_lanes_none()}</p>
	{:else}
		<div class="mt-2 grid gap-3">
			{#each server.universal.lanes as lane (lane.nodeId)}
				<ServerLaneRow {canvasId} {lane} {editable} />
			{/each}
		</div>
	{/if}
{/if}
