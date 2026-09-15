<script lang="ts">
import DicesIcon from '@lucide/svelte/icons/dices';
import FileTextIcon from '@lucide/svelte/icons/file-text';
import PlusIcon from '@lucide/svelte/icons/plus';
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import { untrack } from 'svelte';
import { toast } from 'svelte-sonner';
import CopyButton from '#lib/components/CopyButton.svelte';
import {
	createPodNode,
	deleteServerNode,
	forgetServerApplied,
	getServerConfigToml,
	getServerRollout,
	updateServerNode
} from '#lib/components/canvas/commands.js';
import {
	randomFreePort,
	serverHealthBadge,
	serverHealthLabel
} from '#lib/components/canvas/graph.js';
import ServerGlyph from '#lib/components/canvas/ServerGlyph.svelte';
import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
import * as Alert from '#lib/components/ui/alert/index.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button, buttonVariants } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Separator } from '#lib/components/ui/separator/index.js';
import * as Sheet from '#lib/components/ui/sheet/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import * as Table from '#lib/components/ui/table/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import type { ConfigSnapshotDto, Ipv6ResolveName, ServerDto } from '#lib/dto/topology.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import ConfirmDeleteDialog from './ConfirmDeleteDialog.svelte';
import ServerLaneRow from './ServerLaneRow.svelte';
import ServerPodRow from './ServerPodRow.svelte';

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

const IPV6_OPTIONS: Ipv6ResolveName[] = ['required', 'preferred', 'tolerated', 'forbidden'];
const ipv6Label = (value: Ipv6ResolveName): string =>
	value === 'required'
		? m.editor_ipv6_required()
		: value === 'preferred'
			? m.editor_ipv6_preferred()
			: value === 'forbidden'
				? m.editor_ipv6_forbidden()
				: m.editor_ipv6_tolerated();

let name = $state('');
let icon = $state('');
let comment = $state('');
let ipv6Resolve = $state<Ipv6ResolveName>('tolerated');
let logLevel = $state('info');
// The two fixed address slots are learned from the worker; a pin overrides what
// it reported. Extras are whatever else the operator wants pods to advertise.
let pinV4 = $state('');
let pinV6 = $state('');
let extraAddresses = $state<string[]>([]);
let newExtra = $state('');
let pending = $state(false);
let deleteOpen = $state(false);

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
	const snapshot = server;
	untrack(() => {
		name = snapshot.name;
		icon = snapshot.icon;
		comment = snapshot.comment;
		ipv6Resolve = snapshot.ipv6Resolve;
		logLevel = snapshot.logLevel;
		pinV4 = snapshot.addresses.v4.pinned;
		pinV6 = snapshot.addresses.v6.pinned;
		extraAddresses = [...snapshot.addresses.extra];
		newExtra = '';
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
const bindChoices = $derived([
	...new Set([...addresses.reportedInterfaces, ...addresses.extra])
]);
const sourceLabel = $derived(
	addresses.effectiveSource === 'override'
		? m.editor_server_address_source_override()
		: addresses.effectiveSource === 'reported'
			? m.editor_server_address_source_reported()
			: addresses.effectiveSource === 'observed'
				? m.editor_server_address_source_observed()
				: ''
);
const bindLabel = (value: string): string =>
	value === '' ? m.editor_pod_bind_all() : value === '0.0.0.0' ? m.editor_pod_bind_v4() : value;
const advertiseLabel = (value: string): string =>
	value === '' ? m.editor_pod_advertise_auto() : value;

const addExtra = () => {
	const value = newExtra.trim();
	if (value === '' || extraAddresses.includes(value)) return;
	extraAddresses = [...extraAddresses, value];
	newExtra = '';
};
const removeExtra = (value: string) => {
	extraAddresses = extraAddresses.filter(entry => entry !== value);
};

/** Every mutation here reports the control plane's own text on failure. */
async function run(action: () => Promise<unknown>, success: string) {
	pending = true;
	try {
		await action();
		toast.success(success);
	} catch (err) {
		const body = (err as { body?: App.Error }).body;
		toast.error(errorMessage(body?.code, body?.message ?? ''));
	} finally {
		pending = false;
	}
}

const save = () =>
	run(
		() =>
			updateServerNode({
				canvasId,
				serverId: server.id,
				name,
				icon,
				comment,
				ipv6Resolve,
				logLevel,
				overrideV4: pinV4,
				overrideV6: pinV6,
				extraAddresses
			}),
		m.editor_saved()
	);

// A pod is placed on exactly one server, so pods are created here. Their stored
// position is unused: they render inside the server node.
const addPod = () =>
	run(async () => {
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

// The command retires the server's pods first; the panel closes on its own once
// the refreshed graph no longer holds the server.
const removeServer = () =>
	run(async () => {
		await deleteServerNode({ canvasId, serverId: server.id, force: false });
		deleteOpen = false;
	}, m.editor_deleted());

// The panel is only mounted while it is open for this server, so the rollout is
// fetched exactly then; a different target re-runs the query through its arg.
const rollout = $derived(getServerRollout({ serverId: server.id }));

// The TOML is large and rendered on the control plane, so it is only asked for
// once the operator opens the sheet.
let tomlOpen = $state(false);
const config = $derived(tomlOpen ? getServerConfigToml({ serverId: server.id }) : undefined);

let forgetOpen = $state(false);

const health = $derived(serverHealthBadge(server.healthStatus));

/**
 * The reading the three revisions add up to. Failure outranks revision
 * equality, which on its own is not convergence: the master stores a partial
 * apply as a synthesised mix that keeps the *desired* revision number, so a
 * server whose pods failed individually reports matching revisions while
 * `invalidPods` names what is not running. Checking the errors and that list
 * first is what stops the panel from printing "Converged" directly above a
 * table of failed pods.
 *
 * Named `rolloutState`, not `state`: a variable called `state` makes every
 * `$state(...)` in this component parse as a store read of it.
 */
const rolloutState = $derived.by((): 'loading' | 'converged' | 'rolling' | 'stuck' | 'none' => {
	const current = rollout.current;
	if (current === undefined) return 'loading';
	if (current.applyError !== '' || current.deriveError !== '') return 'stuck';
	if (current.invalidPods.length > 0) return 'stuck';
	if (current.desired === null) return 'none';
	if (current.applied?.revision !== current.desired.revision) return 'rolling';
	return 'converged';
});

const stateLabel = $derived(
	rolloutState === 'converged'
		? m.editor_rollout_converged()
		: rolloutState === 'rolling'
			? m.editor_rollout_rolling()
			: rolloutState === 'stuck'
				? m.editor_rollout_stuck()
				: m.editor_rollout_none()
);
const stateVariant: 'secondary' | 'outline' | 'destructive' = $derived(
	rolloutState === 'converged' ? 'secondary' : rolloutState === 'stuck' ? 'destructive' : 'outline'
);

/** `#12 · 3 forwardings`, or a plain dash when the control plane has no snapshot. */
const revisionOf = (snapshot: ConfigSnapshotDto | null): string =>
	snapshot === null
		? m.editor_rollout_revision_none()
		: m.editor_rollout_revision({
				revision: snapshot.revision,
				count: snapshot.forwardings.length
			});

/**
 * The three snapshots in rollout order: what the control plane wants, what it
 * handed the worker, and what the worker acknowledged.
 */
const revisions = $derived([
	{ key: 'desired', label: m.editor_rollout_desired(), snapshot: rollout.current?.desired ?? null },
	{
		key: 'in_flight',
		label: m.editor_rollout_in_flight(),
		snapshot: rollout.current?.inFlight ?? null
	},
	{ key: 'applied', label: m.editor_rollout_applied(), snapshot: rollout.current?.applied ?? null }
]);

/** An id outside this canvas has no name here; the raw id is still actionable. */
const waitingName = (id: string): string => serverNames.get(id) ?? id;

const forget = () =>
	run(async () => {
		await forgetServerApplied({ canvasId, serverId: server.id });
		forgetOpen = false;
	}, m.editor_rollout_forgotten());
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

<Field.FieldGroup>
	<Field.Field>
		<Field.FieldLabel for="server-name">{m.editor_node_name()}</Field.FieldLabel>
		<Input id="server-name" bind:value={name} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="server-icon">{m.editor_server_icon()}</Field.FieldLabel>
		<!-- The preview is the only feedback that a name resolves: an unknown one
		     shows the default glyph instead of the icon. -->
		<div class="flex items-center gap-2">
			<Input id="server-icon" bind:value={icon} disabled={!editable} />
			<ServerGlyph {icon} />
		</div>
		<Field.FieldDescription>{m.editor_server_icon_hint()}</Field.FieldDescription>
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="server-comment">{m.editor_node_comment()}</Field.FieldLabel>
		<Textarea id="server-comment" bind:value={comment} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="server-ipv6">{m.editor_server_ipv6()}</Field.FieldLabel>
		<Select.Root
			type="single"
			value={ipv6Resolve}
			disabled={!editable}
			onValueChange={next => (ipv6Resolve = next as Ipv6ResolveName)}
		>
			<Select.Trigger id="server-ipv6">{ipv6Label(ipv6Resolve)}</Select.Trigger>
			<Select.Content>
				<Select.Group>
					{#each IPV6_OPTIONS as option (option)}
						<Select.Item value={option} label={ipv6Label(option)}>{ipv6Label(option)}</Select.Item>
					{/each}
				</Select.Group>
			</Select.Content>
		</Select.Root>
		<Field.FieldDescription>{m.editor_server_ipv6_hint()}</Field.FieldDescription>
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="server-log-level">{m.editor_server_log_level()}</Field.FieldLabel>
		<Input id="server-log-level" bind:value={logLevel} disabled={!editable} />
	</Field.Field>
</Field.FieldGroup>

<Separator class="my-6" />

<h3 class="text-sm font-medium">{m.editor_server_addresses()}</h3>
<p class="mt-1 text-sm">
	<span class="text-muted-foreground">{m.editor_server_address_effective()}:</span>
	{#if addresses.effectiveAddress}
		<span class="font-mono">{addresses.effectiveAddress}</span>
		<Badge variant="outline" class="ms-1">{sourceLabel}</Badge>
		{#if addresses.reportedCountry}
			<Badge variant="secondary" class="ms-1 font-mono">{addresses.reportedCountry}</Badge>
		{/if}
	{:else}
		<span class="text-muted-foreground">{m.editor_server_address_none()}</span>
	{/if}
</p>

<div class="mt-3 grid gap-3">
	<Field.Field>
		<Field.FieldLabel for="server-address-v4">{m.editor_server_address_v4()}</Field.FieldLabel>
		<Input
			id="server-address-v4"
			class="font-mono"
			placeholder={m.editor_server_address_pin()}
			bind:value={pinV4}
			disabled={!editable}
		/>
		<Field.FieldDescription class="font-mono">
			{addresses.v4.reported
				? `${m.editor_server_address_reported()}: ${addresses.v4.reported}`
				: m.editor_server_address_unreported()}
		</Field.FieldDescription>
	</Field.Field>
	<Field.Field>
		<Field.FieldLabel for="server-address-v6">{m.editor_server_address_v6()}</Field.FieldLabel>
		<Input
			id="server-address-v6"
			class="font-mono"
			placeholder={m.editor_server_address_pin()}
			bind:value={pinV6}
			disabled={!editable}
		/>
		<Field.FieldDescription class="font-mono">
			{addresses.v6.reported
				? `${m.editor_server_address_reported()}: ${addresses.v6.reported}`
				: m.editor_server_address_unreported()}
		</Field.FieldDescription>
	</Field.Field>
</div>

<h4 class="mt-4 text-xs font-medium text-muted-foreground">{m.editor_server_address_extra()}</h4>
{#if extraAddresses.length > 0}
	<ul class="mt-1 grid gap-1">
		{#each extraAddresses as extra (extra)}
			<li class="flex items-center gap-2 text-sm">
				<span class="flex-1 font-mono">{extra}</span>
				<Button
					size="sm"
					variant="ghost"
					disabled={!editable || pending}
					onclick={() => removeExtra(extra)}
					aria-label={m.common_delete()}
				>
					<Trash2Icon />
				</Button>
			</li>
		{/each}
	</ul>
{/if}
<div class="mt-2 flex items-end gap-2">
	<Input
		class="font-mono"
		placeholder={m.editor_server_address_add()}
		bind:value={newExtra}
		disabled={!editable}
		onkeydown={event => {
			if (event.key === 'Enter') {
				event.preventDefault();
				addExtra();
			}
		}}
	/>
	<Button
		size="sm"
		variant="secondary"
		disabled={!editable || newExtra.trim() === ''}
		onclick={addExtra}
	>
		<PlusIcon />
		{m.editor_server_address_add()}
	</Button>
</div>

{#if addresses.observedAddress || addresses.reportedInterfaces.length > 0}
	<div class="mt-3 grid gap-1 text-xs text-muted-foreground">
		{#if addresses.observedAddress}
			<p>
				{m.editor_server_address_observed()}:
				<span class="font-mono">{addresses.observedAddress}</span>
				· {formatTimestamp(addresses.observedAt)}
			</p>
		{/if}
		{#if addresses.reportedInterfaces.length > 0}
			<p>
				{m.editor_server_address_interfaces()}:
				<span class="font-mono">{addresses.reportedInterfaces.join(', ')}</span>
			</p>
		{/if}
	</div>
{/if}

<Button class="mt-4 w-full" disabled={!editable || pending} onclick={save}>
	{#if pending}<Spinner data-icon="inline-start" />{/if}
	{m.common_save()}
</Button>
<Button
	class="mt-2 w-full"
	variant="outline"
	disabled={!editable || pending}
	onclick={() => (deleteOpen = true)}
>
	<Trash2Icon />
	{m.editor_server_delete()}
</Button>

<Separator class="my-6" />

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
			disabled={!editable || pending || newPodName.trim() === '' || newPodPort === ''}
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

<ConfirmDeleteDialog
	bind:open={deleteOpen}
	title={m.editor_server_delete()}
	description={m.editor_server_delete_description({ name: server.name, count: server.pods.length })}
	{pending}
	onconfirm={removeServer}
/>

<Separator class="my-6" />

<!-- A failing rollout read must not take the whole canvas down with it: the
     panel reports it and the flow on the left keeps working. -->
<svelte:boundary>
	<div class="flex items-center gap-2">
		<h3 class="text-sm font-medium">{m.editor_rollout_title()}</h3>
		{#if rolloutState === 'loading'}
			<Spinner class="ms-auto size-4 shrink-0" />
		{:else}
			<Badge variant={stateVariant} class="ms-auto shrink-0">{stateLabel}</Badge>
		{/if}
	</div>
	<p class="mt-1 text-xs text-muted-foreground">{m.editor_rollout_description()}</p>

	<div class="mt-2 flex items-center gap-2 text-xs text-muted-foreground">
		<Badge variant={health.variant} class="shrink-0 {health.class}">
			{serverHealthLabel(server.healthStatus)}
		</Badge>
		<span class="truncate">
			{m.editor_server_last_seen()}: {formatTimestamp(server.lastSeenAt)}
		</span>
	</div>

	{#if rollout.current === undefined}
		<Skeleton class="mt-3 h-28 w-full" />
	{:else}
		{@const status = rollout.current}
		{#if status.deriveError}
			<Alert.Root variant="destructive" class="mt-3">
				<Alert.Title>{m.editor_rollout_derive_failed()}</Alert.Title>
				<Alert.Description>
					<p>{m.editor_rollout_derive_failed_hint()}</p>
					<p class="font-mono text-xs">{status.deriveError}</p>
				</Alert.Description>
			</Alert.Root>
		{/if}
		{#if status.applyError}
			<Alert.Root variant="destructive" class="mt-3">
				<Alert.Title>{m.editor_rollout_apply_failed()}</Alert.Title>
				<Alert.Description>
					<p>{m.editor_rollout_apply_failed_hint()}</p>
					<p class="font-mono text-xs">{status.applyError}</p>
				</Alert.Description>
			</Alert.Root>
		{/if}

		<div class="mt-3 flex flex-col gap-2">
			{#each revisions as row (row.key)}
				<div class="flex items-baseline gap-2 text-sm">
					<span class="w-20 shrink-0 text-xs text-muted-foreground">{row.label}</span>
					<span class="font-mono">{revisionOf(row.snapshot)}</span>
					{#if row.snapshot}
						<span class="ms-auto shrink-0 text-xs text-muted-foreground">
							{formatTimestamp(row.snapshot.createdAt)}
						</span>
					{/if}
				</div>
			{/each}
		</div>

		{#if status.derivationPending}
			<p class="mt-3 flex items-center gap-2 text-xs text-muted-foreground">
				<Spinner class="size-3 shrink-0" />
				{m.editor_rollout_derivation_pending()}
			</p>
		{/if}

		{#if status.waitingForServerIds.length > 0}
			<div class="mt-4 flex flex-col gap-1">
				<h4 class="text-sm font-medium">{m.editor_rollout_waiting()}</h4>
				<p class="text-xs text-muted-foreground">{m.editor_rollout_waiting_hint()}</p>
				<ul class="mt-1 flex flex-wrap gap-1">
					{#each status.waitingForServerIds as waiting (waiting)}
						<li><Badge variant="outline" class="font-mono">{waitingName(waiting)}</Badge></li>
					{/each}
				</ul>
			</div>
		{/if}

		{#if status.invalidPods.length > 0}
			<div class="mt-4 flex flex-col gap-1">
				<h4 class="text-sm font-medium">{m.editor_rollout_invalid_pods()}</h4>
				<p class="text-xs text-muted-foreground">{m.editor_rollout_invalid_pods_hint()}</p>
				<Table.Root class="mt-1">
					<Table.Header>
						<Table.Row>
							<Table.Head>{m.editor_rollout_invalid_pod_name()}</Table.Head>
							<Table.Head>{m.editor_rollout_invalid_pod_listen()}</Table.Head>
							<Table.Head>{m.editor_rollout_invalid_pod_error()}</Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each status.invalidPods as invalid (invalid.nodeId)}
							<Table.Row>
								<Table.Cell class="font-medium">{invalid.podName}</Table.Cell>
								<Table.Cell class="font-mono text-xs">{invalid.listen}</Table.Cell>
								<Table.Cell class="text-xs text-destructive">{invalid.error}</Table.Cell>
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			</div>
		{/if}
	{/if}

	{#snippet failed(error)}
		{@const body = (error as { body?: App.Error }).body}
		<h3 class="text-sm font-medium">{m.editor_rollout_title()}</h3>
		<Alert.Root variant="destructive" class="mt-2">
			<Alert.Title>{m.editor_rollout_unavailable()}</Alert.Title>
			<Alert.Description>{errorMessage(body?.code, body?.message ?? '')}</Alert.Description>
		</Alert.Root>
	{/snippet}
</svelte:boundary>

<div class="mt-4 flex flex-wrap gap-2">
	<Button size="sm" variant="secondary" onclick={() => (tomlOpen = true)}>
		<FileTextIcon />
		{m.editor_rollout_show_toml()}
	</Button>
	{#if admin}
		<Button size="sm" variant="outline" disabled={pending} onclick={() => (forgetOpen = true)}>
			{m.editor_rollout_forget()}
		</Button>
	{/if}
</div>

<Sheet.Root bind:open={tomlOpen}>
	<Sheet.Content side="right" class="sm:max-w-xl! gap-0">
		<Sheet.Header>
			<Sheet.Title>{m.editor_rollout_toml_title()}</Sheet.Title>
			<Sheet.Description>{m.editor_rollout_toml_description()}</Sheet.Description>
		</Sheet.Header>
		<div class="flex min-h-0 flex-1 flex-col gap-2 px-4 pb-4">
			<svelte:boundary>
				{#if config === undefined || config.current === undefined}
					<Skeleton class="h-64 w-full" />
				{:else}
					{@const rendered = config.current}
					<div class="flex items-center gap-2 text-xs text-muted-foreground">
						<span class="font-mono">
							{m.editor_rollout_revision_only({ revision: rendered.revision })}
						</span>
						<CopyButton
							value={rendered.toml}
							label={m.editor_rollout_toml_copy()}
							size="icon-xs"
							class="shrink-0"
						/>
					</div>
					<pre
						class="min-h-0 flex-1 overflow-auto rounded-md border bg-muted/40 p-3 font-mono text-xs">{rendered.toml}</pre>
				{/if}

				{#snippet failed(error)}
					{@const body = (error as { body?: App.Error }).body}
					<Alert.Root variant="destructive">
						<Alert.Description>{errorMessage(body?.code, body?.message ?? '')}</Alert.Description>
					</Alert.Root>
				{/snippet}
			</svelte:boundary>
		</div>
	</Sheet.Content>
</Sheet.Root>

{#if admin}
	<AlertDialog.Root bind:open={forgetOpen}>
		<AlertDialog.Content>
			<AlertDialog.Header>
				<AlertDialog.Title>{m.editor_rollout_forget_title()}</AlertDialog.Title>
				<AlertDialog.Description>
					{m.editor_rollout_forget_description({ name: server.name })}
				</AlertDialog.Description>
			</AlertDialog.Header>
			<AlertDialog.Footer>
				<AlertDialog.Cancel>{m.common_cancel()}</AlertDialog.Cancel>
				<AlertDialog.Action
					class={buttonVariants({ variant: 'destructive' })}
					onclick={forget}
				>
					{m.editor_rollout_forget_confirm()}
				</AlertDialog.Action>
			</AlertDialog.Footer>
		</AlertDialog.Content>
	</AlertDialog.Root>
{/if}
