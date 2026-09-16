<script lang="ts">
import Trash2Icon from '@lucide/svelte/icons/trash-2';
import PlusIcon from '@lucide/svelte/icons/plus';
import { untrack } from 'svelte';
import ServerGlyph from '#lib/components/canvas/ServerGlyph.svelte';
import { deleteServerNode, updateServerNode } from '#lib/components/canvas/commands.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Separator } from '#lib/components/ui/separator/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import type { Ipv6ResolveName, ServerDto } from '#lib/dto/topology.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';
import { IPV6_OPTIONS, ipv6Label } from '#lib/i18n/labels.js';
import ConfirmDeleteDialog from '#lib/components/ConfirmDeleteDialog.svelte';

/**
 * What an operator may change about the server itself: how it is named and
 * logged, how it resolves IPv6, and the addresses its pods advertise. Every
 * field is a draft until Save; the panel reseeds them when it is pointed at
 * another server.
 */
let { canvasId, server, editable }: { canvasId: string; server: ServerDto; editable: boolean } =
	$props();

const writes = panelWrites();
let deleteOpen = $state(false);

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
	});
});

const addresses = $derived(server.addresses);
const sourceLabel = $derived(
	addresses.effectiveSource === 'override'
		? m.editor_server_address_source_override()
		: addresses.effectiveSource === 'reported'
			? m.editor_server_address_source_reported()
			: addresses.effectiveSource === 'observed'
				? m.editor_server_address_source_observed()
				: ''
);

const addExtra = () => {
	const value = newExtra.trim();
	if (value === '' || extraAddresses.includes(value)) return;
	extraAddresses = [...extraAddresses, value];
	newExtra = '';
};
const removeExtra = (value: string) => {
	extraAddresses = extraAddresses.filter(entry => entry !== value);
};

const save = () =>
	writes.run(
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
				extraAddresses,
				// Chosen in the install dialog; the settings form only carries it along.
				agentUnit: server.agentUnit
			}),
		m.editor_saved()
	);

// The command retires the server's pods first; the panel closes on its own once
// the refreshed graph no longer holds the server.
const removeServer = () =>
	writes.run(async () => {
		await deleteServerNode({ canvasId, serverId: server.id, force: false });
		deleteOpen = false;
	}, m.editor_deleted());
</script>

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
					disabled={!editable || writes.pending}
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

<Button class="mt-4 w-full" disabled={!editable || writes.pending} onclick={save}>
	{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
	{m.common_save()}
</Button>
<Button
	class="mt-2 w-full"
	variant="outline"
	disabled={!editable || writes.pending}
	onclick={() => (deleteOpen = true)}
>
	<Trash2Icon />
	{m.editor_server_delete()}
</Button>

<ConfirmDeleteDialog
	bind:open={deleteOpen}
	title={m.editor_server_delete()}
	description={m.editor_server_delete_description({ name: server.name, count: server.pods.length })}
	pending={writes.pending}
	onconfirm={removeServer}
/>
