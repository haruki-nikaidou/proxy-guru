<script lang="ts">
import FileTextIcon from '@lucide/svelte/icons/file-text';
import CopyButton from '#lib/components/CopyButton.svelte';
import {
	forgetServerApplied,
	getServerConfigToml,
	getServerRollout
} from '#lib/components/canvas/commands.js';
import { serverHealthBadge, serverHealthLabel } from '#lib/components/canvas/graph.js';
import * as Alert from '#lib/components/ui/alert/index.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Sheet from '#lib/components/ui/sheet/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import * as Table from '#lib/components/ui/table/index.js';
import type { ConfigSnapshotDto, ServerDto } from '#lib/dto/topology.js';
import { errorText } from '#lib/i18n/codes.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';
import ConfirmDeleteDialog from '#lib/components/ConfirmDeleteDialog.svelte';

/**
 * Where this server stands between the config the control plane derived and the
 * config its worker confirmed, plus the two things an operator can do about it:
 * read the rendered TOML, and (as an Admin) declare the server dead.
 */
let {
	canvasId,
	server,
	admin,
	serverNames
}: {
	canvasId: string;
	server: ServerDto;
	/** `ForgetServerApplied` is admin-only in the control plane. */
	admin: boolean;
	/** Server id → name, for the ids a rollout says it is waiting for. */
	serverNames: ReadonlyMap<string, string>;
} = $props();

const writes = panelWrites();

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
	writes.run(async () => {
		await forgetServerApplied({ canvasId, serverId: server.id });
		forgetOpen = false;
	}, m.editor_rollout_forgotten());
</script>

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
			{m.editor_server_last_seen()}: {formatTimestamp(server.lastHealthReportAt)}
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
		<h3 class="text-sm font-medium">{m.editor_rollout_title()}</h3>
		<Alert.Root variant="destructive" class="mt-2">
			<Alert.Title>{m.editor_rollout_unavailable()}</Alert.Title>
			<Alert.Description>{errorText(error)}</Alert.Description>
		</Alert.Root>
	{/snippet}
</svelte:boundary>

<div class="mt-4 flex flex-wrap gap-2">
	<Button size="sm" variant="secondary" onclick={() => (tomlOpen = true)}>
		<FileTextIcon />
		{m.editor_rollout_show_toml()}
	</Button>
	{#if admin}
		<Button size="sm" variant="outline" disabled={writes.pending} onclick={() => (forgetOpen = true)}>
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
					<Alert.Root variant="destructive">
						<Alert.Description>{errorText(error)}</Alert.Description>
					</Alert.Root>
				{/snippet}
			</svelte:boundary>
		</div>
	</Sheet.Content>
</Sheet.Root>

{#if admin}
	<ConfirmDeleteDialog
		bind:open={forgetOpen}
		title={m.editor_rollout_forget_title()}
		description={m.editor_rollout_forget_description({ name: server.name })}
		confirm={m.editor_rollout_forget_confirm()}
		pending={writes.pending}
		onconfirm={forget}
	/>
{/if}
