<script lang="ts">
import PlusIcon from '@lucide/svelte/icons/plus';
import SquareTerminalIcon from '@lucide/svelte/icons/square-terminal';
import BoundaryError from '#lib/components/BoundaryError.svelte';
import ConfirmDeleteDialog from '#lib/components/ConfirmDeleteDialog.svelte';
import {
	closeShellSession,
	listShellSessions,
	openShellSession
} from '#lib/components/canvas/commands.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Dialog from '#lib/components/ui/dialog/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import * as Item from '#lib/components/ui/item/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import type { ShellSessionDto } from '#lib/dto/shell.js';
import type { ServerDto } from '#lib/dto/topology.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';
import ShellTerminal from './ShellTerminal.svelte';

/**
 * The remote shell on one server's worker: the sessions open on it, and the
 * transcript of the one this view is attached to.
 */
let { open = $bindable(false), server }: { open?: boolean; server: ServerDto } = $props();

const writes = panelWrites();
// Read only while the dialog is open; the control plane asks the worker.
const sessions = $derived(open ? listShellSessions({ serverId: server.id }) : undefined);
let attached = $state<ShellSessionDto | null>(null);
let closing = $state<ShellSessionDto | null>(null);

$effect(() => {
	if (!open) {
		attached = null;
		closing = null;
	}
});

async function openSession() {
	await writes.run(async () => {
		attached = await openShellSession({ serverId: server.id });
	});
}

async function closeSession() {
	const target = closing;
	if (target === null) return;
	// The attached view stays: its transcript ends with the session closed.
	await writes.run(async () => {
		await closeShellSession({ serverId: server.id, sessionId: target.sessionId });
		closing = null;
	}, m.editor_shell_session_closed());
}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="flex h-[85vh] flex-col sm:max-w-6xl">
		<Dialog.Header>
			<Dialog.Title>{m.editor_shell_title({ name: server.name })}</Dialog.Title>
			<Dialog.Description>{m.editor_shell_description()}</Dialog.Description>
		</Dialog.Header>

		<div class="flex min-h-0 flex-1 gap-4">
			<div class="flex w-64 shrink-0 flex-col gap-2">
				<div class="flex items-center gap-2">
					<h3 class="text-sm font-medium">{m.editor_shell_sessions()}</h3>
					<Button size="sm" class="ms-auto" disabled={writes.pending} onclick={openSession}>
						{#if writes.pending}
							<Spinner data-icon="inline-start" />
						{:else}
							<PlusIcon data-icon="inline-start" />
						{/if}
						{m.editor_shell_new_session()}
					</Button>
				</div>
				{#if sessions?.current === undefined && sessions?.error}
					<BoundaryError error={sessions.error} retry variant="inline" />
				{:else if sessions?.current === undefined}
					<Skeleton class="h-16 w-full" />
				{:else if sessions.current.length === 0}
					<p class="text-xs text-muted-foreground">{m.editor_shell_no_sessions()}</p>
				{:else}
					<Item.Group class="min-h-0 gap-2 overflow-y-auto">
						{#each sessions.current as entry (entry.sessionId)}
							{@const current = attached?.sessionId === entry.sessionId}
							<Item.Root size="xs" variant={current ? 'muted' : 'outline'}>
								<Item.Content class="min-w-0">
									<Item.Title class="w-full truncate font-mono" title={entry.sessionId}>
										{entry.sessionId}
									</Item.Title>
									<Item.Description>
										{formatTimestamp(entry.openedAt)} · {m.editor_shell_viewers({
											count: entry.viewers
										})}
									</Item.Description>
									{#if entry.running}
										<Item.Description class="truncate font-mono">$ {entry.running}</Item.Description>
									{/if}
								</Item.Content>
								<Item.Actions class="w-full">
									<Button
										size="xs"
										variant="outline"
										disabled={current}
										onclick={() => (attached = entry)}
									>
										{m.editor_shell_attach()}
									</Button>
									<Button
										size="xs"
										variant="destructive"
										disabled={writes.pending}
										onclick={() => (closing = entry)}
									>
										{m.editor_shell_close_session()}
									</Button>
								</Item.Actions>
							</Item.Root>
						{/each}
					</Item.Group>
				{/if}
			</div>

			<div class="flex min-h-0 min-w-0 flex-1 flex-col">
				{#if attached}
					{#key attached.sessionId}
						<ShellTerminal serverId={server.id} session={attached} />
					{/key}
				{:else}
					<Empty.Root class="border">
						<Empty.Header>
							<Empty.Media variant="icon"><SquareTerminalIcon /></Empty.Media>
							<Empty.Title>{m.editor_shell_pick_title()}</Empty.Title>
							<Empty.Description>{m.editor_shell_pick_description()}</Empty.Description>
						</Empty.Header>
					</Empty.Root>
				{/if}
			</div>
		</div>
	</Dialog.Content>
</Dialog.Root>

<ConfirmDeleteDialog
	bind:open={() => closing !== null, next => {
		if (!next) closing = null;
	}}
	title={m.editor_shell_close_title()}
	description={m.editor_shell_close_description()}
	confirm={m.editor_shell_close_session()}
	pending={writes.pending}
	onconfirm={closeSession}
/>
