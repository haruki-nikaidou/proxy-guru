<script lang="ts">
import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
import TerminalIcon from '@lucide/svelte/icons/terminal';
import { getAgentRelease, requestAgentUpdate } from '#lib/components/canvas/commands.js';
import * as Alert from '#lib/components/ui/alert/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import type { ServerDto } from '#lib/dto/topology.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';
import AgentInstallDialog from './AgentInstallDialog.svelte';

/**
 * The worker on this server: the version it registered as, the version that is
 * published, and the command that installs it.
 */
let { canvasId, server, editable }: { canvasId: string; server: ServerDto; editable: boolean } =
	$props();

const writes = panelWrites();
let installOpen = $state(false);

// The published worker release, read once per panel: it decides whether an
// install command can be rendered and whether this server is behind it.
const release = getAgentRelease();
/** Mirrors the control plane's default: the name as a slug, else the record key. */
const suggestedUnit = $derived.by(() => {
	const slug = server.name
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, '-')
		.replace(/^-+|-+$/g, '')
		.slice(0, 32)
		.replace(/-+$/, '');
	return slug || server.id.slice(0, 32);
});

const updateAgent = () =>
	writes.run(
		() => requestAgentUpdate({ canvasId, serverId: server.id }),
		m.editor_agent_update_requested()
	);
</script>

<!-- The worker on this server: the version it registered as, the version that
     is published, and the command that installs it. Fenced like the rollout
     below, so a failing release read does not take the panel down. -->
<svelte:boundary>
	<div class="flex items-center gap-2">
		<h3 class="text-sm font-medium">{m.editor_agent_title()}</h3>
		<span
			class="ms-auto shrink-0 font-mono text-xs text-muted-foreground"
			title={m.editor_agent_version_title()}
		>
			{server.agentVersion ? `v${server.agentVersion}` : m.editor_agent_version_unknown()}
		</span>
	</div>
	<p class="mt-1 text-xs text-muted-foreground">{m.editor_agent_description()}</p>

	{#if release.current === undefined}
		<Skeleton class="mt-3 h-10 w-full" />
	{:else}
		{@const published = release.current}
		{@const installReady = published.version !== '' && published.baseUrlConfigured}
		{@const behind =
			server.agentVersion !== '' &&
			published.version !== '' &&
			server.agentVersion !== published.version}
		<div class="mt-2 space-y-1 text-xs text-muted-foreground">
			{#if server.agentUnit}
				<p class="font-mono">guru-worker@{server.agentUnit}</p>
			{/if}
			{#if server.agentKeyIssuedAt}
				<p>{m.editor_agent_key_issued()}: {formatTimestamp(server.agentKeyIssuedAt)}</p>
			{/if}
			{#if published.version}
				<p>
					{m.editor_agent_published()}: <span class="font-mono">v{published.version}</span>
					{#if behind}· {m.editor_agent_update_available()}{/if}
				</p>
			{:else}
				<p>{m.editor_agent_not_published()}</p>
			{/if}
			{#if !published.baseUrlConfigured}
				<p>{m.editor_agent_no_base_url()}</p>
			{/if}
		</div>
		{#if server.agentUpdateRequested}
			<p class="mt-2 flex items-center gap-2 text-xs text-muted-foreground">
				<Spinner class="size-3 shrink-0" />
				{m.editor_agent_updating({ version: server.agentUpdateRequested })}
			</p>
		{/if}
		{#if server.agentUpdateError}
			<Alert.Root variant="destructive" class="mt-2">
				<Alert.Title>{m.editor_agent_update_failed()}</Alert.Title>
				<Alert.Description class="font-mono text-xs">{server.agentUpdateError}</Alert.Description>
			</Alert.Root>
		{/if}
		{#if editable}
			<div class="mt-3 flex flex-wrap gap-2">
				{#if behind && !server.agentUpdateRequested}
					<Button size="sm" disabled={!installReady || writes.pending} onclick={updateAgent}>
						<RefreshCwIcon />
						{m.editor_agent_update({ version: published.version })}
					</Button>
				{/if}
				<Button
					size="sm"
					variant={server.agentKeyIssuedAt ? 'outline' : 'secondary'}
					disabled={!installReady || writes.pending}
					onclick={() => (installOpen = true)}
				>
					<TerminalIcon />
					{server.agentKeyIssuedAt ? m.editor_agent_reissue() : m.editor_agent_install()}
				</Button>
			</div>
		{/if}
	{/if}

	{#snippet failed(error)}
		{@const body = (error as { body?: App.Error }).body}
		<Alert.Root variant="destructive" class="mt-3">
			<Alert.Description>{errorMessage(body?.code, body?.message ?? '')}</Alert.Description>
		</Alert.Root>
	{/snippet}
</svelte:boundary>

<AgentInstallDialog
	bind:open={installOpen}
	{canvasId}
	serverId={server.id}
	unit={server.agentUnit || suggestedUnit}
	replacing={server.agentKeyIssuedAt !== ''}
/>
