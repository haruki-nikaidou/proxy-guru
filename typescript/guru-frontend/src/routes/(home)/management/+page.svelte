<script lang="ts">
import * as Tabs from '#lib/components/ui/tabs/index.js';
import { canManageAccounts, canManageApiKeys, canManageConfig } from '#lib/permissions.js';
import { m } from '#lib/paraglide/messages.js';
import AccountsPanel from './AccountsPanel.svelte';
import ApiKeysPanel from './ApiKeysPanel.svelte';
import ConfigPanel from './ConfigPanel.svelte';
import ProfilePanel from './ProfilePanel.svelte';
import type { PageProps } from './$types.js';

let { data }: PageProps = $props();

const identity = $derived(data.identity);
// Only render tabs the role may use, so no panel fires an RPC that would be
// rejected with PERMISSION_DENIED.
const tabs = $derived([
	...(canManageAccounts(identity.role) ? (['accounts'] as const) : []),
	...(canManageApiKeys(identity.role) ? (['api-keys'] as const) : []),
	...(canManageConfig(identity.role) ? (['config'] as const) : []),
	'profile' as const
]);

let value = $state('');
const active = $derived(tabs.includes(value as (typeof tabs)[number]) ? value : tabs[0]);
</script>

<div>
	<h1 class="text-2xl font-semibold">{m.management_title()}</h1>
	<p class="text-sm text-muted-foreground">{m.management_description()}</p>
</div>

<Tabs.Root value={active} onValueChange={(next) => (value = next)}>
	<Tabs.List>
		{#each tabs as tab (tab)}
			<Tabs.Trigger value={tab}>
				{#if tab === 'accounts'}{m.management_tab_accounts()}
				{:else if tab === 'api-keys'}{m.management_tab_api_keys()}
				{:else if tab === 'config'}{m.management_tab_config()}
				{:else}{m.management_tab_profile()}{/if}
			</Tabs.Trigger>
		{/each}
	</Tabs.List>

	{#if canManageAccounts(identity.role)}
		<Tabs.Content value="accounts"><AccountsPanel {identity} /></Tabs.Content>
	{/if}
	{#if canManageApiKeys(identity.role)}
		<Tabs.Content value="api-keys"><ApiKeysPanel /></Tabs.Content>
	{/if}
	{#if canManageConfig(identity.role)}
		<Tabs.Content value="config"><ConfigPanel /></Tabs.Content>
	{/if}
	<Tabs.Content value="profile"><ProfilePanel {identity} /></Tabs.Content>
</Tabs.Root>
