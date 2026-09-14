<script lang="ts">
import ShieldCheckIcon from '@lucide/svelte/icons/shield-check';
import * as Empty from '#lib/components/ui/empty/index.js';
import * as Tabs from '#lib/components/ui/tabs/index.js';
import { m } from '#lib/paraglide/messages.js';
import { canManageTls } from '#lib/permissions.js';
import CertificatesPanel from './CertificatesPanel.svelte';
import DnsProvidersPanel from './DnsProvidersPanel.svelte';
import type { PageProps } from './$types.js';

let { data }: PageProps = $props();

// Both panels are Admin only in the control plane, so a lesser role never
// renders one and never fires an RPC that would come back PERMISSION_DENIED.
const manageable = $derived(canManageTls(data.identity.role));

const tabs = ['dns-providers', 'certificates'] as const;
let value = $state('');
const active = $derived(tabs.includes(value as (typeof tabs)[number]) ? value : tabs[0]);
</script>

<div>
	<h1 class="text-2xl font-semibold">{m.tls_title()}</h1>
	<p class="text-sm text-muted-foreground">{m.tls_description()}</p>
</div>

{#if manageable}
	<Tabs.Root value={active} onValueChange={(next) => (value = next)}>
		<Tabs.List>
			{#each tabs as tab (tab)}
				<Tabs.Trigger value={tab}>
					{#if tab === 'dns-providers'}{m.tls_tab_dns_providers()}
					{:else}{m.tls_tab_certificates()}{/if}
				</Tabs.Trigger>
			{/each}
		</Tabs.List>

		<Tabs.Content value="dns-providers"><DnsProvidersPanel /></Tabs.Content>
		<Tabs.Content value="certificates"><CertificatesPanel /></Tabs.Content>
	</Tabs.Root>
{:else}
	<Empty.Root>
		<Empty.Header>
			<Empty.Media variant="icon"><ShieldCheckIcon /></Empty.Media>
			<Empty.Title>{m.tls_admin_only_title()}</Empty.Title>
			<Empty.Description>{m.tls_admin_only_description()}</Empty.Description>
		</Empty.Header>
	</Empty.Root>
{/if}
