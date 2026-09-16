<script lang="ts">
import GlobeIcon from '@lucide/svelte/icons/globe';
import PlusIcon from '@lucide/svelte/icons/plus';
import { toast } from 'svelte-sonner';
import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button, buttonVariants } from '#lib/components/ui/button/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import * as Table from '#lib/components/ui/table/index.js';
import type { DnsProviderDto, DnsProviderKindName } from '#lib/dto/tls.js';
import { errorText } from '#lib/i18n/codes.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import DnsProviderFormDialog from './DnsProviderFormDialog.svelte';
import { deleteDnsProvider, listDnsProviders } from './tls.remote.js';

const providers = listDnsProviders();
let createOpen = $state(false);
let editTarget = $state<DnsProviderDto | null>(null);
let editOpen = $state(false);
let deleteTarget = $state<DnsProviderDto | null>(null);

function kindLabel(kind: DnsProviderKindName): string {
	switch (kind) {
		case 'vercel':
			return m.tls_provider_kind_vercel();
		default:
			return m.tls_provider_kind_cloudflare();
	}
}

async function confirmDelete(row: DnsProviderDto) {
	try {
		await deleteDnsProvider({ dnsProviderId: row.id });
		deleteTarget = null;
		toast.success(m.tls_provider_deleted());
	} catch (err) {
		// FAILED_PRECONDITION while an Entry still references the provider; the
		// control plane's own text names what is still using it.
		toast.error(errorText(err));
	}
}
</script>

<Card.Root>
	<Card.Header>
		<Card.Title>{m.tls_providers_title()}</Card.Title>
		<Card.Description>{m.tls_providers_description()}</Card.Description>
		<Card.Action>
			<Button size="sm" onclick={() => (createOpen = true)}>
				<PlusIcon />
				{m.tls_providers_create()}
			</Button>
		</Card.Action>
	</Card.Header>

	<Card.Content>
		{#if providers.current === undefined}
			<Skeleton class="h-32 w-full" />
		{:else if (providers.current ?? []).length === 0}
			<Empty.Root>
				<Empty.Header>
					<Empty.Media variant="icon"><GlobeIcon /></Empty.Media>
					<Empty.Title>{m.tls_providers_empty_title()}</Empty.Title>
					<Empty.Description>{m.tls_providers_empty_description()}</Empty.Description>
				</Empty.Header>
				<Empty.Content>
					<Button onclick={() => (createOpen = true)}>
						<PlusIcon />
						{m.tls_providers_create()}
					</Button>
				</Empty.Content>
			</Empty.Root>
		{:else}
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>{m.tls_provider_name()}</Table.Head>
						<Table.Head>{m.tls_provider_kind()}</Table.Head>
						<Table.Head>{m.tls_provider_account_id()}</Table.Head>
						<Table.Head>{m.tls_provider_created_at()}</Table.Head>
						<Table.Head class="text-right">{m.common_actions()}</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each providers.current ?? [] as row (row.id)}
						<Table.Row>
							<Table.Cell class="font-medium">{row.name}</Table.Cell>
							<Table.Cell><Badge variant="outline">{kindLabel(row.provider)}</Badge></Table.Cell>
							<Table.Cell>
								{#if row.provider === 'cloudflare'}
									<span class="text-muted-foreground">{m.tls_provider_account_unused()}</span>
								{:else if row.accountId === ''}
									<span class="text-muted-foreground">{m.tls_provider_account_personal()}</span>
								{:else}
									<span class="font-mono text-xs">{row.accountId}</span>
								{/if}
							</Table.Cell>
							<Table.Cell>{formatTimestamp(row.createdAt)}</Table.Cell>
							<Table.Cell>
								<div class="flex justify-end gap-2">
									<Button
										variant="ghost"
										size="sm"
										onclick={() => {
											editTarget = row;
											editOpen = true;
										}}
									>
										{m.tls_provider_edit()}
									</Button>
									<Button variant="ghost" size="sm" onclick={() => (deleteTarget = row)}>
										{m.common_delete()}
									</Button>
								</div>
							</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		{/if}
	</Card.Content>
</Card.Root>

<DnsProviderFormDialog mode="create" bind:open={createOpen} />

{#if editTarget}
	<DnsProviderFormDialog mode="edit" provider={editTarget} bind:open={editOpen} />
{/if}

<AlertDialog.Root
	open={deleteTarget !== null}
	onOpenChange={(next) => {
		if (!next) deleteTarget = null;
	}}
>
	<AlertDialog.Content>
		<AlertDialog.Header>
			<AlertDialog.Title>{m.tls_provider_delete_title()}</AlertDialog.Title>
			<AlertDialog.Description>
				{m.tls_provider_delete_description({ name: deleteTarget?.name ?? '' })}
			</AlertDialog.Description>
		</AlertDialog.Header>
		<AlertDialog.Footer>
			<AlertDialog.Cancel>{m.common_cancel()}</AlertDialog.Cancel>
			<AlertDialog.Action
				class={buttonVariants({ variant: 'destructive' })}
				onclick={() => deleteTarget && confirmDelete(deleteTarget)}
			>
				{m.common_delete()}
			</AlertDialog.Action>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>
