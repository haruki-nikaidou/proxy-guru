<script lang="ts">
import KeyRoundIcon from '@lucide/svelte/icons/key-round';
import PlusIcon from '@lucide/svelte/icons/plus';
import { toast } from 'svelte-sonner';
import { Button } from '#lib/components/ui/button/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import * as Table from '#lib/components/ui/table/index.js';
import type { ApiKeyRow } from '#lib/dto/identity.js';
import { errorText } from '#lib/i18n/codes.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import ConfirmDeleteDialog from '#lib/components/ConfirmDeleteDialog.svelte';
import CreateApiKeyDialog from './CreateApiKeyDialog.svelte';
import { listApiKeys, revokeApiKey } from './apiKeys.remote.js';

const keys = listApiKeys();
let createOpen = $state(false);
let revokeTarget = $state<ApiKeyRow | null>(null);

async function confirmRevoke(row: ApiKeyRow) {
	try {
		await revokeApiKey({ keyId: row.id });
		revokeTarget = null;
		toast.success(m.api_keys_revoked());
	} catch (err) {
		toast.error(errorText(err));
	}
}
</script>

<Card.Root>
	<Card.Header>
		<Card.Title>{m.api_keys_title()}</Card.Title>
		<Card.Description>{m.api_keys_description()}</Card.Description>
		<Card.Action>
			<Button size="sm" onclick={() => (createOpen = true)}>
				<PlusIcon />
				{m.api_keys_create()}
			</Button>
		</Card.Action>
	</Card.Header>

	<Card.Content>
		{#if keys.loading}
			<Skeleton class="h-32 w-full" />
		{:else if (keys.current ?? []).length === 0}
			<Empty.Root>
				<Empty.Header>
					<Empty.Media variant="icon"><KeyRoundIcon /></Empty.Media>
					<Empty.Title>{m.api_keys_empty_title()}</Empty.Title>
					<Empty.Description>{m.api_keys_empty_description()}</Empty.Description>
				</Empty.Header>
			</Empty.Root>
		{:else}
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>{m.api_keys_name()}</Table.Head>
						<Table.Head>{m.api_keys_created()}</Table.Head>
						<Table.Head class="text-right">{m.common_actions()}</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each keys.current ?? [] as row (row.id)}
						<Table.Row>
							<Table.Cell class="font-medium">{row.name}</Table.Cell>
							<Table.Cell>{formatTimestamp(row.createdAt)}</Table.Cell>
							<Table.Cell class="text-right">
								<Button variant="ghost" size="sm" onclick={() => (revokeTarget = row)}>
									{m.api_keys_revoke()}
								</Button>
							</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		{/if}
	</Card.Content>
</Card.Root>

<CreateApiKeyDialog bind:open={createOpen} />

<ConfirmDeleteDialog
	bind:open={() => revokeTarget !== null, next => {
		if (!next) revokeTarget = null;
	}}
	title={m.api_keys_revoke_title()}
	description={m.api_keys_revoke_description({ name: revokeTarget?.name ?? '' })}
	confirm={m.api_keys_revoke()}
	onconfirm={() => revokeTarget && confirmRevoke(revokeTarget)}
/>
