<script lang="ts">
import PlusIcon from '@lucide/svelte/icons/plus';
import { toast } from 'svelte-sonner';
import BoundaryError from '#lib/components/BoundaryError.svelte';
import { Button } from '#lib/components/ui/button/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import * as Table from '#lib/components/ui/table/index.js';
import * as Tooltip from '#lib/components/ui/tooltip/index.js';
import {
	ASSIGNABLE_ROLES,
	type AccountRow,
	type AssignableRole,
	type Identity
} from '#lib/dto/identity.js';
import { accountMutationBlock } from '#lib/guards.js';
import { errorMessage } from '#lib/i18n/codes.js';
import { roleLabel } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import { reportError } from '#lib/report.js';
import ConfirmDeleteDialog from '#lib/components/ConfirmDeleteDialog.svelte';
import CreateAccountDialog from './CreateAccountDialog.svelte';
import { deleteAccount, listAccounts, setAccountRole } from './accounts.remote.js';

let { identity }: { identity: Identity } = $props();

const accounts = listAccounts();
let createOpen = $state(false);
let deleteTarget = $state<AccountRow | null>(null);

async function changeRole(row: AccountRow, role: AssignableRole) {
	try {
		await setAccountRole({ accountId: row.id, role });
		toast.success(m.accounts_role_updated());
	} catch (err) {
		reportError(err);
		// The refreshed query is authoritative; re-reading it snaps the select back.
		await listAccounts().refresh();
	}
}

async function confirmDelete(row: AccountRow) {
	try {
		await deleteAccount({ accountId: row.id });
		deleteTarget = null;
		toast.success(m.accounts_deleted());
	} catch (err) {
		reportError(err);
	}
}
</script>

<Card.Root>
	<Card.Header>
		<Card.Title>{m.accounts_title()}</Card.Title>
		<Card.Description>{m.accounts_description()}</Card.Description>
		<Card.Action>
			<Button size="sm" onclick={() => (createOpen = true)}>
				<PlusIcon />
				{m.accounts_create()}
			</Button>
		</Card.Action>
	</Card.Header>

	<Card.Content>
		{#if accounts.current === undefined && accounts.error}
			<BoundaryError error={accounts.error} retry variant="inline" />
		{:else if accounts.loading}
			<Skeleton class="h-40 w-full" />
		{:else}
			{@const rows = accounts.current ?? []}
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>{m.accounts_email()}</Table.Head>
						<Table.Head>{m.accounts_role()}</Table.Head>
						<Table.Head class="text-right">{m.common_actions()}</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each rows as row (row.id)}
						{@const roleBlock = accountMutationBlock(identity.accountId, rows, row.id, {
							kind: 'set_role',
							role: 'observer'
						})}
						{@const deleteBlock = accountMutationBlock(identity.accountId, rows, row.id, {
							kind: 'delete'
						})}
						<Table.Row>
							<Table.Cell class="font-medium">{row.email}</Table.Cell>
							<Table.Cell>
								<Tooltip.Root>
									<Tooltip.Trigger>
										{#snippet child({ props })}
											<span {...props} class="inline-block">
												<Select.Root
													type="single"
													value={row.role}
													disabled={roleBlock !== null}
													onValueChange={(next) => changeRole(row, next as AssignableRole)}
												>
													<Select.Trigger class="w-40">{roleLabel(row.role)}</Select.Trigger>
													<Select.Content>
														<Select.Group>
															{#each ASSIGNABLE_ROLES as assignable (assignable)}
																<Select.Item value={assignable} label={roleLabel(assignable)}>
																	{roleLabel(assignable)}
																</Select.Item>
															{/each}
														</Select.Group>
													</Select.Content>
												</Select.Root>
											</span>
										{/snippet}
									</Tooltip.Trigger>
									{#if roleBlock}
										<Tooltip.Content>{errorMessage(roleBlock, '')}</Tooltip.Content>
									{/if}
								</Tooltip.Root>
							</Table.Cell>
							<Table.Cell class="text-right">
								<Tooltip.Root>
									<Tooltip.Trigger>
										{#snippet child({ props })}
											<span {...props}>
												<Button
													variant="ghost"
													size="sm"
													disabled={deleteBlock !== null}
													onclick={() => (deleteTarget = row)}
												>
													{m.common_delete()}
												</Button>
											</span>
										{/snippet}
									</Tooltip.Trigger>
									{#if deleteBlock}
										<Tooltip.Content>{errorMessage(deleteBlock, '')}</Tooltip.Content>
									{/if}
								</Tooltip.Root>
							</Table.Cell>
						</Table.Row>
					{:else}
						<Table.Row>
							<Table.Cell colspan={3} class="text-muted-foreground">
								{m.accounts_empty()}
							</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		{/if}
	</Card.Content>
</Card.Root>

<CreateAccountDialog bind:open={createOpen} />

<ConfirmDeleteDialog
	bind:open={() => deleteTarget !== null, next => {
		if (!next) deleteTarget = null;
	}}
	title={m.accounts_delete_title()}
	description={m.accounts_delete_description({ email: deleteTarget?.email ?? '' })}
	onconfirm={() => deleteTarget && confirmDelete(deleteTarget)}
/>
