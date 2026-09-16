<script lang="ts">
import { toast } from 'svelte-sonner';
import { Button } from '#lib/components/ui/button/index.js';
import * as Dialog from '#lib/components/ui/dialog/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { ASSIGNABLE_ROLES } from '#lib/dto/identity.js';
import { issueMessage, resultMessage } from '#lib/i18n/codes.js';
import { roleLabel } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import { createAccount } from './accounts.remote.js';

let { open = $bindable(false) }: { open?: boolean } = $props();

$effect(() => {
	if (!createAccount.result?.ok) return;
	open = false;
	toast.success(m.accounts_created());
});

const emailIssues = $derived(createAccount.fields.email.issues());
const passwordIssues = $derived(createAccount.fields.password.issues());
// Role must be a registered remote field and never submit empty: the backend
// rejects Role.UNSPECIFIED with INVALID_ARGUMENT.
const roleProps = $derived(createAccount.fields.role.as('select', 'observer'));
</script>

<Dialog.Root bind:open>
	<Dialog.Content>
		<Dialog.Header>
			<Dialog.Title>{m.accounts_create_title()}</Dialog.Title>
			<Dialog.Description>{m.accounts_create_description()}</Dialog.Description>
		</Dialog.Header>

		<form {...createAccount}>
			<Field.FieldGroup>
				<Field.Field data-invalid={emailIssues !== undefined}>
					<Field.FieldLabel for="account-email">{m.accounts_email()}</Field.FieldLabel>
					<Input
						id="account-email"
						{...createAccount.fields.email.as('email')}
						aria-invalid={emailIssues !== undefined}
					/>
					{#each emailIssues ?? [] as issue (issue.message)}
						<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
					{/each}
				</Field.Field>

				<Field.Field data-invalid={passwordIssues !== undefined}>
					<Field.FieldLabel for="account-password">{m.accounts_password()}</Field.FieldLabel>
					<Input
						id="account-password"
						{...createAccount.fields.password.as('password')}
						aria-invalid={passwordIssues !== undefined}
					/>
					<Field.FieldDescription>{m.accounts_password_hint()}</Field.FieldDescription>
					{#each passwordIssues ?? [] as issue (issue.message)}
						<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
					{/each}
				</Field.Field>

				<Field.Field>
					<Field.FieldLabel for="account-role">{m.accounts_role()}</Field.FieldLabel>
					<Select.Root
						type="single"
						name={roleProps.name}
						value={roleProps.value}
						onValueChange={(next) => {
							roleProps.value = next;
						}}
					>
						<Select.Trigger id="account-role">{roleLabel(roleProps.value)}</Select.Trigger>
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
				</Field.Field>
			</Field.FieldGroup>

			{#if createAccount.result?.error}
				<p class="mt-4 text-sm text-destructive">
					{resultMessage(createAccount.result.error)}
				</p>
			{/if}

			<Dialog.Footer class="mt-6">
				<Button type="button" variant="outline" onclick={() => (open = false)}>
					{m.common_cancel()}
				</Button>
				<Button type="submit" disabled={createAccount.pending > 0}>
					{#if createAccount.pending > 0}<Spinner data-icon="inline-start" />{/if}
					{m.common_create()}
				</Button>
			</Dialog.Footer>
		</form>
	</Dialog.Content>
</Dialog.Root>
