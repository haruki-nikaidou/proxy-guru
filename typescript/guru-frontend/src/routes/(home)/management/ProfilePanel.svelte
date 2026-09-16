<script lang="ts">
import { toast } from 'svelte-sonner';
import { invalidateAll } from '$app/navigation';
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import type { Identity } from '#lib/dto/identity.js';
import { issueMessage, resultMessage } from '#lib/i18n/codes.js';
import { roleLabel } from '#lib/i18n/labels.js';
import { m } from '#lib/paraglide/messages.js';
import { changeEmail, changePassword } from './profile.remote.js';

let { identity }: { identity: Identity } = $props();

$effect(() => {
	if (!changeEmail.result?.ok) return;
	toast.success(m.profile_email_changed());
	// The sidebar renders the identity from the layout load.
	invalidateAll();
});

$effect(() => {
	if (!changePassword.result?.ok) return;
	toast.success(m.profile_password_changed());
	changePassword.fields.currentPassword.set('');
	changePassword.fields.newPassword.set('');
	changePassword.fields.confirmPassword.set('');
});

const newEmailIssues = $derived(changeEmail.fields.newEmail.issues());
const emailPasswordIssues = $derived(changeEmail.fields.currentPassword.issues());
const currentPasswordIssues = $derived(changePassword.fields.currentPassword.issues());
const newPasswordIssues = $derived(changePassword.fields.newPassword.issues());
const confirmPasswordIssues = $derived(changePassword.fields.confirmPassword.issues());

const emailError = $derived(changeEmail.result?.error);
const passwordError = $derived(changePassword.result?.error);
</script>

<div class="grid gap-6 lg:grid-cols-2">
	<Card.Root>
		<Card.Header>
			<Card.Title>{m.profile_title()}</Card.Title>
			<Card.Description>{m.profile_description()}</Card.Description>
		</Card.Header>
		<Card.Content class="flex flex-col gap-6">
			<div class="flex flex-col gap-1">
				<span class="text-sm text-muted-foreground">{m.profile_email()}</span>
				<span class="font-medium">{identity.email}</span>
			</div>
			<div class="flex flex-col items-start gap-1">
				<span class="text-sm text-muted-foreground">{m.profile_role()}</span>
				<Badge variant="secondary">{roleLabel(identity.role)}</Badge>
			</div>

			<form {...changeEmail}>
				<Field.FieldGroup>
					<Field.FieldSeparator />
					<Field.FieldTitle>{m.profile_change_email_title()}</Field.FieldTitle>
					<Field.FieldDescription>{m.profile_change_email_description()}</Field.FieldDescription>

					<Field.Field data-invalid={newEmailIssues !== undefined || emailError === 'email_taken'}>
						<Field.FieldLabel for="new-email">{m.profile_new_email()}</Field.FieldLabel>
						<Input
							id="new-email"
							{...changeEmail.fields.newEmail.as('email')}
							aria-invalid={newEmailIssues !== undefined || emailError === 'email_taken'}
						/>
						{#each newEmailIssues ?? [] as issue (issue.message)}
							<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
						{/each}
						{#if emailError === 'email_taken'}
							<Field.FieldError>{resultMessage(emailError)}</Field.FieldError>
						{/if}
					</Field.Field>

					<Field.Field
						data-invalid={emailPasswordIssues !== undefined || emailError === 'wrong_password'}
					>
						<Field.FieldLabel for="email-current-password">
							{m.profile_current_password()}
						</Field.FieldLabel>
						<Input
							id="email-current-password"
							autocomplete="current-password"
							{...changeEmail.fields.currentPassword.as('password')}
							aria-invalid={emailPasswordIssues !== undefined || emailError === 'wrong_password'}
						/>
						{#each emailPasswordIssues ?? [] as issue (issue.message)}
							<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
						{/each}
						{#if emailError === 'wrong_password'}
							<Field.FieldError>{resultMessage(emailError)}</Field.FieldError>
						{/if}
					</Field.Field>

					<Button type="submit" disabled={changeEmail.pending > 0}>
						{#if changeEmail.pending > 0}<Spinner data-icon="inline-start" />{/if}
						{m.profile_submit_email()}
					</Button>
				</Field.FieldGroup>
			</form>
		</Card.Content>
	</Card.Root>

	<Card.Root>
		<Card.Header>
			<Card.Title>{m.profile_change_password_title()}</Card.Title>
			<Card.Description>{m.profile_change_password_description()}</Card.Description>
		</Card.Header>
		<Card.Content>
			<form {...changePassword}>
				<Field.FieldGroup>
					<Field.Field
						data-invalid={currentPasswordIssues !== undefined || passwordError === 'wrong_password'}
					>
						<Field.FieldLabel for="current-password">
							{m.profile_current_password()}
						</Field.FieldLabel>
						<Input
							id="current-password"
							autocomplete="current-password"
							{...changePassword.fields.currentPassword.as('password')}
							aria-invalid={currentPasswordIssues !== undefined ||
								passwordError === 'wrong_password'}
						/>
						{#each currentPasswordIssues ?? [] as issue (issue.message)}
							<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
						{/each}
						{#if passwordError === 'wrong_password'}
							<Field.FieldError>{resultMessage(passwordError)}</Field.FieldError>
						{/if}
					</Field.Field>

					<Field.Field data-invalid={newPasswordIssues !== undefined}>
						<Field.FieldLabel for="new-password">{m.profile_new_password()}</Field.FieldLabel>
						<Input
							id="new-password"
							autocomplete="new-password"
							{...changePassword.fields.newPassword.as('password')}
							aria-invalid={newPasswordIssues !== undefined}
						/>
						{#each newPasswordIssues ?? [] as issue (issue.message)}
							<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
						{/each}
					</Field.Field>

					<Field.Field data-invalid={confirmPasswordIssues !== undefined}>
						<Field.FieldLabel for="confirm-password">
							{m.profile_confirm_password()}
						</Field.FieldLabel>
						<Input
							id="confirm-password"
							autocomplete="new-password"
							{...changePassword.fields.confirmPassword.as('password')}
							aria-invalid={confirmPasswordIssues !== undefined}
						/>
						{#each confirmPasswordIssues ?? [] as issue (issue.message)}
							<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
						{/each}
					</Field.Field>

					<Button type="submit" disabled={changePassword.pending > 0}>
						{#if changePassword.pending > 0}<Spinner data-icon="inline-start" />{/if}
						{m.profile_submit_password()}
					</Button>
				</Field.FieldGroup>
			</form>
		</Card.Content>
	</Card.Root>
</div>
