<script lang="ts">
import EyeIcon from '@lucide/svelte/icons/eye';
import EyeOffIcon from '@lucide/svelte/icons/eye-off';
import { untrack } from 'svelte';
import { toast } from 'svelte-sonner';
import { Button } from '#lib/components/ui/button/index.js';
import * as Dialog from '#lib/components/ui/dialog/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import * as InputGroup from '#lib/components/ui/input-group/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import type { DnsProviderDto, DnsProviderKindName } from '#lib/dto/tls.js';
import { errorText, issueMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';
import { createDnsProvider, updateDnsProvider } from './tls.remote.js';

let {
	mode,
	provider,
	open = $bindable(false)
}: { mode: 'create' | 'edit'; provider?: DnsProviderDto; open?: boolean } = $props();

type ProviderForm = typeof createDnsProvider | ReturnType<typeof updateDnsProvider.for>;

const PROVIDER_KINDS: DnsProviderKindName[] = ['cloudflare', 'vercel'];

function kindLabel(kind: DnsProviderKindName): string {
	switch (kind) {
		case 'vercel':
			return m.tls_provider_kind_vercel();
		default:
			return m.tls_provider_kind_cloudflare();
	}
}

// `.for(id)` keeps every row-level edit form independent of the others.
const form: ProviderForm = $derived(
	mode === 'edit' && provider ? updateDnsProvider.for(provider.id) : createDnsProvider
);

// The kind is immutable once the provider exists, so it is a field of the
// create form only; it must never submit empty (the backend rejects
// DNS_PROVIDER_KIND_UNSPECIFIED with INVALID_ARGUMENT).
const kindProps = $derived(createDnsProvider.fields.provider.as('select', 'cloudflare'));
const selectedKind: DnsProviderKindName = $derived(
	mode === 'edit' && provider ? provider.provider : (kindProps.value as DnsProviderKindName)
);

let revealSecret = $state(false);

// Seed once per open: `.set()` writes the same field the effect reads, so an
// unguarded effect would clobber every keystroke.
let seededFor = $state('');
$effect(() => {
	if (!open) {
		seededFor = '';
		revealSecret = false;
		return;
	}
	if (mode !== 'edit' || !provider) return;
	if (seededFor === provider.id) return;
	seededFor = provider.id;
	const { name, accountId } = provider;
	untrack(() => {
		form.fields.name.set(name);
		form.fields.accountId.set(accountId);
	});
});

$effect(() => {
	if (!form.result?.ok) return;
	open = false;
	toast.success(mode === 'edit' ? m.tls_provider_updated() : m.tls_provider_created());
});

async function submit(current: { submit(): Promise<boolean> }) {
	try {
		await current.submit();
	} catch (err) {
		// A refusal (duplicate name, unknown provider) arrives as actionable
		// English text from the control plane.
		toast.error(errorText(err));
	}
}
</script>

{#snippet body(current: ProviderForm)}
	{@const nameIssues = current.fields.name.issues()}
	{@const accountIssues = current.fields.accountId.issues()}
	{@const secretIssues = current.fields.apiSecret.issues()}
	<Field.FieldGroup>
		<Field.Field data-invalid={nameIssues !== undefined}>
			<Field.FieldLabel for="dns-provider-name">{m.tls_provider_name()}</Field.FieldLabel>
			<Input
				id="dns-provider-name"
				{...current.fields.name.as('text')}
				aria-invalid={nameIssues !== undefined}
			/>
			{#each nameIssues ?? [] as issue (issue.message)}
				<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
			{/each}
		</Field.Field>

		<Field.Field>
			<Field.FieldLabel for="dns-provider-kind">{m.tls_provider_kind()}</Field.FieldLabel>
			{#if mode === 'edit'}
				<Input id="dns-provider-kind" readonly value={kindLabel(selectedKind)} />
				<Field.FieldDescription>{m.tls_provider_kind_immutable()}</Field.FieldDescription>
			{:else}
				<Select.Root
					type="single"
					name={kindProps.name}
					value={kindProps.value}
					onValueChange={(next) => {
						kindProps.value = next;
					}}
				>
					<Select.Trigger id="dns-provider-kind">{kindLabel(selectedKind)}</Select.Trigger>
					<Select.Content>
						<Select.Group>
							{#each PROVIDER_KINDS as kind (kind)}
								<Select.Item value={kind} label={kindLabel(kind)}>{kindLabel(kind)}</Select.Item>
							{/each}
						</Select.Group>
					</Select.Content>
				</Select.Root>
			{/if}
		</Field.Field>

		<Field.Field data-invalid={accountIssues !== undefined}>
			<Field.FieldLabel for="dns-provider-account">{m.tls_provider_account_id()}</Field.FieldLabel>
			<Input
				id="dns-provider-account"
				{...current.fields.accountId.as('text')}
				aria-invalid={accountIssues !== undefined}
			/>
			<Field.FieldDescription>
				{selectedKind === 'cloudflare'
					? m.tls_provider_account_hint_cloudflare()
					: m.tls_provider_account_hint_vercel()}
			</Field.FieldDescription>
			{#each accountIssues ?? [] as issue (issue.message)}
				<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
			{/each}
		</Field.Field>

		<Field.Field data-invalid={secretIssues !== undefined}>
			<Field.FieldLabel for="dns-provider-secret">{m.tls_provider_api_secret()}</Field.FieldLabel>
			<InputGroup.Root>
				<InputGroup.Input
					id="dns-provider-secret"
					autocomplete="off"
					{...current.fields.apiSecret.as('password')}
					type={revealSecret ? 'text' : 'password'}
					aria-invalid={secretIssues !== undefined}
				/>
				<InputGroup.Addon align="inline-end">
					<Button
						type="button"
						variant="ghost"
						size="icon"
						aria-label={revealSecret ? m.tls_provider_secret_hide() : m.tls_provider_secret_show()}
						onclick={() => (revealSecret = !revealSecret)}
					>
						{#if revealSecret}<EyeOffIcon />{:else}<EyeIcon />{/if}
					</Button>
				</InputGroup.Addon>
			</InputGroup.Root>
			<Field.FieldDescription>
				{mode === 'edit' ? m.tls_provider_secret_hint_edit() : m.tls_provider_secret_hint_create()}
			</Field.FieldDescription>
			{#each secretIssues ?? [] as issue (issue.message)}
				<Field.FieldError>{issueMessage(issue.message)}</Field.FieldError>
			{/each}
		</Field.Field>
	</Field.FieldGroup>

	<Dialog.Footer class="mt-6">
		<Button type="button" variant="outline" onclick={() => (open = false)}>
			{m.common_cancel()}
		</Button>
		<Button type="submit" disabled={current.pending > 0}>
			{#if current.pending > 0}<Spinner data-icon="inline-start" />{/if}
			{mode === 'edit' ? m.common_save() : m.common_create()}
		</Button>
	</Dialog.Footer>
{/snippet}

<Dialog.Root bind:open>
	<Dialog.Content>
		<Dialog.Header>
			<Dialog.Title>
				{mode === 'edit' ? m.tls_provider_edit_title() : m.tls_provider_create_title()}
			</Dialog.Title>
			<Dialog.Description>
				{mode === 'edit'
					? m.tls_provider_edit_description()
					: m.tls_provider_create_description()}
			</Dialog.Description>
		</Dialog.Header>

		{#if mode === 'edit' && provider}
			{@const edit = updateDnsProvider.for(provider.id)}
			<form {...edit.enhance((instance) => submit(instance))}>
				<input {...edit.fields.dnsProviderId.as('hidden', provider.id)} />
				{@render body(edit)}
			</form>
		{:else}
			<form {...createDnsProvider.enhance((instance) => submit(instance))}>
				{@render body(createDnsProvider)}
			</form>
		{/if}
	</Dialog.Content>
</Dialog.Root>
