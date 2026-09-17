<script lang="ts" module>
import { SNI_PATTERN } from '#lib/tls.js';

export type TlsDraft = {
	sni: string;
	dnsProviderId: string;
	domainId: string;
	acmeDirectory: string;
};

/**
 * Mirrors the TLS schema of the remote functions so a doomed save never
 * round-trips: both sides speak the same issue codes.
 */
export function tlsIssues(tls: TlsDraft) {
	const sni = tls.sni.trim();
	const acme = tls.acmeDirectory.trim();
	return {
		sni:
			sni === ''
				? 'sni_required'
				: sni.length > 253
					? 'sni_too_long'
					: SNI_PATTERN.test(sni)
						? ''
						: 'sni_invalid',
		provider: tls.dnsProviderId.trim() === '' ? 'dns_provider_required' : '',
		domain: tls.domainId.trim() === '' ? 'domain_id_required' : '',
		acme: acme === '' || /^https:\/\/[^\s]+$/.test(acme) ? '' : 'acme_directory_invalid'
	};
}
</script>

<script lang="ts">
import ShieldOffIcon from '@lucide/svelte/icons/shield-off';
import BoundaryError from '#lib/components/BoundaryError.svelte';
import { Button } from '#lib/components/ui/button/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import type { DnsProviderDto } from '#lib/dto/tls.js';
import { issueMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';
import { listDnsProviders } from '../../../../routes/(home)/tls/tls.remote.js';

/**
 * The ACME certificate a TLS client pod terminates with. The certificate itself
 * is never created here: the derivation pass turns this config into a
 * certificate row, which is managed on `/tls`.
 */
let {
	tls = $bindable(),
	editable,
	admin
}: { tls: TlsDraft; editable: boolean; admin: boolean } = $props();

// `ListDnsProviders` is admin-only in the control plane, so the listing is never
// requested for anyone else: a maintainer sees the stored id instead of a select.
const providers = $derived(admin ? listDnsProviders() : undefined);
const selected = $derived(providers?.current?.find(entry => entry.id === tls.dnsProviderId));

const providerLabel = (provider: DnsProviderDto): string =>
	provider.provider === 'vercel'
		? m.editor_entry_tls_provider_vercel({ name: provider.name })
		: m.editor_entry_tls_provider_cloudflare({ name: provider.name });

/** The zone/domain id is named differently by each provider. */
const domainHint = $derived(
	selected === undefined
		? m.editor_entry_tls_domain_hint()
		: selected.provider === 'vercel'
			? m.editor_entry_tls_domain_hint_vercel()
			: m.editor_entry_tls_domain_hint_cloudflare()
);
const issues = $derived(tlsIssues(tls));
</script>

<Field.FieldSet class="mt-6">
	<Field.FieldLegend variant="label">{m.editor_entry_tls()}</Field.FieldLegend>
	<Field.FieldDescription>{m.editor_entry_tls_description()}</Field.FieldDescription>

	<Field.FieldGroup>
		<Field.Field>
			<Field.FieldLabel for="pod-tls-sni">{m.editor_entry_tls_sni()}</Field.FieldLabel>
			<Input id="pod-tls-sni" bind:value={tls.sni} disabled={!editable} placeholder="edge.example.com" />
			<Field.FieldDescription>{m.editor_entry_tls_sni_hint()}</Field.FieldDescription>
			{#if issues.sni}
				<Field.FieldError>{issueMessage(issues.sni)}</Field.FieldError>
			{/if}
		</Field.Field>

		<Field.Field>
			<Field.FieldLabel for="pod-tls-provider">{m.editor_entry_tls_provider()}</Field.FieldLabel>
			{#if !admin}
				<!-- `ListDnsProviders` refuses anyone but an admin, so the stored id is
				     shown verbatim rather than firing a call that would be denied. -->
				<Input id="pod-tls-provider" value={tls.dnsProviderId} disabled class="font-mono" />
				<Field.FieldDescription>{m.editor_entry_tls_provider_admin_only()}</Field.FieldDescription>
			{:else if providers?.current === undefined && providers?.error}
				<BoundaryError error={providers.error} retry variant="inline" />
			{:else if providers?.current === undefined}
				<Spinner class="size-4" />
			{:else if providers.current.length === 0}
				<Empty.Root class="border">
					<Empty.Header>
						<Empty.Media variant="icon"><ShieldOffIcon /></Empty.Media>
						<Empty.Title>{m.editor_entry_tls_no_providers()}</Empty.Title>
						<Empty.Description>{m.editor_entry_tls_no_providers_hint()}</Empty.Description>
					</Empty.Header>
					<Empty.Content>
						<Button size="sm" variant="secondary" href="/tls">{m.editor_entry_tls_manage()}</Button>
					</Empty.Content>
				</Empty.Root>
			{:else}
				<Select.Root
					type="single"
					value={tls.dnsProviderId}
					disabled={!editable}
					onValueChange={next => (tls.dnsProviderId = next)}
				>
					<Select.Trigger id="pod-tls-provider">
						{selected ? providerLabel(selected) : m.editor_entry_tls_provider_pick()}
					</Select.Trigger>
					<Select.Content>
						<Select.Group>
							{#each providers.current as provider (provider.id)}
								<Select.Item value={provider.id} label={providerLabel(provider)}>
									{providerLabel(provider)}
								</Select.Item>
							{/each}
						</Select.Group>
					</Select.Content>
				</Select.Root>
			{/if}
			{#if issues.provider}
				<Field.FieldError>{issueMessage(issues.provider)}</Field.FieldError>
			{/if}
		</Field.Field>

		<Field.Field>
			<Field.FieldLabel for="pod-tls-domain">{m.editor_entry_tls_domain()}</Field.FieldLabel>
			<Input id="pod-tls-domain" bind:value={tls.domainId} disabled={!editable} class="font-mono" />
			<Field.FieldDescription>{domainHint}</Field.FieldDescription>
			{#if issues.domain}
				<Field.FieldError>{issueMessage(issues.domain)}</Field.FieldError>
			{/if}
		</Field.Field>

		<Field.Field>
			<Field.FieldLabel for="pod-tls-acme">{m.editor_entry_tls_acme()}</Field.FieldLabel>
			<Input
				id="pod-tls-acme"
				bind:value={tls.acmeDirectory}
				disabled={!editable}
				class="font-mono"
				placeholder="https://acme-v02.api.letsencrypt.org/directory"
			/>
			<Field.FieldDescription>{m.editor_entry_tls_acme_hint()}</Field.FieldDescription>
			{#if issues.acme}
				<Field.FieldError>{issueMessage(issues.acme)}</Field.FieldError>
			{/if}
		</Field.Field>
	</Field.FieldGroup>
</Field.FieldSet>
