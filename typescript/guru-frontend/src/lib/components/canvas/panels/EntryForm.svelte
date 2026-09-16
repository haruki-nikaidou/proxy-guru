<script lang="ts">
import ShieldOffIcon from '@lucide/svelte/icons/shield-off';
import { untrack } from 'svelte';
import { replaceEntrySpec, updateNodeText } from '#lib/components/canvas/commands.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import * as Field from '#lib/components/ui/field/index.js';
import { Input } from '#lib/components/ui/input/index.js';
import * as Select from '#lib/components/ui/select/index.js';
import { Spinner } from '#lib/components/ui/spinner/index.js';
import { Switch } from '#lib/components/ui/switch/index.js';
import { Textarea } from '#lib/components/ui/textarea/index.js';
import type { DnsProviderDto } from '#lib/dto/tls.js';
import type { EntryNodeDto, ProxyProtocolName } from '#lib/dto/topology.js';
import { errorMessage, issueMessage } from '#lib/i18n/codes.js';
import { m } from '#lib/paraglide/messages.js';
import { panelWrites } from '#lib/writes.svelte.js';
import { PROXY_OPTIONS, proxyLabel } from '#lib/i18n/labels.js';
import { listDnsProviders } from '../../../../routes/(home)/tls/tls.remote.js';

let {
	canvasId,
	node,
	editable,
	admin
}: { canvasId: string; node: EntryNodeDto; editable: boolean; admin: boolean } = $props();

let name = $state('');
let comment = $state('');
let proxy = $state<ProxyProtocolName>('none');
const writes = panelWrites();

// TLS termination. Disabling the switch sends `tls: null`, which clears it; the
// drafts stay so a toggle round-trip does not lose what was typed.
let tlsEnabled = $state(false);
let sni = $state('');
let dnsProviderId = $state('');
let domainId = $state('');
let acmeDirectory = $state('');

// Seed once per node: the effect writes the same state it would otherwise read
// back, so an unguarded version would clobber every keystroke.
let seededFor = $state('');
$effect(() => {
	if (seededFor === node.id) return;
	seededFor = node.id;
	const snapshot = node;
	untrack(() => {
		name = snapshot.name;
		comment = snapshot.comment;
		proxy = snapshot.receiveProxyProtocol;
		tlsEnabled = snapshot.tls !== null;
		sni = snapshot.tls?.sni ?? '';
		dnsProviderId = snapshot.tls?.dnsProviderId ?? '';
		domainId = snapshot.tls?.domainId ?? '';
		acmeDirectory = snapshot.tls?.acmeDirectory ?? '';
	});
});

// `ListDnsProviders` is admin-only in the control plane, so the listing is never
// requested for anyone else: a maintainer sees the stored id instead of a select.
const providers = $derived(admin && tlsEnabled ? listDnsProviders() : undefined);
const selected = $derived(providers?.current?.find(entry => entry.id === dnsProviderId));

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

/**
 * Mirrors `tlsSchema` in `topology.remote.ts` so a doomed save never round-trips
 * — a `command` rejects invalid arguments with a bare 400, which would say
 * nothing about which field is wrong. Both sides speak the same issue codes.
 */
const SNI_PATTERN = /^(\*\.)?[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$/i;
const sniIssue = $derived.by(() => {
	if (!tlsEnabled) return '';
	const value = sni.trim();
	if (value === '') return 'sni_required';
	if (value.length > 253) return 'sni_too_long';
	return SNI_PATTERN.test(value) ? '' : 'sni_invalid';
});
const providerIssue = $derived(
	tlsEnabled && dnsProviderId.trim() === '' ? 'dns_provider_required' : ''
);
const domainIssue = $derived(tlsEnabled && domainId.trim() === '' ? 'domain_id_required' : '');
const acmeIssue = $derived.by(() => {
	const value = acmeDirectory.trim();
	if (!tlsEnabled || value === '') return '';
	return /^https:\/\/[^\s]+$/.test(value) ? '' : 'acme_directory_invalid';
});
const invalid = $derived(
	sniIssue !== '' || providerIssue !== '' || domainIssue !== '' || acmeIssue !== ''
);

async function save() {
	await writes.run(async () => {
		await updateNodeText({ canvasId, nodeId: node.id, name, comment });
		// The certificate is never created here: the derivation pass turns this
		// config into a certificate row, which is managed on `/tls`.
		await replaceEntrySpec({
			canvasId,
			nodeId: node.id,
			receiveProxyProtocol: proxy,
			tls: tlsEnabled ? { sni, dnsProviderId, domainId, acmeDirectory } : null
		});
	}, m.editor_saved());
}
</script>

<Field.FieldGroup>
	<Field.Field>
		<Field.FieldLabel for="entry-name">{m.editor_node_name()}</Field.FieldLabel>
		<Input id="entry-name" bind:value={name} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="entry-comment">{m.editor_node_comment()}</Field.FieldLabel>
		<Textarea id="entry-comment" bind:value={comment} disabled={!editable} />
	</Field.Field>

	<Field.Field>
		<Field.FieldLabel for="entry-proxy">{m.editor_receive_proxy()}</Field.FieldLabel>
		<Select.Root
			type="single"
			value={proxy}
			disabled={!editable}
			onValueChange={next => (proxy = next as ProxyProtocolName)}
		>
			<Select.Trigger id="entry-proxy">{proxyLabel(proxy)}</Select.Trigger>
			<Select.Content>
				<Select.Group>
					{#each PROXY_OPTIONS as option (option)}
						<Select.Item value={option} label={proxyLabel(option)}>{proxyLabel(option)}</Select.Item>
					{/each}
				</Select.Group>
			</Select.Content>
		</Select.Root>
	</Field.Field>
</Field.FieldGroup>

<Field.FieldSet class="mt-6">
	<Field.FieldLegend variant="label">{m.editor_entry_tls()}</Field.FieldLegend>
	<Field.FieldDescription>{m.editor_entry_tls_description()}</Field.FieldDescription>

	<Field.FieldGroup>
		<Field.Field orientation="horizontal">
			<Field.FieldContent>
				<Field.FieldLabel for="entry-tls-enabled">{m.editor_entry_tls_enable()}</Field.FieldLabel>
				<Field.FieldDescription>{m.editor_entry_tls_enable_hint()}</Field.FieldDescription>
			</Field.FieldContent>
			<Switch id="entry-tls-enabled" bind:checked={tlsEnabled} disabled={!editable} />
		</Field.Field>

		{#if tlsEnabled}
			<Field.Field>
				<Field.FieldLabel for="entry-tls-sni">{m.editor_entry_tls_sni()}</Field.FieldLabel>
				<Input
					id="entry-tls-sni"
					bind:value={sni}
					disabled={!editable}
					placeholder="edge.example.com"
				/>
				<Field.FieldDescription>{m.editor_entry_tls_sni_hint()}</Field.FieldDescription>
				{#if sniIssue}
					<Field.FieldError>{issueMessage(sniIssue)}</Field.FieldError>
				{/if}
			</Field.Field>

			<Field.Field>
				<Field.FieldLabel for="entry-tls-provider">
					{m.editor_entry_tls_provider()}
				</Field.FieldLabel>
				{#if !admin}
					<!-- `ListDnsProviders` refuses anyone but an admin, so the stored id is
					     shown verbatim rather than firing a call that would be denied. -->
					<Input id="entry-tls-provider" value={dnsProviderId} disabled class="font-mono" />
					<Field.FieldDescription>
						{m.editor_entry_tls_provider_admin_only()}
					</Field.FieldDescription>
				{:else if providers?.current === undefined}
					<Spinner class="size-4" />
				{:else if providers.current.length === 0}
					<Empty.Root class="border">
						<Empty.Header>
							<Empty.Media variant="icon"><ShieldOffIcon /></Empty.Media>
							<Empty.Title>{m.editor_entry_tls_no_providers()}</Empty.Title>
							<Empty.Description>
								{m.editor_entry_tls_no_providers_hint()}
							</Empty.Description>
						</Empty.Header>
						<Empty.Content>
							<Button size="sm" variant="secondary" href="/tls">
								{m.editor_entry_tls_manage()}
							</Button>
						</Empty.Content>
					</Empty.Root>
				{:else}
					<Select.Root
						type="single"
						value={dnsProviderId}
						disabled={!editable}
						onValueChange={next => (dnsProviderId = next)}
					>
						<Select.Trigger id="entry-tls-provider">
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
				{#if providerIssue}
					<Field.FieldError>{issueMessage(providerIssue)}</Field.FieldError>
				{/if}
			</Field.Field>

			<Field.Field>
				<Field.FieldLabel for="entry-tls-domain">{m.editor_entry_tls_domain()}</Field.FieldLabel>
				<Input id="entry-tls-domain" bind:value={domainId} disabled={!editable} class="font-mono" />
				<Field.FieldDescription>{domainHint}</Field.FieldDescription>
				{#if domainIssue}
					<Field.FieldError>{issueMessage(domainIssue)}</Field.FieldError>
				{/if}
			</Field.Field>

			<Field.Field>
				<Field.FieldLabel for="entry-tls-acme">{m.editor_entry_tls_acme()}</Field.FieldLabel>
				<Input
					id="entry-tls-acme"
					bind:value={acmeDirectory}
					disabled={!editable}
					class="font-mono"
					placeholder="https://acme-v02.api.letsencrypt.org/directory"
				/>
				<Field.FieldDescription>{m.editor_entry_tls_acme_hint()}</Field.FieldDescription>
				{#if acmeIssue}
					<Field.FieldError>{issueMessage(acmeIssue)}</Field.FieldError>
				{/if}
			</Field.Field>
		{/if}
	</Field.FieldGroup>
</Field.FieldSet>

<Button class="mt-6 w-full" disabled={!editable || writes.pending || invalid} onclick={save}>
	{#if writes.pending}<Spinner data-icon="inline-start" />{/if}
	{m.common_save()}
</Button>
