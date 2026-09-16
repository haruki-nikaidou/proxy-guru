<script lang="ts">
import ShieldCheckIcon from '@lucide/svelte/icons/shield-check';
import { toast } from 'svelte-sonner';
import * as Alert from '#lib/components/ui/alert/index.js';
import { Badge } from '#lib/components/ui/badge/index.js';
import { Button } from '#lib/components/ui/button/index.js';
import * as Card from '#lib/components/ui/card/index.js';
import * as Empty from '#lib/components/ui/empty/index.js';
import { Skeleton } from '#lib/components/ui/skeleton/index.js';
import * as Table from '#lib/components/ui/table/index.js';
import * as Tooltip from '#lib/components/ui/tooltip/index.js';
import type { CertificateDto, CertificateStatusName } from '#lib/dto/tls.js';
import { errorText } from '#lib/i18n/codes.js';
import { formatTimestamp } from '#lib/i18n/format.js';
import { m } from '#lib/paraglide/messages.js';
import ConfirmDeleteDialog from '#lib/components/ConfirmDeleteDialog.svelte';
import {
	deleteCertificate,
	listCertificates,
	listDnsProviders,
	retryCertificate
} from './tls.remote.js';

const certificates = listCertificates();
// The certificate only carries the provider's record id; its name comes from
// the provider listing this page already loads.
const providers = listDnsProviders();
const providerNames = $derived(
	new Map((providers.current ?? []).map(provider => [provider.id, provider.name]))
);

let deleteTarget = $state<CertificateDto | null>(null);
let retrying = $state('');

const DAY_MS = 86_400_000;

function statusLabel(status: CertificateStatusName): string {
	switch (status) {
		case 'pending':
			return m.tls_certificate_status_pending();
		case 'issued':
			return m.tls_certificate_status_issued();
		case 'failed':
			return m.tls_certificate_status_failed();
		default:
			return m.tls_certificate_status_unknown();
	}
}

function statusVariant(status: CertificateStatusName): 'secondary' | 'outline' | 'destructive' {
	switch (status) {
		case 'issued':
			return 'secondary';
		case 'failed':
			return 'destructive';
		default:
			return 'outline';
	}
}

/** `null` when the certificate has never been issued or the date is unusable. */
function expiryHint(notAfter: string): string | null {
	if (notAfter === '') return null;
	const parsed = new Date(notAfter);
	if (Number.isNaN(parsed.getTime())) return null;
	const days = Math.floor((parsed.getTime() - Date.now()) / DAY_MS);
	return days < 0 ? m.tls_certificate_expired() : m.tls_certificate_expires_in({ days });
}

function reportError(err: unknown) {
	toast.error(errorText(err));
}

async function retry(row: CertificateDto) {
	retrying = row.id;
	try {
		await retryCertificate({ certificateId: row.id });
		toast.success(m.tls_certificate_retried({ sni: row.sni }));
	} catch (err) {
		reportError(err);
	} finally {
		retrying = '';
	}
}

async function confirmDelete(row: CertificateDto) {
	try {
		await deleteCertificate({ certificateId: row.id });
		deleteTarget = null;
		toast.success(m.tls_certificate_deleted());
	} catch (err) {
		// Refused while an Entry still references `(sni, acme_directory)`.
		reportError(err);
	}
}
</script>

<Card.Root>
	<Card.Header>
		<Card.Title>{m.tls_certificates_title()}</Card.Title>
		<Card.Description>{m.tls_certificates_description()}</Card.Description>
	</Card.Header>

	<Card.Content>
		{#if certificates.current === undefined}
			<Skeleton class="h-32 w-full" />
		{:else if (certificates.current ?? []).length === 0}
			<Empty.Root>
				<Empty.Header>
					<Empty.Media variant="icon"><ShieldCheckIcon /></Empty.Media>
					<Empty.Title>{m.tls_certificates_empty_title()}</Empty.Title>
					<Empty.Description>{m.tls_certificates_empty_description()}</Empty.Description>
				</Empty.Header>
			</Empty.Root>
		{:else}
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>{m.tls_certificate_sni()}</Table.Head>
						<Table.Head>{m.tls_certificate_status()}</Table.Head>
						<Table.Head>{m.tls_certificate_provider()}</Table.Head>
						<Table.Head>{m.tls_certificate_domain_id()}</Table.Head>
						<Table.Head>{m.tls_certificate_directory()}</Table.Head>
						<Table.Head>{m.tls_certificate_validity()}</Table.Head>
						<Table.Head>{m.tls_certificate_last_attempt()}</Table.Head>
						<Table.Head class="text-right">{m.common_actions()}</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each certificates.current ?? [] as row (row.id)}
						{@const hint = expiryHint(row.notAfter)}
						<Table.Row>
							<Table.Cell class="font-medium">{row.sni}</Table.Cell>
							<Table.Cell>
								<Badge variant={statusVariant(row.status)}>{statusLabel(row.status)}</Badge>
							</Table.Cell>
							<Table.Cell>{providerNames.get(row.dnsProviderId) ?? row.dnsProviderId}</Table.Cell>
							<Table.Cell class="font-mono text-xs">{row.domainId}</Table.Cell>
							<Table.Cell>
								{#if row.acmeDirectory === ''}
									<span class="text-muted-foreground">{m.tls_certificate_directory_default()}</span>
								{:else}
									<span class="font-mono text-xs">{row.acmeDirectory}</span>
								{/if}
							</Table.Cell>
							<Table.Cell>
								<div class="flex flex-col">
									<span>
										{formatTimestamp(row.notBefore)} – {formatTimestamp(row.notAfter)}
									</span>
									{#if hint}
										<span class="text-xs text-muted-foreground">{hint}</span>
									{/if}
								</div>
							</Table.Cell>
							<Table.Cell>{formatTimestamp(row.lastAttemptAt)}</Table.Cell>
							<Table.Cell>
								<div class="flex justify-end gap-2">
									<Tooltip.Root>
										<Tooltip.Trigger>
											{#snippet child({ props })}
												<Button
													{...props}
													variant="ghost"
													size="sm"
													disabled={retrying === row.id}
													onclick={() => retry(row)}
												>
													{m.tls_certificate_retry()}
												</Button>
											{/snippet}
										</Tooltip.Trigger>
										<Tooltip.Content>{m.tls_certificate_retry_hint()}</Tooltip.Content>
									</Tooltip.Root>
									<Button variant="ghost" size="sm" onclick={() => (deleteTarget = row)}>
										{m.common_delete()}
									</Button>
								</div>
							</Table.Cell>
						</Table.Row>
						{#if row.status === 'failed' && row.lastError !== ''}
							<Table.Row>
								<Table.Cell colspan={8}>
									<Alert.Root variant="destructive">
										<Alert.Title>{m.tls_certificate_error_title({ sni: row.sni })}</Alert.Title>
										<Alert.Description>
											<details class="w-full">
												<summary class="cursor-pointer truncate">{row.lastError}</summary>
												<p class="mt-2 font-mono text-xs break-all whitespace-pre-wrap">
													{row.lastError}
												</p>
											</details>
										</Alert.Description>
									</Alert.Root>
								</Table.Cell>
							</Table.Row>
						{/if}
					{/each}
				</Table.Body>
			</Table.Root>
		{/if}
	</Card.Content>
</Card.Root>

<ConfirmDeleteDialog
	bind:open={() => deleteTarget !== null, next => {
		if (!next) deleteTarget = null;
	}}
	title={m.tls_certificate_delete_title()}
	description={m.tls_certificate_delete_description({ sni: deleteTarget?.sni ?? '' })}
	onconfirm={() => deleteTarget && confirmDelete(deleteTarget)}
/>
