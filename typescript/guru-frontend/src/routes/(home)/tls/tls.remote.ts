import { CertificateStatus, DnsProviderKind } from 'app-protobuf/orchestration/orchestration';
import * as v from 'valibot';
import type {
	CertificateDto,
	CertificateStatusName,
	DnsProviderDto,
	DnsProviderKindName
} from '#lib/dto/tls.js';
import { callGrpc } from '#lib/server/errors.js';
import { orchestrationClient } from '#lib/server/grpc.js';
import { requireSessionId, sessionMetadata } from '#lib/server/session.js';
import { command, form, query } from '$app/server';

// Every endpoint below is Admin only in `modules/orchestration`; the control
// plane stays authoritative and this layer does not re-check the role.
const idSchema = v.pipe(v.string(), v.minLength(1, 'id_required'));
const providerNameSchema = v.pipe(
	v.string(),
	v.trim(),
	v.minLength(1, 'dns_provider_name_required'),
	v.maxLength(64, 'dns_provider_name_too_long')
);
const providerKindSchema = v.picklist(['cloudflare', 'vercel'] as const, 'dns_provider_invalid');
// Unused for Cloudflare, and optional (personal account) for Vercel.
const accountIdSchema = v.optional(v.pipe(v.string(), v.trim()), '');
const newSecretSchema = v.pipe(v.string(), v.trim(), v.minLength(1, 'dns_api_secret_required'));
/** On update an empty secret is sent verbatim: the backend keeps the stored one. */
const keptSecretSchema = v.optional(v.pipe(v.string(), v.trim()), '');

// A stored row always carries a valid kind, so the default is unreachable in
// practice; it maps to the backend's own default rather than inventing a state.
function toProviderKind(value: DnsProviderKind): DnsProviderKindName {
	switch (value) {
		case DnsProviderKind.DNS_VERCEL:
			return 'vercel';
		default:
			return 'cloudflare';
	}
}
function fromProviderKind(value: DnsProviderKindName): DnsProviderKind {
	switch (value) {
		case 'vercel':
			return DnsProviderKind.DNS_VERCEL;
		default:
			return DnsProviderKind.DNS_CLOUDFLARE;
	}
}
function toCertificateStatus(value: CertificateStatus): CertificateStatusName {
	switch (value) {
		case CertificateStatus.CERTIFICATE_PENDING:
			return 'pending';
		case CertificateStatus.CERTIFICATE_ISSUED:
			return 'issued';
		case CertificateStatus.CERTIFICATE_FAILED:
			return 'failed';
		default:
			return 'unknown';
	}
}

/** The `api_secret` is write-only: the reply never carries it. */
export const listDnsProviders = query(async (): Promise<DnsProviderDto[]> => {
	const metadata = sessionMetadata(requireSessionId());
	const { providers } = await callGrpc(() =>
		orchestrationClient().listDnsProviders({}, { metadata })
	);
	return providers
		.map(provider => ({
			id: provider.id,
			name: provider.name,
			provider: toProviderKind(provider.provider),
			accountId: provider.accountId,
			createdAt: provider.createdAt
		}))
		.sort((a, b) => a.name.localeCompare(b.name));
});

export const createDnsProvider = form(
	v.object({
		name: providerNameSchema,
		provider: providerKindSchema,
		accountId: accountIdSchema,
		apiSecret: newSecretSchema
	}),
	async ({ name, provider, accountId, apiSecret }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() =>
			orchestrationClient().createDnsProvider(
				{ name, provider: fromProviderKind(provider), accountId, apiSecret },
				{ metadata }
			)
		);
		await listDnsProviders().refresh();
		return { ok: true as const };
	}
);

export const updateDnsProvider = form(
	v.object({
		dnsProviderId: idSchema,
		name: providerNameSchema,
		accountId: accountIdSchema,
		apiSecret: keptSecretSchema
	}),
	async ({ dnsProviderId, name, accountId, apiSecret }) => {
		const metadata = sessionMetadata(requireSessionId());
		// The kind is immutable; an empty `apiSecret` keeps the stored secret.
		await callGrpc(() =>
			orchestrationClient().updateDnsProvider(
				{ dnsProviderId, name, accountId, apiSecret },
				{ metadata }
			)
		);
		await listDnsProviders().refresh();
		return { ok: true as const };
	}
);

/**
 * Refused with FAILED_PRECONDITION while an Entry still references the
 * provider; `callGrpc` turns that into a 400 carrying the control plane's own
 * English text, which the panel shows verbatim.
 */
export const deleteDnsProvider = command(
	v.object({ dnsProviderId: idSchema }),
	async ({ dnsProviderId }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() => orchestrationClient().deleteDnsProvider({ dnsProviderId }, { metadata }));
		await listDnsProviders().refresh();
		return { ok: true as const };
	}
);

/** Certificate rows are created by the derivation pass, never by the dashboard. */
export const listCertificates = query(async (): Promise<CertificateDto[]> => {
	const metadata = sessionMetadata(requireSessionId());
	const { certificates } = await callGrpc(() =>
		orchestrationClient().listCertificates({}, { metadata })
	);
	return certificates
		.map(certificate => ({
			id: certificate.id,
			sni: certificate.sni,
			dnsProviderId: certificate.dnsProviderId,
			domainId: certificate.domainId,
			acmeDirectory: certificate.acmeDirectory,
			status: toCertificateStatus(certificate.status),
			notBefore: certificate.notBefore,
			notAfter: certificate.notAfter,
			lastError: certificate.lastError,
			lastAttemptAt: certificate.lastAttemptAt
		}))
		.sort((a, b) => a.sni.localeCompare(b.sni));
});

/** Clears a failure, or forces a renewal, so the next pass picks the row up. */
export const retryCertificate = command(
	v.object({ certificateId: idSchema }),
	async ({ certificateId }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() => orchestrationClient().retryCertificate({ certificateId }, { metadata }));
		await listCertificates().refresh();
		return { ok: true as const };
	}
);

/** Refused while an Entry still references the certificate's `(sni, acme_directory)`. */
export const deleteCertificate = command(
	v.object({ certificateId: idSchema }),
	async ({ certificateId }) => {
		const metadata = sessionMetadata(requireSessionId());
		await callGrpc(() => orchestrationClient().deleteCertificate({ certificateId }, { metadata }));
		await listCertificates().refresh();
		return { ok: true as const };
	}
);
