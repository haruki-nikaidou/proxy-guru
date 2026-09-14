/**
 * The ACME plumbing behind an Entry node's TLS config: DNS providers and the
 * certificates the derivation pass creates from them. Protobuf never reaches
 * the client, so every enum is a string union here.
 *
 * The provider's API secret is **write-only**: it is sent on create (and on
 * update, when it is being replaced) and is never returned by the control
 * plane, so no DTO below carries it.
 */

export type DnsProviderKindName = 'cloudflare' | 'vercel';

export type DnsProviderDto = {
	id: string;
	name: string;
	provider: DnsProviderKindName;
	/**
	 * Unused for Cloudflare (the zone id lives on the Entry's
	 * `TlsConfig.domain_id`); the team id — or `''` for a personal account —
	 * for Vercel.
	 */
	accountId: string;
	/** RFC3339, or `''` when absent. */
	createdAt: string;
};

export type CertificateStatusName = 'unknown' | 'pending' | 'issued' | 'failed';

/**
 * One certificate row, identified by `(sni, acmeDirectory)`. The dashboard
 * never issues one: the derivation pass creates the row from an Entry node's
 * TLS config. Key material is never returned.
 */
export type CertificateDto = {
	id: string;
	sni: string;
	dnsProviderId: string;
	domainId: string;
	/** `''` means the installation default ACME directory. */
	acmeDirectory: string;
	status: CertificateStatusName;
	/** RFC3339 timestamps, or `''` while the certificate has never been issued. */
	notBefore: string;
	notAfter: string;
	lastError: string;
	lastAttemptAt: string;
};
