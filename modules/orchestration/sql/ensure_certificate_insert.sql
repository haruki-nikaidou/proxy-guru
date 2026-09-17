-- `DO NOTHING` returns no row when the pair already exists.
INSERT INTO certificate
    (id, sni, dns_provider, domain_id, acme_directory, status, created_at)
VALUES ($1, $2, $3, $4, $5, 'pending', $6)
ON CONFLICT ON CONSTRAINT certificate_sni_acme_directory_key DO NOTHING
RETURNING
    id AS "id: CertificateId",
    sni,
    dns_provider AS "dns_provider: DnsProviderId",
    domain_id,
    acme_directory,
    status AS "status: CertificateStatus",
    acme_account_key,
    private_key_pem,
    full_chain_pem,
    not_before,
    not_after,
    last_error,
    last_attempt_at,
    version,
    created_at
