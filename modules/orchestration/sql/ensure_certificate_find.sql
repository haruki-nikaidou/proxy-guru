-- The row that won the insert, whether it was ours or a concurrent pod's.
SELECT
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
FROM certificate WHERE sni = $1 AND acme_directory = $2
