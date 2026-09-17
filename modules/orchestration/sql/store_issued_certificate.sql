UPDATE certificate SET
    status = 'issued', acme_account_key = $2, private_key_pem = $3,
    full_chain_pem = $4, not_before = $5, not_after = $6,
    last_error = NULL, last_attempt_at = $7, version = version + 1
WHERE id = $1
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
