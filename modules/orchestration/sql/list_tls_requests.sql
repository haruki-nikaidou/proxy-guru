SELECT
    canvas AS "canvas: CanvasId",
    -- `orchestration_pod_tls_shape` makes every `tls_*` column non-null exactly
    -- for a `client_tls` pod.
    tls_sni AS "tls_sni!",
    tls_dns_provider AS "tls_dns_provider!: DnsProviderId",
    tls_domain_id AS "tls_domain_id!",
    tls_acme_directory AS "tls_acme_directory!"
FROM orchestration_pod WHERE ingress = 'client_tls' ORDER BY id
