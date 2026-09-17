INSERT INTO relay_certificate
    (id, pod, sni, private_key_pem, certificate_pem, not_before, not_after, version)
VALUES ($1, $2, $3, $4, $5, $6, $7, 1)
ON CONFLICT ON CONSTRAINT relay_certificate_pod_key DO UPDATE
    SET sni = EXCLUDED.sni, private_key_pem = EXCLUDED.private_key_pem,
        certificate_pem = EXCLUDED.certificate_pem,
        not_before = EXCLUDED.not_before, not_after = EXCLUDED.not_after,
        version = relay_certificate.version + 1
RETURNING
    id AS "id: RelayCertificateId",
    pod AS "pod: PodId",
    sni,
    private_key_pem,
    certificate_pem,
    not_before,
    not_after,
    version
