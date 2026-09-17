UPDATE relay_certificate
SET sni = $2, private_key_pem = $3, certificate_pem = $4,
    not_before = $5, not_after = $6, version = version + 1
WHERE pod = $1 AND version = $7
RETURNING
    id AS "id: RelayCertificateId",
    pod AS "pod: PodId",
    sni,
    private_key_pem,
    certificate_pem,
    not_before,
    not_after,
    version
