-- Statement 0 is BEGIN, 1-2 the LETs; the RETURN is statement 3.
-- The UPDATE returns an array indexed with [0] because `UPDATE ONLY` errors when
-- the WHERE fences the write out, which is a lost race and not a failure.
BEGIN TRANSACTION;
LET $existing = (SELECT VALUE id FROM relay_certificate WHERE pod = $pod LIMIT 1)[0];
LET $row = IF $existing = NONE {
    IF $expected_version = NONE {
        CREATE ONLY relay_certificate CONTENT {
            pod: $pod, sni: $sni, private_key_pem: $private_key_pem,
            certificate_pem: $certificate_pem, not_before: $not_before,
            not_after: $not_after, version: 1
        }
    }
} ELSE {
    (UPDATE $existing SET
        sni = $sni, private_key_pem = $private_key_pem,
        certificate_pem = $certificate_pem, not_before = $not_before,
        not_after = $not_after, version += 1
    WHERE $expected_version = NONE OR version = $expected_version
    RETURN AFTER)[0]
};
RETURN $row;
COMMIT TRANSACTION;
