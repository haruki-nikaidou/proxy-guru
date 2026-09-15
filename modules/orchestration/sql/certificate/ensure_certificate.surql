-- Statement 0 is BEGIN; the RETURN is statement 3.
BEGIN TRANSACTION;
LET $existing = (SELECT * FROM certificate WHERE sni = $sni AND acme_directory = $acme_directory LIMIT 1);
LET $row = IF array::len($existing) > 0 { $existing[0] } ELSE {
    (CREATE ONLY certificate CONTENT {
        sni: $sni, dns_provider: $dns_provider, domain_id: $domain_id,
        acme_directory: $acme_directory, status: 'pending', created_at: $now
    })
};
RETURN $row;
COMMIT TRANSACTION;
