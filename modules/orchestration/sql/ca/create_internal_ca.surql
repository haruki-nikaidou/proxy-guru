-- Statement 0 is BEGIN, 1 the LET; the RETURN is statement 2.
BEGIN TRANSACTION;
LET $created = IF record::exists($id) { [] } ELSE {
    CREATE $id CONTENT {
        certificate_pem: $certificate_pem, private_key_pem: $private_key_pem,
        not_after: $not_after, created_at: $now
    }
};
RETURN array::len($created) > 0;
COMMIT TRANSACTION;
