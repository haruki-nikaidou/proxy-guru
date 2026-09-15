UPDATE $id SET
    status = 'issued',
    acme_account_key = $acme_account_key,
    private_key_pem = $private_key_pem,
    full_chain_pem = $full_chain_pem,
    not_before = $not_before,
    not_after = $not_after,
    last_error = NONE,
    last_attempt_at = $now,
    version += 1
RETURN AFTER;
