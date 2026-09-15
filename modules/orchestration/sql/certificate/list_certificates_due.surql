SELECT * FROM certificate WHERE
    (status = 'pending' AND (last_attempt_at = NONE OR last_attempt_at < $retry_before))
    OR (status = 'failed' AND (last_attempt_at = NONE OR last_attempt_at < $retry_before))
    OR (status = 'issued' AND (
        last_attempt_at = NONE
        OR (not_after != NONE AND not_after < $renew_before AND last_attempt_at < $retry_before)
    ))
ORDER BY created_at ASC;
