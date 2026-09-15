-- A worker's refreshed address set, accepted only from the session that
-- currently holds the server (the generation fence) and only when it differs
-- from what is stored, so an unchanged report never bumps the view's `seq`.
-- The canvas row is never touched: the view's `seq` is what makes derivation
-- look again.
-- Statement 0 is BEGIN, 1 the LET, 2 the IF; the RETURN is statement 3.
BEGIN TRANSACTION;
LET $m = (UPDATE $server SET reported_addresses = $reported
    WHERE refresh_key_generation = $generation
        AND (reported_addresses == NONE
            OR reported_addresses.public_v4 != $reported.public_v4
            OR reported_addresses.public_v6 != $reported.public_v6
            OR reported_addresses.interfaces != $reported.interfaces
            OR reported_addresses.country != $reported.country)
    RETURN AFTER);
IF array::len($m) > 0 {
    UPDATE orchestration_server_config_view SET seq += 1 WHERE server = $server;
};
RETURN array::len($m) > 0;
COMMIT TRANSACTION;
