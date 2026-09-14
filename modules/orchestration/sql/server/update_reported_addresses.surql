-- A worker's refreshed address set, accepted only from the session that
-- currently holds the server (the generation fence) and only when it differs
-- from what is stored, so an unchanged report never bumps the canvas.
-- Statement 0 is BEGIN, 1 the LET, 2 the IF; the RETURN is statement 3.
BEGIN TRANSACTION;
LET $m = (UPDATE $server SET reported_addresses = $reported
    WHERE refresh_key_generation = $generation
        AND (reported_addresses == NONE
            OR reported_addresses.public_v4 != $reported.public_v4
            OR reported_addresses.public_v6 != $reported.public_v6
            OR reported_addresses.interfaces != $reported.interfaces)
    RETURN AFTER);
IF array::len($m) > 0 { fn::orchestration_touch($canvas); };
RETURN array::len($m) > 0;
COMMIT TRANSACTION;
