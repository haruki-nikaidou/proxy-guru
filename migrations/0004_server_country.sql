-- The country of a server's IPv4 address, looked up by the master rather than
-- reported by the worker. `country_address` is the IPv4 the answer belongs to, so
-- an answer for an address the server no longer has is never shown;
-- `country_checked_at` is the last attempt, successful or not, which spaces out
-- the retries of a lookup that failed.
ALTER TABLE orchestration_server
    ADD COLUMN country            text,
    ADD COLUMN country_address    text,
    ADD COLUMN country_checked_at timestamptz;

-- Workers no longer look the country up themselves.
UPDATE orchestration_server
   SET reported_addresses = reported_addresses - 'country'
 WHERE reported_addresses ? 'country';
