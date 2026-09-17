-- A server's log level is one of the five the dashboard offers, which the worker
-- now applies on every config it receives. Every row so far holds `info`.
ALTER TABLE orchestration_server
    ADD CONSTRAINT orchestration_server_log_level
    CHECK (log_level IN ('trace', 'debug', 'info', 'warn', 'error'));
