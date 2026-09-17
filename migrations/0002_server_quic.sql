-- A server's side of every QUIC relay link it takes part in: congestion control,
-- the rates it sends and receives at, and optional receive windows. The master
-- derives each link's tuning from both ends' settings.
ALTER TABLE orchestration_server
    ADD COLUMN quic jsonb NOT NULL DEFAULT '{}'::jsonb;
