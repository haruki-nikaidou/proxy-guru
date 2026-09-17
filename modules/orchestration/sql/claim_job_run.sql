INSERT INTO orchestration_job_run (id, last_run_at, last_signal_tick)
VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE
    SET last_run_at = EXCLUDED.last_run_at, last_signal_tick = EXCLUDED.last_signal_tick
    WHERE orchestration_job_run.last_signal_tick < EXCLUDED.last_signal_tick
      AND orchestration_job_run.last_signal_tick <= $4
RETURNING id AS "id: JobRunId"
