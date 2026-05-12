UPDATE migration_run
SET completed_at = now()
WHERE id = $run_id
