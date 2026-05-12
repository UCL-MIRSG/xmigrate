INSERT INTO migration_run_item (run, map)
VALUES ($run, $map)
ON CONFLICT (run, map) DO NOTHING
