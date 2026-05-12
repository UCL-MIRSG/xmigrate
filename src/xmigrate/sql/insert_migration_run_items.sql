-- Bulk-insert rows from the registered DataFrame ``run_items_df``
-- (columns: run, map) into migration_run_item, ignoring duplicates.
INSERT INTO migration_run_item (run, map)
SELECT
    run,
    map
FROM run_items_df
ON CONFLICT (run, map) DO NOTHING
