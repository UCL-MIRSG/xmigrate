-- Bulk-insert rows from the registered DataFrame ``experiment_df``
-- (columns: xnat_id, label, insert_user, insert_date, last_modified).
INSERT INTO experiment (
    instance,
    project,
    xnat_id,
    label,
    insert_user,
    insert_date,
    last_modified
)
SELECT
    $instance,
    $project,
    xnat_id,
    NULLIF(label, ''),
    NULLIF(insert_user, ''),
    NULLIF(insert_date, ''),
    NULLIF(last_modified, '')
FROM experiment_df
ON CONFLICT (instance, project, xnat_id) DO UPDATE SET
    label = excluded.label,
    insert_user = excluded.insert_user,
    insert_date = excluded.insert_date,
    last_modified = excluded.last_modified
