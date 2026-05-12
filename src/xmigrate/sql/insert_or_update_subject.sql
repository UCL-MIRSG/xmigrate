-- Bulk-insert rows from the registered DataFrame ``subject_df``
-- (columns: xnat_id, label, insert_user, insert_date, last_modified).
INSERT INTO subject (
    instance,
    project,
    owner_project,
    xnat_id,
    label,
    insert_user,
    insert_date,
    last_modified
)
SELECT
    $instance,
    $project,
    $owner_project,
    xnat_id,
    NULLIF(label, '') AS lbl,
    NULLIF(insert_user, '') AS insert_user,
    NULLIF(insert_date, '') AS insert_date,
    NULLIF(last_modified, '') AS last_modified
FROM subject_df
ON CONFLICT (instance, project, xnat_id) DO UPDATE SET
    label = excluded.label,
    insert_user = excluded.insert_user,
    insert_date = excluded.insert_date,
    last_modified = excluded.last_modified
