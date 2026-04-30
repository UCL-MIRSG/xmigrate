-- Update destination subject metadata from a source_subjects staging table.
UPDATE destination.xnat_subjectdata_meta_data m
SET
    insert_date = source.insert_date,
    last_modified = source.last_modified,
    insert_user_xdat_user_id = source.insert_user_xdat_user_id  -- the integer ID, not username
FROM
    destination.xnat_subjectdata s
INNER JOIN
    source_subjects source ON s.label = source.label
WHERE
    s.subjectdata_info = m.meta_data_id
    AND s.project = source.project;  -- project ID
