-- Update subject metadata
UPDATE destination.xnat_subjectdata_meta_data subject_metadata
SET
    insert_date = updated.insert_date,
    last_modified = updated.last_modified,
    insert_user_xdat_user_id = destination_user.xdat_user_id
FROM
    destination.xnat_subjectdata subject
INNER JOIN
    updated_metadata updated ON subject.id = updated.ID
LEFT JOIN
    destination.xdat_user destination_user ON destination_user.login = updated.insert_user
WHERE
    subject.subjectdata_info = subject_metadata.meta_data_id
    AND subject.project = updated.project;
