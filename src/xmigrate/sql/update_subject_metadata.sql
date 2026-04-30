-- Update subject metadata
UPDATE destination.xnat_subjectdata_meta_data subject_metadata
SET
    insert_date = updated.insert_date,
    last_modified = updated.last_modified,
    insert_user_xdat_user_id = destination_user.xdat_user_id
FROM
    destination.xnat_subjectdata AS subject
INNER JOIN
    updated_metadata AS updated
    ON subject.id = updated.id
LEFT JOIN
    destination.xdat_user AS destination_user
    ON updated.insert_user = destination_user.login
WHERE
    subject.subjectdata_info = subject_metadata.meta_data_id
    AND subject.project = updated.project;
