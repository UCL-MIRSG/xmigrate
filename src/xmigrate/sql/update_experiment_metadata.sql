-- Update experiment metadata
UPDATE destination.xnat_experimentdata_meta_data experiment_metadata
SET
    insert_date = updated.insert_date,
    last_modified = updated.last_modified,
    insert_user_xdat_user_id = destination_user.xdat_user_id
FROM
    destination.xnat_experimentdata AS experiment
INNER JOIN
    updated_metadata AS updated
    ON experiment.label = updated.label
LEFT JOIN
    destination.xdat_user AS destination_user
    ON updated.insert_user = destination_user.login
WHERE
    experiment.experimentdata_info = experiment_metadata.meta_data_id
    AND experiment.project = updated.project;
