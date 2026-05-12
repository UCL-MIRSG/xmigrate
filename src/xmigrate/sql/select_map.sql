SELECT
    source_xnat_id,
    destination_xnat_id
FROM map
WHERE
    type = $type
    AND source_project = $source_project
    AND destination_project = $destination_project
