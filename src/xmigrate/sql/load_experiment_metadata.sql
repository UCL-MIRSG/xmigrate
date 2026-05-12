CREATE OR REPLACE TABLE updated_metadata AS
SELECT
    e.xnat_id AS id,
    e.label,
    e.insert_user,
    e.insert_date,
    e.last_modified,
    p.xnat_id AS project
FROM experiment AS e
INNER JOIN project AS p ON e.project = p.id
WHERE e.project = $destination_project_id
