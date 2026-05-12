CREATE OR REPLACE TABLE updated_metadata AS
SELECT
    s.xnat_id AS id,
    s.label,
    s.insert_user,
    s.insert_date,
    s.last_modified,
    p.xnat_id AS project
FROM subject AS s
INNER JOIN project AS p ON s.project = p.id
WHERE s.project = $destination_project_id
