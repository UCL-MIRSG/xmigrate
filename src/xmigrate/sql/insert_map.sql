-- Insert a single source → destination XNAT ID mapping.
-- On conflict (same type/source_project/destination_project/source_xnat_id),
-- update destination_xnat_id in-place so that RETURNING always yields the row id.
INSERT INTO map (
    type,
    source_project,
    destination_project,
    source_xnat_id,
    destination_xnat_id
)
VALUES (
    $type,
    $source_project,
    $destination_project,
    $source_xnat_id,
    $destination_xnat_id
)
ON CONFLICT (
    type, source_project, destination_project, source_xnat_id
) DO UPDATE SET destination_xnat_id = excluded.destination_xnat_id
RETURNING id
