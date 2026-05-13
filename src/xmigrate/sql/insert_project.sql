INSERT INTO project (instance, xnat_id, secondary_id, description)
VALUES ($instance, $xnat_id, $secondary_id, $description)
ON CONFLICT (instance, xnat_id) DO NOTHING
