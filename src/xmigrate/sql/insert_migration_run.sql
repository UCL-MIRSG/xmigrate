INSERT INTO migration_run (source_instance, destination_instance)
VALUES ($source_instance, $destination_instance)
RETURNING id
