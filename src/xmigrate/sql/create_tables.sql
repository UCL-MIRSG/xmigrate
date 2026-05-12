-- xmigrate DuckDB schema
-- All surrogate PKs use explicit sequences with DEFAULT nextval(...).
-- XNAT-assigned identifiers are stored in columns named xnat_id (or
-- source_xnat_id / destination_xnat_id in the map table) to distinguish
-- them unambiguously from the integer surrogate PKs used as FK references.

CREATE SEQUENCE IF NOT EXISTS instance_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS project_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS subject_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS experiment_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS map_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS migration_run_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS migration_run_item_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS xnat_user_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS user_permission_id_seq START 1;

CREATE TABLE IF NOT EXISTS instance (
    id INTEGER PRIMARY KEY DEFAULT nextval('instance_id_seq'),
    -- full base URL, e.g. https://xnat.example.com
    url VARCHAR NOT NULL UNIQUE,
    scheme VARCHAR NOT NULL,
    hostname VARCHAR NOT NULL,
    port INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS project (
    id INTEGER PRIMARY KEY DEFAULT nextval('project_id_seq'),
    instance INTEGER NOT NULL REFERENCES instance (id),
    xnat_id VARCHAR NOT NULL,      -- XNAT's own project ID / label
    secondary_id VARCHAR,
    description VARCHAR,
    UNIQUE (instance, xnat_id)
);

CREATE TABLE IF NOT EXISTS subject (
    id INTEGER PRIMARY KEY DEFAULT nextval('subject_id_seq'),
    instance INTEGER NOT NULL REFERENCES instance (id),
    project INTEGER NOT NULL REFERENCES project (id),
    owner_project INTEGER NOT NULL REFERENCES project (id),
    -- project != owner_project means this subject was shared into project by owner_project
    xnat_id VARCHAR NOT NULL,     -- XNAT's subject ID on the given instance
    label VARCHAR,
    insert_user VARCHAR,
    insert_date TIMESTAMP,
    last_modified TIMESTAMP,
    uri VARCHAR,
    UNIQUE (instance, project, xnat_id)
);

CREATE TABLE IF NOT EXISTS experiment (
    id INTEGER PRIMARY KEY DEFAULT nextval('experiment_id_seq'),
    instance INTEGER NOT NULL REFERENCES instance (id),
    project INTEGER NOT NULL REFERENCES project (id),
    -- nullable: may not be known at insert time
    subject INTEGER REFERENCES subject (id),
    xnat_id VARCHAR NOT NULL,     -- XNAT's experiment ID on the given instance
    label VARCHAR,
    xsi_type VARCHAR,              -- e.g. xnat:mrSessionData
    insert_user VARCHAR,
    insert_date TIMESTAMP,
    last_modified TIMESTAMP,
    uri VARCHAR,
    UNIQUE (instance, project, xnat_id)
);

CREATE TABLE IF NOT EXISTS map (
    id INTEGER PRIMARY KEY DEFAULT nextval('map_id_seq'),
    -- mirrors XnatType: project/subject/experiment/scan/assessor
    type VARCHAR NOT NULL,
    source_project INTEGER NOT NULL REFERENCES project (id),
    destination_project INTEGER NOT NULL REFERENCES project (id),
    source_xnat_id VARCHAR NOT NULL,
    destination_xnat_id VARCHAR NOT NULL,
    UNIQUE (type, source_project, destination_project, source_xnat_id)
);

CREATE TABLE IF NOT EXISTS migration_run (
    id INTEGER PRIMARY KEY DEFAULT nextval('migration_run_id_seq'),
    source_instance INTEGER NOT NULL REFERENCES instance (id),
    destination_instance INTEGER NOT NULL REFERENCES instance (id),
    started_at TIMESTAMP NOT NULL DEFAULT now(),
    completed_at TIMESTAMP          -- NULL until the run finishes
);

CREATE TABLE IF NOT EXISTS migration_run_item (
    id INTEGER PRIMARY KEY DEFAULT nextval('migration_run_item_id_seq'),
    run INTEGER NOT NULL REFERENCES migration_run (id),
    map INTEGER NOT NULL REFERENCES map (id),
    UNIQUE (run, map)
);

CREATE TABLE IF NOT EXISTS xnat_user (
    id INTEGER PRIMARY KEY DEFAULT nextval('xnat_user_id_seq'),
    instance INTEGER NOT NULL REFERENCES instance (id),
    login VARCHAR NOT NULL,
    firstname VARCHAR,
    lastname VARCHAR,
    email VARCHAR,
    UNIQUE (instance, login)
);

CREATE TABLE IF NOT EXISTS user_permission (
    id INTEGER PRIMARY KEY DEFAULT nextval('user_permission_id_seq'),
    instance INTEGER NOT NULL REFERENCES instance (id),
    project INTEGER NOT NULL REFERENCES project (id),
    user INTEGER NOT NULL REFERENCES xnat_user (id),
    -- run that created/updated this permission
    run INTEGER REFERENCES migration_run (id),
    displayname VARCHAR NOT NULL,   -- XNAT role, e.g. "Owners", "Members"
    group_id VARCHAR,            -- XNAT GROUP_ID, e.g. "myproject_owner"
    UNIQUE (project, user)
);
