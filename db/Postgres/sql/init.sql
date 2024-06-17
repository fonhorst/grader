-- Drop the "horizon" table if it exists
-- DROP TABLE IF EXISTS horizon;

-- Create the "horizon" table with partitions and indexes
CREATE TABLE IF NOT EXISTS horizon
(
    horizon_id INT            NOT NULL,
    inline     INT            NOT NULL,
    xline      INT            NOT NULL,
    value      NUMERIC(16, 6) NULL,
    deleted    BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (horizon_id, inline, xline)
) PARTITION BY LIST (horizon_id);

CREATE INDEX IF NOT EXISTS inlineindex ON horizon (inline);
CREATE INDEX IF NOT EXISTS xlineindex ON horizon (xline);

-- Create the "info" table with a serial primary key
CREATE TABLE IF NOT EXISTS info
(
    horizon_id  SERIAL,
    horizon_uid uuid DEFAULT gen_random_uuid(),
    cube_uid    uuid,
    PRIMARY KEY (cube_uid, horizon_uid)
);

--create types
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'execution_type_enum') THEN
            CREATE TYPE execution_type_enum AS ENUM ('SYNC', 'ASYNC');
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'status_enum') THEN
            CREATE TYPE status_enum AS ENUM ('REGISTERED', 'RUNNING', 'FAILED', 'FINISHED', 'PAUSED', 'CANCELLED');
        END IF;
    END
$$;


CREATE TABLE IF NOT EXISTS task_info
(
    task_id                   SERIAL PRIMARY KEY,
    task_uid                  uuid                NOT NULL,
    task_name                 CHAR(100)           NOT NULL,
    user_id                   INT                 NOT NULL,
    status                    status_enum   DEFAULT 'REGISTERED',
    priority                  INT                 NULL,
    execution_progress        NUMERIC(6, 2) DEFAULT 0,
    node_count                INT                 NULL,
    created_at                TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    estimated_completion_time TIMESTAMP           NULL,
    execution_type            execution_type_enum NOT NULL
);

--Create types
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'domain_type_enum') THEN
            CREATE TYPE domain_type_enum AS ENUM ('TIME', 'DEPTH');
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'measurement_type_enum') THEN
            CREATE TYPE measurement_type_enum AS ENUM ('SEC', 'M');
        END IF;
    END
$$;


-- Create the "volume_info" table
CREATE TABLE IF NOT EXISTS volume_info(
    uuid uuid DEFAULT gen_random_uuid(),
    volume_name VARCHAR(100) NOT NULL UNIQUE,
    domain domain_type_enum NOT NULL,
    measurement measurement_type_enum NOT NULL,
    created_by serial NOT NULL,
    deleted boolean DEFAULT false,
    props jsonb,
    grid_object_uuid uuid NOT NULL,
    trace_value_count smallint NOT NULL,
    subtrace_count smallint NOT NULL,
    PRIMARY KEY (uuid)
);

-- Alter volume_info.table_name to not Unique
ALTER TABLE volume_info
DROP CONSTRAINT volume_info_volume_name_key;

ALTER TABLE volume_info
ADD COLUMN created_ts timestamptz DEFAULT current_timestamp NOT NULL;

ALTER TABLE volume_info
ADD COLUMN deleted_ts timestamptz DEFAULT NULL;

ALTER TABLE volume_info DROP COLUMN domain;
ALTER TABLE volume_info DROP COLUMN measurement;

CREATE TABLE IF NOT EXISTS "objects" (
    uuid UUID PRIMARY KEY,
    created_user_id int NOT NULL,
    project_id int NOT NULL,
    object_type varchar(255), -- types
    name varchar(2047),
    props json,
    source json,
    created_ts timestamptz DEFAULT current_timestamp NOT NULL,
    updated_ts timestamptz DEFAULT current_timestamp NOT NULL,
    is_deleted boolean DEFAULT false NOT NULL,
    deleted_ts timestamptz
);

CREATE TABLE IF NOT EXISTS volume
(
    uuid           UUID PRIMARY KEY REFERENCES "objects" (uuid)    NOT NULL,
-- uncomment this on the server
--     grid_uuid      UUID REFERENCES "objects" (uuid)    NOT NULL,
    grid_uuid      UUID NOT NULL,
    subtrace_count smallint NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes
(
    uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_uuid UUID NOT NULL,
    project_uuid UUID NULL,
    hostname        VARCHAR(255) NOT NULL,
    name            VARCHAR(255) NOT NULL,
    node_type       VARCHAR(255) NOT NULL,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    is_deleted      BOOLEAN      NOT NULL DEFAULT FALSE,
    created_ts      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_ts      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    deleted_ts      TIMESTAMP    DEFAULT NULL,
    created_by      INT          NOT NULL,
    updated_by      INT          NOT NULL,
    deleted_by      INT          DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS project_limits
(
    project_uuid UUID,
    max_nodes SMALLINT NOT NULL,
    PRIMARY KEY (project_uuid)
);