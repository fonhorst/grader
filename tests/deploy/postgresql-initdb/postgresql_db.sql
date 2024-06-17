CREATE TABLE IF NOT EXISTS Tasks (
    id                  UUID CONSTRAINT firstkey PRIMARY KEY,
    name                varchar(50),
    task_type           varchar(16) NOT NULL,
    user_id             UUID NOT NULL,
    project_id          UUID NOT NULL,
    job_id              varchar(100) NOT NULL,
    priority            numeric NOT NULL DEFAULT 0.0,
    parameters          jsonb NOT NULL,
    status              varchar(16) NOT NULL,
    status_updated_at   timestamp,
    submit_time         timestamp NOT NULL,
    end_time            timestamp,
    progress            numeric NOT NULL DEFAULT 0.0,
    progress_message    varchar(200),
    metrics             jsonb,
    context             jsonb
);

CREATE INDEX idx_tasks_project__id ON Tasks (project_id);
CREATE INDEX idx_tasks_user__id ON Tasks (user_id);
CREATE INDEX idx_tasks_job__id ON Tasks (job_id);
CREATE INDEX idx_tasks_status ON Tasks (status);
CREATE INDEX idx_tasks_submit__time ON Tasks (submit_time);


CREATE TABLE IF NOT EXISTS TasksFailReasons (
    id                  SERIAL PRIMARY KEY,
    task_id             UUID references Tasks(id) ON DELETE CASCADE,
    error_message       TEXT NOT NULL,
    error_full          TEXT
);

CREATE INDEX idx_tasksfailreasons_task__id ON TasksFailReasons (task_id);

CREATE TABLE IF NOT EXISTS nodes
(
    uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_uuid UUID NOT NULL,
    project_uuid UUID NULL,
    hostname        VARCHAR(255) NOT NULL,
    name            VARCHAR(255) NOT NULL,
    node_type       VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS project_limits
(
    project_uuid UUID,
    max_nodes SMALLINT NOT NULL,
    PRIMARY KEY (project_uuid)
);

CREATE TABLE IF NOT EXISTS codegen_introspection (
    uuid UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    image_name TEXT NOT NULL,
    pipeline_type TEXT NOT NULL,
    ops_lib_yaml TEXT NOT NULL,
    ops_lib_path TEXT
);

CREATE INDEX idx_codegen_introspection__image_name ON codegen_introspection(image_name);
