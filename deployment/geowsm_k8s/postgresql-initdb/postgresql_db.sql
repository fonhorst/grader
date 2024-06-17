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


CREATE TABLE IF NOT EXISTS TasksFailReasons (
    id                  SERIAL PRIMARY KEY,
    task_id             UUID references Tasks(id) ON DELETE CASCADE,
    error_message       TEXT NOT NULL,
    error_full          TEXT
);

CREATE INDEX idx_tasksfailreasons_task__id ON TasksFailReasons (task_id);
