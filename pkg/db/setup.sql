CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    command TEXT NOT NULL,
    scheduled_at TIMESTAMP NOT NULL,
    picked_at TIMESTAMP,
    started_at TIMESTAMP, -- when the worker executing the task
    completed_at TIMESTAMP, -- when the task is completed
    failed_at TIMESTAMP -- when the task is failed
);

CREATE INDEX idx_tasks_scheduled_at ON tasks (scheduled_at);