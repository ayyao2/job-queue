DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS executions;

CREATE TABLE jobs (
    id         BIGSERIAL PRIMARY KEY,
    job_type   TEXT NOT NULL,
    payload    JSONB NOT NULL,
    status     TEXT NOT NULL DEFAULT 'queued',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE executions (
    job_id BIGINT NOT NULL,
    worker_id TEXT, 
    at TIMESTAMPTZ NOT NULL DEFAULT now()
);