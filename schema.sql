DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS executions;
DROP TABLE IF EXISTS side_effects;
DROP TABLE IF EXISTS job_completions;

CREATE TABLE jobs (
    id         BIGSERIAL PRIMARY KEY,
    job_type   TEXT NOT NULL,
    payload    JSONB NOT NULL,
    status     TEXT NOT NULL DEFAULT 'queued',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts   INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until TIMESTAMPTZ,
    locked_by TEXT,
    error TEXT
);

CREATE TABLE executions (
    job_id BIGINT NOT NULL,
    worker_id TEXT, 
    at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE side_effects (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE job_completions (
    job_id BIGINT PRIMARY KEY,
    at TIMESTAMPTZ NOT NULL DEFAULT now()
);