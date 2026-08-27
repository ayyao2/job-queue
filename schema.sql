CREATE TABLE jobs (
    id         BIGSERIAL PRIMARY KEY,
    job_type   TEXT NOT NULL,
    payload    JSONB NOT NULL,
    status     TEXT NOT NULL DEFAULT 'queued',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);