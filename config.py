import os

CONN = os.environ.get("DATABASE_URL", "postgres://postgres:dev@localhost:5432/jobs")