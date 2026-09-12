import os

CONN = os.environ.get("DATABASE_URL", "postgres://postgres:dev@localhost:5432/jobs")
BACKOFF_BASE = float(os.environ.get("BACKOFF_BASE", "1.0"))
BACKOFF_CAP = float(os.environ.get("BACKOFF_CAP", "60.0"))
LOCK_TIMEOUT = float(os.environ.get("LOCK_TIMEOUT", "5.0"))