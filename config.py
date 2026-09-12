import os

CONN = os.environ.get("DATABASE_URL", "postgres://postgres:dev@localhost:5432/jobs")
BACKOFF_BASE = float(os.environ.get("BACKOFF_BASE", "1.0"))
BACKOFF_CAP = float(os.environ.get("BACKOFF_CAP", "60.0"))
LOCK_TIMEOUT = float(os.environ.get("LOCK_TIMEOUT", "3.0"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "1.0"))

assert LOCK_TIMEOUT >= 3 * HEARTBEAT_INTERVAL, (
    f"LOCK_TIMEOUT ({LOCK_TIMEOUT}) must be at least 3x "
    f"HEARTBEAT_INTERVAL ({HEARTBEAT_INTERVAL})"
)