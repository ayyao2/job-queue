import psycopg
import time
from config import CONN
import os, socket

def handler(job_type, payload):
    if job_type == "sleep_job":
        time.sleep(payload["seconds"])
        return "done sleeping"
    else:
        raise ValueError(f"unknown job type: {job_type}")

if __name__ == "__main__":
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    with psycopg.connect(CONN) as conn:
        while True:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE jobs SET status = 'running' WHERE id = ("
                        "  SELECT id FROM jobs WHERE status = 'queued'"
                        "  ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED"
                        ") RETURNING id, job_type, payload"
                    )
                    row = cur.fetchone()
                    if row is not None:
                        job_id, job_type, payload = row
                        cur.execute(
                            "INSERT INTO executions (job_id, worker_id) VALUES (%s, %s)",
                            (job_id, worker_id),
                        )
            if row is None:
                time.sleep(1)
                continue

            print("running job", job_id)
            try:
                status, message = "done", handler(job_type, payload)
            except Exception as e:
                status, message = "failed", f"job failed: {e}"

            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE jobs SET status = %s WHERE id = %s",
                        (status, job_id),
                    )
            print(message, job_id)
