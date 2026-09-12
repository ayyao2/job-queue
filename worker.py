import psycopg
import time
from config import CONN, BACKOFF_BASE, BACKOFF_CAP, LOCK_TIMEOUT
import os, socket
import random

class PermanentError(Exception):
    pass

def handle_sleep(payload):
    time.sleep(payload["seconds"])
    return "done sleeping"

def handle_flaky(payload):
    if random.random() < 0.3:
        raise RuntimeError("flaky job failed")
    return "flaky job succeeded"

HANDLERS = {
    "sleep_job": handle_sleep,
    "flaky_job": handle_flaky,
}

def handler(job_type, payload):
    if job_type in HANDLERS:
        return HANDLERS[job_type](payload)
    else:
        raise PermanentError(f"Unknown job type: {job_type}")

if __name__ == "__main__":
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    with psycopg.connect(CONN) as conn:
        while True:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE jobs SET status = 'running', attempts = attempts + 1, locked_until = now() + make_interval(secs => %s), locked_by = %s WHERE id = ("
                        "  SELECT id FROM jobs WHERE (status = 'queued' AND run_at <= now()) OR (status = 'running' AND locked_until <= now() AND attempts < max_attempts)"
                        "  ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED"
                        ") RETURNING id, job_type, payload, attempts, max_attempts",
                        (LOCK_TIMEOUT, worker_id),
                    )
                    row = cur.fetchone()
                    if row is not None:
                        job_id, job_type, payload, attempts, max_attempts = row
                        cur.execute(
                            "INSERT INTO executions (job_id, worker_id) VALUES (%s, %s)",
                            (job_id, worker_id),
                        )
                        

            if row is None:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE jobs SET status = 'dead', error = 'exceeded max attempts' WHERE status = 'running' AND locked_until <= now() AND attempts >= max_attempts"
                        )
                time.sleep(1)
                continue
        
            print("running job", job_id)
            permanent = False
            try:
                success, message = True, handler(job_type, payload)
            except PermanentError as e:
                success, message = False, str(e)
                permanent = True
            except Exception as e:
                success, message = False, str(e)

            with conn.transaction():
                with conn.cursor() as cur:
                    if success:
                        cur.execute(
                            "UPDATE jobs SET status = 'done' WHERE id = %s",
                            (job_id,),
                        )
                    elif attempts < max_attempts and not permanent:
                        backoff = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** attempts)) * random.uniform(0.5, 1.5)
                        cur.execute(
                            "UPDATE jobs SET status = 'queued', run_at = now()+make_interval(secs => %s), error = %s WHERE id = %s",
                            (backoff, message, job_id),
                        )
                    else:
                        cur.execute(
                            "UPDATE jobs SET status = 'dead', error = %s WHERE id = %s",
                            (message, job_id),
                        )
            print(message, job_id)
