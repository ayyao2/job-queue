import psycopg
import time
from config import CONN

def handler(job_type, payload):
    if job_type == "sleep_job":
        time.sleep(payload["seconds"])
        return "done sleeping"
    else:
        raise ValueError(f"unknown job type: {job_type}")

if __name__ == "__main__":
    with psycopg.connect(CONN) as conn:
        while True:
            with conn.cursor() as cur:
                with conn.transaction():
                    cur.execute("UPDATE jobs SET status = 'running' WHERE id = (SELECT id FROM jobs WHERE status = 'queued' LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING id, job_type, payload")
                row = cur.fetchone()
                if row is None:
                    time.sleep(1)
                    continue
                job_id, job_type, payload = row
                print("running job", job_id)
                try:
                    result = handler(job_type, payload)
                    with conn.transaction():
                        cur.execute(
                            "UPDATE jobs SET status = 'done' WHERE id = %s",
                            (job_id,),
                        )
                    print(result, job_id)
                except Exception as e:
                    with conn.transaction():
                        cur.execute(
                            "UPDATE jobs SET status = 'failed' WHERE id = %s",
                            (job_id,),
                        )
                    print("job failed:", e)