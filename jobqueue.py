import psycopg
from psycopg.types.json import Jsonb
from config import CONN

def enqueue(job_type, payload):
    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO jobs (job_type, payload) VALUES (%s, %s) RETURNING id",
                (job_type, Jsonb(payload)),
            )
            return cur.fetchone()[0]

if __name__ == "__main__":
    enqueue("sleep_job", {"seconds": 6})