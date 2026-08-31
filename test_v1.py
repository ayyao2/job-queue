import pytest
import psycopg
from config import CONN
from jobqueue import enqueue
import time
import sys

def start_workers(n):
    import subprocess
    procs = []
    for _ in range(n):
        p = subprocess.Popen([sys.executable, "worker.py"])
        procs.append(p)
    return procs

def enqueue_jobs(n):
    for _ in range(n):
        enqueue("sleep_job", {"seconds": 0.2})

def wait_for_jobs_to_finish(timeout):
    deadline = time.time() + timeout
    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            while time.time() < deadline:
                cur.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running')")
                count = cur.fetchone()[0]
                if count == 0:
                    return
                time.sleep(1)
    raise TimeoutError("Jobs did not finish in time")

@pytest.fixture
def reset_db():
    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE jobs, executions RESTART IDENTITY")
    yield

@pytest.fixture
def workers(reset_db):
    procs = start_workers(5)
    yield procs
    for p in procs:
        p.terminate()
        p.wait() 

def test_v1(workers):
    enqueue_jobs(200)
    wait_for_jobs_to_finish(100)
    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM jobs WHERE status = 'done'")
            done_count = cur.fetchone()[0]
            assert done_count == 200
            cur.execute("SELECT job_id, count(*) FROM executions GROUP BY job_id HAVING count(*) > 1")
            duplicates = cur.fetchall()
            assert len(duplicates) == 0, f"Duplicate executions found: {duplicates}"