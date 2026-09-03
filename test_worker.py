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

def enqueue_jobs(n, job_type, payload):
    for _ in range(n):
        enqueue(job_type, payload)

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

def test_concurrency(workers):
    enqueue_jobs(200, "sleep_job", {"seconds": 0.2})
    wait_for_jobs_to_finish(100)
    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM jobs WHERE status = 'done'")
            done_count = cur.fetchone()[0]
            assert done_count == 200, f"Expected 200 done jobs, got {done_count}"
            cur.execute("SELECT job_id, COUNT(*) FROM executions GROUP BY job_id HAVING COUNT(*) > 1")
            duplicates = cur.fetchall()
            assert len(duplicates) == 0, f"Duplicate executions found: {duplicates}"

def test_failure_retry(workers):
    enqueue_jobs(100, "flaky_job", {})
    wait_for_jobs_to_finish(100)
    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) from executions")
            total_executions = cur.fetchone()[0]
            assert total_executions > 100, f"Expected more than 100 executions, got {total_executions}"
            cur.execute("SELECT COUNT(*) FROM jobs WHERE status = 'done'")
            done_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM jobs WHERE status = 'dead'")
            dead_count = cur.fetchone()[0]
            assert done_count + dead_count == 100, f"Expected 100 total done or dead jobs, got {done_count + dead_count}"
            cur.execute("SELECT COUNT(*) FROM jobs WHERE status = 'dead' and attempts != max_attempts")
            unexhuasted_dead = cur.fetchone()[0]
            assert unexhuasted_dead == 0, f"{unexhuasted_dead} jobs dead without max attempts reached"

def test_unknown_job_type(workers):
    enqueue_jobs(1, "unknown_job", {})
    wait_for_jobs_to_finish(10)
    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, attempts, error FROM jobs WHERE job_type = 'unknown_job'")
            status, attempts, error = cur.fetchone()
            assert status == "dead", f"Expected status 'dead', got {status}"
            assert attempts == 1, f"Expected 1 attempt, got {attempts}"
            assert "Unknown job type" in error, f"Expected error message about unknown job type, got {error}"