import pytest
import psycopg
from config import CONN, LOCK_TIMEOUT, HEARTBEAT_INTERVAL
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

def wait_for_job(job_id, statuses, timeout):
    deadline = time.time() + timeout
    status = None
    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            while time.time() < deadline:
                cur.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
                status = cur.fetchone()[0]
                if status in statuses:
                    return status
                time.sleep(0.2)
    raise TimeoutError(f"Job {job_id} never reached {statuses}, last status {status}")

def seed_running_job(attempts, max_attempts, locked_until_offset):
    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO jobs (job_type, payload, status, attempts, max_attempts, locked_until, locked_by) "
                "VALUES ('sleep_job', '{\"seconds\": 0}', 'running', %s, %s, now() + make_interval(secs => %s), 'dead-worker') "
                "RETURNING id",
                (attempts, max_attempts, locked_until_offset),
            )
            return cur.fetchone()[0]

@pytest.fixture
def manual_workers(reset_db):
    procs = []
    yield procs
    for p in procs:
        p.terminate()
        p.wait()

def test_killed_worker_job_reclaimed(manual_workers):
    # A job whose worker is SIGKILLed mid-run stays 'running' until its lock
    # expires, then another worker must reclaim it and finish it.
    job_id = enqueue("sleep_job", {"seconds": LOCK_TIMEOUT * 0.6})
    # Register the victim with the fixture before anything that can raise, or a
    # timeout here leaks a live worker into every test that follows.
    victim = start_workers(1)[0]
    manual_workers.append(victim)
    wait_for_job(job_id, ("running",), 10)
    victim.kill()
    victim.wait()

    manual_workers.extend(start_workers(1))
    wait_for_job(job_id, ("done",), 4 * LOCK_TIMEOUT + 20)

    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT attempts FROM jobs WHERE id = %s", (job_id,))
            attempts = cur.fetchone()[0]
            assert attempts == 2, f"Expected the reclaim to count as a 2nd attempt, got {attempts}"
            cur.execute("SELECT COUNT(*) FROM executions WHERE job_id = %s", (job_id,))
            executions = cur.fetchone()[0]
            assert executions == 2, f"Expected 2 executions, got {executions}"

def test_expired_lock_with_exhausted_attempts_goes_dead(manual_workers):
    # An orphaned 'running' job that has already used up its attempts must not
    # be reclaimed forever -- it gets swept into 'dead'.
    job_id = seed_running_job(attempts=5, max_attempts=5, locked_until_offset=-1)
    manual_workers.extend(start_workers(1))
    wait_for_job(job_id, ("dead",), 20)

    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT attempts, error FROM jobs WHERE id = %s", (job_id,))
            attempts, error = cur.fetchone()
            assert attempts == 5, f"Sweep should not consume an attempt, got {attempts}"
            assert "exceeded max attempts" in error, f"Unexpected error message: {error}"
            cur.execute("SELECT COUNT(*) FROM executions WHERE job_id = %s", (job_id,))
            assert cur.fetchone()[0] == 0, "Swept job should never have been executed"

def test_live_lock_is_not_stolen(manual_workers):
    # A job held by a healthy worker (lock still in the future) must be left
    # alone by both the reclaim query and the dead sweep.
    job_id = seed_running_job(attempts=1, max_attempts=5, locked_until_offset=60)
    manual_workers.extend(start_workers(5))
    time.sleep(2 * LOCK_TIMEOUT)

    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, attempts, locked_by FROM jobs WHERE id = %s", (job_id,))
            status, attempts, locked_by = cur.fetchone()
            assert status == "running", f"Expected job to stay 'running', got {status}"
            assert attempts == 1, f"Expected attempts to stay 1, got {attempts}"
            assert locked_by == "dead-worker", f"Job was stolen by {locked_by}"

def job_row(job_id):
    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, attempts, locked_by, locked_until FROM jobs WHERE id = %s",
                (job_id,),
            )
            return cur.fetchone()

def execution_count(job_id):
    with psycopg.connect(CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM executions WHERE job_id = %s", (job_id,))
            return cur.fetchone()[0]

def wait_until(predicate, timeout, message):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise TimeoutError(message)

def test_long_job_not_reclaimed(manual_workers):
    job_id = enqueue("sleep_job", {"seconds": 4 * LOCK_TIMEOUT})
    manual_workers.extend(start_workers(3))
    wait_for_job(job_id, ("done",), 8 * LOCK_TIMEOUT + 20)

    attempts = job_row(job_id)[1]
    assert attempts == 1, f"Expected a single claim, got {attempts} attempts"
    executions = execution_count(job_id)
    assert executions == 1, f"Job ran more than once: {executions} executions"

def test_heartbeat_stops_when_worker_dies(manual_workers):
    job_id = enqueue("sleep_job", {"seconds": 10 * LOCK_TIMEOUT})
    victim = start_workers(1)[0]
    manual_workers.append(victim)
    wait_for_job(job_id, ("running",), 10)

    claimed_until = job_row(job_id)[3]
    time.sleep(4 * HEARTBEAT_INTERVAL)
    _, _, first_worker, renewed_until = job_row(job_id)
    assert renewed_until > claimed_until, "Heartbeat never extended locked_until"

    victim.kill()
    victim.wait()

    manual_workers.extend(start_workers(1))
    wait_until(
        lambda: job_row(job_id)[1] == 2,
        4 * LOCK_TIMEOUT + 20,
        "Job was never reclaimed -- the lock outlived the worker that held it",
    )

    status, _, locked_by, _ = job_row(job_id)
    assert status == "running", f"Expected the reclaimed job to be running, got {status}"
    assert locked_by != first_worker, "Job was reclaimed by the worker that died"
    assert execution_count(job_id) == 2, "Expected the reclaim to log a 2nd execution"
