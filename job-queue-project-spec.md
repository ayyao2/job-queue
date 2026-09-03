# Project Specification: A Job Queue and Worker Pool

**Build a system that accepts units of background work, hands them out to a pool of worker processes, and guarantees each one eventually completes — even when workers crash, hang, or run the same job twice.**

---

## 1. What this actually is

Most web requests should be fast. But some work is slow: sending email, generating a PDF, transcoding video, calling a flaky third-party API, resizing images. If you do that work inside the HTTP request, the user waits and your web server burns capacity on it.

The standard solution splits the work in two:

1. The **producer** (usually your web app) records a description of the work — a **job** — and immediately returns to the user.
2. A **worker**, running in a separate process, picks the job up later and actually does it.
3. The **queue** is the durable place jobs sit in between.

```
   HTTP request              ┌──────────────┐
        │                    │              │
        ▼                    │    QUEUE     │        ┌──────────┐
   ┌─────────┐   enqueue     │  (durable    │ claim  │ Worker 1 │
   │ Web app │──────────────▶│   storage)   │◀──────▶└──────────┘
   └─────────┘               │              │        ┌──────────┐
        │                    │  job records │◀──────▶│ Worker 2 │
        ▼ returns instantly  │              │        └──────────┘
     202 Accepted            └──────────────┘        ┌──────────┐
                                    ▲       ◀──────▶ │ Worker 3 │
                                    │                └──────────┘
                              ┌───────────┐
                              │ Dead      │  jobs that failed
                              │ letter    │  too many times
                              └───────────┘
```

You are building the middle box and the machinery around it. You are essentially writing a miniature version of Celery, Sidekiq, BullMQ, or AWS SQS.

**Why this project and not a prettier one:** almost every nontrivial production system contains a queue, and the failure modes are the same everywhere. Once you have personally caused a duplicate charge because a worker crashed after doing its work but before marking the job complete, you understand at-least-once delivery in a way no lecture provides.

---

## 2. Learning goals

By the end you should be able to explain, from experience rather than memorization:

- Why exactly-once delivery is effectively impossible, and what at-least-once means in practice
- What idempotency is and why it's the consumer's responsibility, not the queue's
- How a visibility timeout lets you recover work from a crashed worker without a heartbeat protocol
- Why retries need exponential backoff and jitter, and what a thundering herd is
- What a dead letter queue is for
- How multiple workers claim distinct rows without stepping on each other
- Why "the job ran but the result vanished" is a normal thing to design around

---

## 3. Scope

### In scope
- Durable job storage that survives a full system restart
- An enqueue API
- A worker process that claims, executes, and acknowledges jobs
- Retry with exponential backoff
- A dead letter queue
- Scheduled/delayed jobs (`run_at` in the future)
- At least one real job type that does something genuinely slow
- Basic metrics and a way to observe queue depth

### Explicitly out of scope (resist these)
- A pretty web UI. A JSON endpoint and a terminal are fine.
- Authentication and multi-tenancy
- Distributed consensus. One database is your source of truth.
- Horizontal scaling of the *storage* layer
- Writing your own network protocol

### Non-goals worth stating out loud
You are not trying to beat Sidekiq on throughput. You are trying to *reproduce its problems* so you understand why it's built the way it is.

---

## 4. Recommended stack

| Component | Choice | Why |
|---|---|---|
| Storage | PostgreSQL | `FOR UPDATE SKIP LOCKED` makes the claim query clean, and you get durability and transactions for free. Redis is the alternative but hides the interesting parts. |
| Language | Whatever you're fluent in | Go and Python are both excellent here. Concurrency is easier to reason about in Go; Python is faster to write. |
| Transport | Direct DB polling | Add `LISTEN/NOTIFY` later as an optimization, once you've felt the cost of polling. |
| Orchestration | Docker Compose | You need to start and kill workers constantly. Compose makes `docker compose up --scale worker=5` trivial. |
| Metrics | Prometheus + Grafana | Queue depth over time is the single most informative graph in the project. |
| Load generation | A script, or k6 | You need to be able to dump 50,000 jobs in and watch what happens. |

---

## 5. Data model

A single table carries most of the design.

```sql
CREATE TYPE job_status AS ENUM ('queued', 'running', 'succeeded', 'failed', 'dead');

CREATE TABLE jobs (
    id              BIGSERIAL PRIMARY KEY,
    queue           TEXT        NOT NULL DEFAULT 'default',
    job_type        TEXT        NOT NULL,
    payload         JSONB       NOT NULL,

    status          job_status  NOT NULL DEFAULT 'queued',
    priority        INT         NOT NULL DEFAULT 0,

    attempts        INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL DEFAULT 5,

    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),  -- earliest eligible time
    locked_until    TIMESTAMPTZ,                          -- visibility timeout expiry
    locked_by       TEXT,                                 -- worker id, for debugging

    last_error      TEXT,
    idempotency_key TEXT UNIQUE,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX idx_jobs_claimable
    ON jobs (queue, priority DESC, run_at)
    WHERE status IN ('queued', 'running');
```

**Notes on specific fields:**

- `run_at` does double duty: it schedules delayed jobs *and* implements backoff. To retry in 30 seconds, set the job back to `queued` with `run_at = now() + 30s`.
- `locked_until` is the visibility timeout. A job is `running`, but if `locked_until` has passed, it's considered abandoned and becomes claimable again. This is how you recover from a worker that was hard-killed and never got to say anything.
- `idempotency_key` lets a producer safely retry an enqueue without creating a duplicate job. The unique constraint does the work.
- `payload` is deliberately opaque JSON. The queue must not care what the job means.

---

## 6. Core mechanics

### 6.1 Claiming a job

This one query is the heart of the system. It must be atomic, and two workers running it simultaneously must never receive the same row.

```sql
UPDATE jobs
SET status       = 'running',
    locked_until = now() + interval '30 seconds',
    locked_by    = $1,
    attempts     = attempts + 1
WHERE id = (
    SELECT id FROM jobs
    WHERE queue = $2
      AND (
            (status = 'queued'  AND run_at <= now())
         OR (status = 'running' AND locked_until < now())   -- reclaim abandoned
      )
    ORDER BY priority DESC, run_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;
```

`SKIP LOCKED` is the important part: instead of blocking on a row another transaction holds, the query skips past it to the next candidate. Without it, ten workers serialize into one. **Try it without `SKIP LOCKED` first and measure the throughput collapse.** That comparison is worth an hour of your time.

Note that `attempts` increments at *claim* time, not failure time. If it incremented on failure, a worker that dies mid-job would never record the attempt and the job could retry forever.

### 6.2 Completing and failing

- **Success:** set `status = 'succeeded'`, `completed_at = now()`, clear the lock.
- **Failure with retries left:** set `status = 'queued'`, `run_at = now() + backoff(attempts)`, record `last_error`.
- **Failure with no retries left:** set `status = 'dead'`. It stays in the table but is no longer claimable. This is your dead letter queue.

### 6.3 Backoff

```
delay = min(base * 2^attempts, cap) * random_between(0.5, 1.5)
```

With `base = 1s` and `cap = 1h`, retries land at roughly 2s, 4s, 8s, 16s… The random multiplier is **jitter**, and it matters more than it looks. Without it, if a downstream API goes down and 5,000 jobs all fail at once, all 5,000 retry at the identical moment, hammer the recovering service, and fail again in lockstep. Jitter smears them out.

### 6.4 Visibility timeout vs. long jobs

A 30-second timeout breaks any job that legitimately takes two minutes — it gets reclaimed and run twice while the first copy is still working. Two ways to handle it, and you should implement one:

- **Per-job timeout:** each job type declares how long it may take.
- **Heartbeating:** the worker periodically extends `locked_until` while it's still alive.

Heartbeating is more robust and more interesting. It also introduces its own puzzle: what if the worker is alive enough to heartbeat but wedged and making no progress?

---

## 7. Worker loop

```
worker_id = hostname + "-" + pid + "-" + random

loop:
    job = claim_one(queue)
    if job is None:
        sleep(poll_interval with jitter)
        continue

    start heartbeat timer for job
    try:
        handler = registry[job.job_type]
        handler(job.payload)
        mark_succeeded(job)
    except RetryableError as e:
        mark_failed(job, e)          # goes back to queued with backoff
    except PermanentError as e:
        mark_dead(job, e)            # skip remaining attempts
    finally:
        stop heartbeat timer
```

Two details that are easy to skip and shouldn't be:

**Graceful shutdown.** On `SIGTERM`, stop claiming new jobs, finish the one in flight, then exit. Without this, every deploy kills work mid-flight and relies on the visibility timeout to clean up. Handle the signal, and give yourself a grace period after which you give up and exit anyway.

**Poll interval jitter.** If all workers poll every 500ms starting from the same deploy, they hit the database in synchronized waves. Randomize the sleep.

---

## 8. Milestones

Build in this order. Each stage should work before the next begins.

### v0 — Walking skeleton (a few hours)
Table, an `enqueue()` function, a worker that claims one job at a time and prints the payload. One handler: `sleep_job`, which sleeps for N seconds. Sounds trivial; it gives you the harness for everything else.

### v1 — Concurrency
Run five workers. Enqueue 1,000 jobs. Verify each executed exactly once with no lost or duplicated work under normal conditions. This is where `SKIP LOCKED` earns its keep. Write a test that asserts on the count.

### v2 — Failure and retry
Add a handler that fails randomly 30% of the time. Implement backoff, `max_attempts`, and the dead letter queue. Verify that everything either succeeds or lands in `dead`, and nothing is lost.

### v3 — Crash recovery
Add the visibility timeout and heartbeating. Then **kill workers with `SIGKILL` mid-job** and confirm the work gets picked up by someone else. This is the milestone that teaches the most.

### v4 — Idempotency
Introduce a job with a side effect you can observe and count — incrementing a row, writing a file, recording a "charge." Now demonstrate a duplicate execution by killing a worker after the side effect but before acknowledgment. Then fix it: the handler records a completion marker keyed by job id, in the same transaction as the side effect, and checks it on entry. Write up why the queue could not have solved this for you.

### v5 — Observability
Export metrics: queue depth by status, jobs completed per second, job duration histogram, retry count, dead letter count. Graph them. Then load test until queue depth grows without bound, and watch the graph show you the exact moment arrival rate exceeded service rate.

### v6 — A real job
Replace the toy handlers with something actually slow. Image thumbnailing is the classic and needs no external accounts. Web page screenshotting, PDF generation, or calling a deliberately slow local API all work too.

---

## 9. Failure scenarios you must deliberately test

Do these by hand, write down what happened, and fix what breaks. This list is the actual curriculum.

| # | Scenario | What you're checking |
|---|---|---|
| 1 | `SIGKILL` a worker mid-job | Job is reclaimed after `locked_until` and completes elsewhere |
| 2 | `SIGKILL` after side effect, before ack | Duplicate execution occurs — and your idempotency check absorbs it |
| 3 | Stop Postgres for 30s while workers run | Workers retry connections and recover rather than crashing permanently |
| 4 | Enqueue 100k jobs at once | Claim query stays fast; check `EXPLAIN ANALYZE` and confirm it uses your index |
| 5 | A handler that hangs forever | Heartbeat keeps it locked; do you have a hard timeout that kills it? |
| 6 | Downstream dependency fails for all jobs simultaneously | Backoff + jitter spread the retries instead of synchronizing them |
| 7 | Scale from 1 to 20 workers | Throughput scales roughly linearly, then flattens — find where and explain why |
| 8 | Restart the whole system | No jobs lost; `running` jobs with expired locks return to the pool |

Scenario 2 is the centerpiece. If you take nothing else from this project, take the understanding of why that duplicate is unavoidable and where the fix belongs.

---

## 10. Acceptance criteria

You're done with the core project when:

- [ ] `docker compose up --scale worker=5` starts the whole system
- [ ] 10,000 jobs enqueued with random failure and random worker kills result in: every job either `succeeded` or `dead`, zero stuck in `running`, and observable side effects counted exactly once
- [ ] The claim query uses an index and stays under ~5ms at 100k rows
- [ ] Killing every worker and restarting loses no work
- [ ] A Grafana dashboard shows queue depth, throughput, and latency
- [ ] `README.md` explains the delivery guarantee you provide, in your own words

---

## 11. Stretch goals

Pick one or two, not all of them:

- **`LISTEN/NOTIFY`** so workers wake instantly instead of polling. Measure the latency improvement and the load reduction.
- **Priority queues and fairness.** One tenant enqueues 100k jobs and starves everyone else. Fix it with weighted round-robin across queues.
- **Rate-limited job types.** "This job type may only run 10/sec globally." Now you need distributed rate limiting.
- **Job chains and fan-out.** Job A completing enqueues B and C; a job that waits for both.
- **Cron scheduling.** A separate scheduler process that enqueues on a recurring basis — and must not double-enqueue when you run two schedulers for redundancy.
- **Swap the backend.** Reimplement storage on Redis behind the same interface. The comparison teaches you what the database was doing for you.

---

## 12. Documents to write

The code is half the deliverable. Write these too — they are what makes it interview-usable.

**Before you start — a design doc (1–2 pages):**
Expected throughput, job size, latency requirements, the schema, and a list of failure modes you anticipate.

**After you finish — a postmortem (1–2 pages):**
What your original design got wrong, which failure mode surprised you, and what you'd do differently. Cite specific incidents from your testing.

The gap between those two documents is the thing you actually learned. It's also, concretely, the best answer you will ever have to "tell me about a technical challenge you faced."

---

## 13. Reading

Consult these *after* you hit the corresponding problem, not before:

- Postgres docs on `SELECT ... FOR UPDATE SKIP LOCKED`
- AWS SQS documentation on visibility timeout — the clearest plain-language explanation of the concept
- "Exponential Backoff and Jitter" (AWS Architecture Blog)
- *Designing Data-Intensive Applications*, Kleinmann — Chapter 11 on stream processing, and the delivery-guarantee discussion in Chapter 8
- Sidekiq's and River's source code, once you've built your own and can read theirs critically
