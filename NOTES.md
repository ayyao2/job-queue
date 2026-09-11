### Rebuild / Reset Database
docker compose exec -T db psql -U postgres -d jobs < schema.sql

### Enter Jobs Database
docker compose exec db psql -U postgres -d jobs

### Notes / Lessons
- Two workers ran the same job because B selected job X after A did, but before
  A's claim was committed. A's write is invisible to B until commit, so the row
  still looks 'queued'.
    - Marking it 'running' right after the SELECT narrows the window to
      microseconds but doesn't close it. There is always some window of time between the write and the commit.
- Combining into one statement (UPDATE ... WHERE id = (SELECT ...)) doesn't fix
  it either. B's SELECT subquery doesn't block, since reads don't wait on write locks in Postgres. Thus, B gets blocked after trying to UPDATE row X, and resumes once A commits and releases since the id still matches. 
- The fix is FOR UPDATE SKIP LOCKED, which tells Postgres to lock the row on select
  and exclude rows that are already locked from the scan. 
    - Alternative is to check that status = "queued" in the UPDATE query as well as the subquery. However, then I would need retry logic to distinguish between losing the race and the queue being empty. Would be better if we can't hold onto the lock for a long time.

- A transaction is a set of queries that all get committed together, atomically, and any updates are invisible to other transactions until the commit. 
    - Handlers should run outside of transactions. Suppose handlers run inside the transaction the row was acquired in. Then, in the event of a crash, the "attempts" get rolled back, so the job never reaches max_attempts. Also, the if transaction crashes and it gets rolled back, the row in the queue may look like nothing happened, but maybe the handler did send an email / do the job. Finally, long transactions block the autovacuum.

- Increment attempts at claim. If at failure, a job that kills workers will keep attempts the same, causing the job to cycle forever.

- Use exponential backoff. If a job has already failed 3 times, it's much more likely to be a longer term outage rather than something momentary. Thus, our expected time a problem lasts increases exponentially with the number of failed attempts. 
    - May cause a thundering herd, if an API goes down and a ton of jobs fail at one instant. Then, under exponential backoff, they will also retry at the same time, overwhelming the downstream service.
    - Jitter. Multiply each delay by some random scalar, to spread out the exponential backoff over some window.

- Create a "dead" status for jobs that exhaust retries. Keep it in the same table as other jobs, for problem diagnosis and the ability to easily requeue it when the problem gets fixed. 

- Define permanent and retryable errors, so permanent errors like a handler not existing don't cause retries. That problem won't get fixed by retrying.
    - Make retry the default. 