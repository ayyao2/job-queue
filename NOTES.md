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
  it either. B's SELECT subquery doesn't block, since reads don't wait on write locks in Postgres. Thus, B gets blocked after trying to UPDATE row X, and resumes once A commits and releases. 
- Fix: FOR UPDATE SKIP LOCKED inside the subquery. B's scan excludes the locked
  row entirely, so B's subquery returns a different id. B never blocks and never
  touches row 3.