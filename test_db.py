import psycopg

CONN = "postgres://postgres:dev@localhost:5432/jobs"

with psycopg.connect(CONN) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        print("connected:", cur.fetchone())