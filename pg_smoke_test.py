import os
from dotenv import load_dotenv
import psycopg

load_dotenv()

db = os.getenv("DATABASE_URL")
print("DATABASE_URL:", db)

with psycopg.connect(db) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        print(cur.fetchone()[0])

print("OK: konek ke Postgres sukses")
