import os, json
from fastapi import FastAPI, Request
import redis
import psycopg2

app = FastAPI()

REDIS_URL = os.environ.get("REDIS_URL", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def db_conn():
    return psycopg2.connect(DATABASE_URL)

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/webhook/lead")
async def webhook_lead(req: Request):
    payload = await req.json()
    email = payload.get("email", "unknown@example.com")
    name = payload.get("name", "unknown")

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id SERIAL PRIMARY KEY,
            name TEXT,
            email TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("INSERT INTO leads (name, email) VALUES (%s, %s)", (name, email))
    conn.commit()
    cur.close()
    conn.close()

    r.lpush("leads:queue", json.dumps({"name": name, "email": email}))

    return {"queued": True}
