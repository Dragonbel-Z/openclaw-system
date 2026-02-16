import os, json
from fastapi import FastAPI, Request, HTTPException
import redis
import psycopg2
import requests

app = FastAPI()

REDIS_URL = os.environ.get("REDIS_URL", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")

# Redis (允许为空：如果没配会在用到时报错)
r = redis.Redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None

def db_conn():
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL missing")
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

    if r:
        r.lpush("leads:queue", json.dumps({"name": name, "email": email}))

    return {"queued": True}


@app.get("/scrape/spas/richmond")
def scrape_richmond_spas(limit: int = 60, use_cache: bool = True):
    # 1) 必须有 key
    if not GOOGLE_MAPS_API_KEY:
        raise HTTPException(status_code=500, detail="GOOGLE_MAPS_API_KEY missing in Render Env")

    # 2) New Places Text Search endpoint（新版）
    url = "https://places.googleapis.com/v1/places:searchText"

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_MAPS_API_KEY,
        # FieldMask 必填：不填会报错/不给数据
        "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.rating,places.websiteUri,places.nationalPhoneNumber"
    }

    body = {
        "textQuery": "spa in Richmond BC",
        "pageSize": min(limit, 20)  # 先保守：一次最多 20
    }

    resp = requests.post(url, headers=headers, json=body, timeout=30)

    if resp.status_code != 200:
        # 把 Google 返回原样吐出来，方便你定位
        raise HTTPException(status_code=502, detail={"google_status": "ERROR", "raw": resp.text})

    return resp.json()