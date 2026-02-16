import os, json, time
from typing import List, Dict, Any, Optional

import requests
from fastapi import FastAPI, Request, HTTPException
import redis
import psycopg2

app = FastAPI()

REDIS_URL = os.environ.get("REDIS_URL", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")

# Redis（允许为空：没配就不缓存）
r = None
if REDIS_URL:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    return psycopg2.connect(DATABASE_URL)

def ensure_spa_table():
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS spa_places (
            id SERIAL PRIMARY KEY,
            place_id TEXT UNIQUE,
            name TEXT,
            formatted_address TEXT,
            rating REAL,
            user_ratings_total INT,
            business_status TEXT,
            types TEXT,
            fetched_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

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

def google_places_text_search(query: str, pagetoken: Optional[str] = None) -> Dict[str, Any]:
    if not GOOGLE_MAPS_API_KEY:
        raise HTTPException(status_code=500, detail="GOOGLE_MAPS_API_KEY is missing in Render Environment")

    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"key": GOOGLE_MAPS_API_KEY}
    if pagetoken:
        params["pagetoken"] = pagetoken
    else:
        params["query"] = query

    resp = requests.get(url, params=params, timeout=30)
    data = resp.json()

    # 常见：REQUEST_DENIED / OVER_QUERY_LIMIT / INVALID_REQUEST
    if data.get("status") not in ("OK", "ZERO_RESULTS"):
        raise HTTPException(status_code=502, detail={"google_status": data.get("status"), "error_message": data.get("error_message")})

    return data

def upsert_places(rows: List[Dict[str, Any]]):
    ensure_spa_table()
    conn = db_conn()
    cur = conn.cursor()
    for it in rows:
        place_id = it.get("place_id")
        name = it.get("name")
        formatted_address = it.get("formatted_address")
        rating = it.get("rating")
        user_ratings_total = it.get("user_ratings_total")
        business_status = it.get("business_status")
        types = ",".join(it.get("types", [])) if it.get("types") else None

        if not place_id:
            continue

        cur.execute("""
            INSERT INTO spa_places (place_id, name, formatted_address, rating, user_ratings_total, business_status, types)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (place_id) DO UPDATE SET
              name = EXCLUDED.name,
              formatted_address = EXCLUDED.formatted_address,
              rating = EXCLUDED.rating,
              user_ratings_total = EXCLUDED.user_ratings_total,
              business_status = EXCLUDED.business_status,
              types = EXCLUDED.types,
              fetched_at = NOW()
        """, (place_id, name, formatted_address, rating, user_ratings_total, business_status, types))

    conn.commit()
    cur.close()
    conn.close()

@app.get("/scrape/spas/richmond")
def scrape_richmond_spas(limit: int = 60, use_cache: bool = True):
    """
    抓取 Richmond BC 的 spa 列表（使用 Places Text Search，非爬网页）
    """
    cache_key = f"spa:richmond:limit={limit}"
    if use_cache and r:
        cached = r.get(cache_key)
        if cached:
            return json.loads(cached)

    query = "spa in Richmond BC Canada"
    collected: List[Dict[str, Any]] = []

    # 第 1 页
    data = google_places_text_search(query=query)
    collected.extend(data.get("results", []))

    # 后续分页（Google 要求 next_page_token 生成需要等一下）
    next_token = data.get("next_page_token")
    while next_token and len(collected) < limit:
        time.sleep(2.2)  # 关键：不给等会返回 INVALID_REQUEST
        data2 = google_places_text_search(query=query, pagetoken=next_token)
        collected.extend(data2.get("results", []))
        next_token = data2.get("next_page_token")

    # 截断到 limit
    collected = collected[:limit]

    # 入库
    upsert_places(collected)

    # 返回精简字段（便于你后续做列表/导出/售卖）
    out = []
    for it in collected:
        out.append({
            "place_id": it.get("place_id"),
            "name": it.get("name"),
            "address": it.get("formatted_address"),
            "rating": it.get("rating"),
            "user_ratings_total": it.get("user_ratings_total"),
            "business_status": it.get("business_status"),
            "types": it.get("types", []),
        })

    result = {"count": len(out), "items": out}

    if use_cache and r:
        r.setex(cache_key, 3600, json.dumps(result))  # 1小时缓存

    return result
