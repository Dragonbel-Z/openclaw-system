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
    import csv
from io import StringIO
from fastapi.responses import Response

@app.get("/export/spas/richmond")
async def export_richmond_spas(limit: int = 60, use_cache: bool = True):
    """
    导出 Richmond Spa 列表为 CSV
    依赖你现有的 /scrape/spas/richmond 返回格式：{"places":[{...}, ...]}
    """

    # ✅ 方式A（推荐）：直接调用你现有的 scrape 逻辑函数（如果你有的话）
    # 比如你内部可能有：scrape_richmond_spas(limit, use_cache)
    # data = await scrape_richmond_spas(limit=limit, use_cache=use_cache)

    # ✅ 方式B（最省事/不改你原逻辑）：直接复用同一个接口（自调用）
    # 注意：需要 requests 库；如果你 requirements 没有它，要加上 `requests`
    import requests
    base_url = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
    if not base_url:
        # Render 上通常可以用你服务的公开域名；你也可以在 Render env 手动设置 PUBLIC_BASE_URL
        raise Exception("PUBLIC_BASE_URL is missing. Set it to https://co-api-xxxx.onrender.com")

    url = f"{base_url}/scrape/spas/richmond?limit={limit}&use_cache={'true' if use_cache else 'false'}"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    places = data.get("places", [])

    # 生成 CSV
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["name", "rating", "phone", "address", "website"])

    for p in places:
        writer.writerow([
            p.get("displayName", ""),
            p.get("rating", ""),
            p.get("nationalPhoneNumber", ""),
            p.get("formattedAddress", ""),
            p.get("websiteUri", ""),
        ])

    csv_bytes = output.getvalue().encode("utf-8-sig")  # utf-8-sig 方便 Excel 直接打开不乱码

    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=richmond_spas.csv"}
    )