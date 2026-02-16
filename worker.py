import os, json
import redis

REDIS_URL = os.environ.get("REDIS_URL", "")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

print("worker started...")

while True:
    item = r.brpop("leads:queue", timeout=10)
    if not item:
        continue
    _, raw = item
    lead = json.loads(raw)
    print("processed lead:", lead)
