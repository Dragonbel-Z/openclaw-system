import os
import requests
from fastapi import FastAPI

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/spas")
def spas():
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        return {"error": "Missing GOOGLE_MAPS_API_KEY"}

    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": "spa in Richmond, BC", "key": api_key}
    r = requests.get(url, params=params, timeout=20)
    data = r.json()

    results = []
    for p in data.get("results", []):
        results.append({
            "name": p.get("name"),
            "address": p.get("formatted_address"),
            "rating": p.get("rating"),
            "place_id": p.get("place_id"),
        })
    return {"count": len(results), "results": results}
