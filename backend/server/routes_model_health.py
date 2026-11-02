"""Model Health tab (display-only, added 2026-11-01, PickMint V3).

Serves the payload engine.model_health builds in a BACKGROUND thread (started module-level in
backend/server/__init__.py, every 10 min) and caches to data/model_health.json. Nothing is computed on
request: GET returns the cached JSON bytes as-is (plus a small cache-age block), so it answers in
milliseconds; POST /refresh only nudges the background thread to rebuild early (at most once a minute).

READ-ONLY: the builder opens every pick DB with mode=ro and only calls the gates' read functions.
"""
from __future__ import annotations

import json
import time

from fastapi import APIRouter
from fastapi.responses import Response

router = APIRouter()


@router.get("/api/model-health")
def api_model_health():
    from engine import model_health as mh
    raw, built_at, building, err = mh.cached_bytes()
    if raw is None:
        body = {"building": True, "rows": [], "integrity": [], "sports": [], "error": err}
        return Response(json.dumps(body), media_type="application/json", headers={"Cache-Control": "no-store"})
    meta = json.dumps({"age_s": int(time.time() - built_at), "building": bool(building), "last_error": err})
    body = raw[:-1] + b',"cache":' + meta.encode("utf-8") + b"}"     # raw is one JSON object -> splice, no re-serialize
    return Response(body, media_type="application/json", headers={"Cache-Control": "no-store"})


@router.post("/api/model-health/refresh")
def api_model_health_refresh():
    from engine import model_health as mh
    mh.kick()
    return {"ok": True, "queued": True}
