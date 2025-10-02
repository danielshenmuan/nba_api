from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.responses import PlainTextResponse
from pathlib import Path
from service.nba_fetch import get_daily_leaders, get_player_time_series
from datetime import date, timedelta
from typing import List, Optional
import math

app = FastAPI(openapi_url="/openapi.json", docs_url="/docs")

from fastapi.openapi.utils import get_openapi
from fastapi.responses import PlainTextResponse
import yaml

@app.get("/openapi.yaml", include_in_schema=False)
def openapi_yaml_runtime():
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=getattr(app, "description", None),
        routes=app.routes,
    )
    # Explicit servers entry required by GPT Actions
    schema["servers"] = [{"url": "https://nba-gbq-api-896368614747.us-central1.run.app"}]
    return PlainTextResponse(
        yaml.safe_dump(schema, sort_keys=False),
        media_type="text/yaml",
    )

@app.get("/openapi.yaml", include_in_schema=False)
def openapi_yaml():
    return PlainTextResponse(
        Path(__file__).with_name("openapi.yaml").read_text(),
        media_type="text/yaml"
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # tighten later if desired
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

def sanitize_response(obj):
    """Recursively convert NaN/inf to None in dicts/lists."""
    if isinstance(obj, dict):
        return {k: sanitize_response(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_response(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    else:
        return obj

@app.get("/health", operation_id="healthCheck")
def health():
    return {"status": "ok", "service": "nba-gbq-api"}

@app.get("/daily_leaders", operation_id="getDailyLeaders")
def daily_leaders(
    game_date: date = Query(default=date.today() - timedelta(days=1)),
    limit: int = Query(default=10, ge=1, le=50),
    mode: str = Query(default="best", regex="^(best|worst)$"),
    min_minutes: int = Query(default=20, ge=0),  # NEW: user-adjustable cutoff
):
    """
    Get top or bottom performers by z_score for a given date.
    - mode="best": top N players
    - mode="worst": bottom N players
    - min_minutes: minimum minutes played filter (applies to both modes)
    """
    try:
        # Ensure your service function accepts min_minutes; see note below.
        leaders = get_daily_leaders(game_date, limit, mode, min_minutes=min_minutes)
    except TypeError:
        # Backward-compat: if service doesn’t yet support min_minutes, call old signature.
        leaders = get_daily_leaders(game_date, limit, mode)

    payload = {
        "date": str(game_date),
        "limit": limit,
        "mode": mode,
        "min_minutes": min_minutes,
        "leaders": leaders,
    }
    return JSONResponse(
        sanitize_response(payload),
        headers={"Cache-Control": "public, max-age=600"},
    )

@app.get("/player_timeseries", operation_id="getPlayerTimeSeries")
def player_timeseries(
    player_id: List[int] = Query(..., alias="player_id", description="One or more player IDs"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    limit: Optional[int] = Query(None, ge=1, le=2000),  # simple per-player cap
):
    """
    Time series stats & z-scores for one or more players between start_date and end_date.
    Supports multiple player_id values (?player_id=2544&player_id=201939).
    """
    try:
        players = []
        for pid in player_id:
            ts = get_player_time_series(pid, start_date, end_date)  # existing service fn
            if isinstance(ts, list) and limit:
                ts = ts[:limit]
            players.append({
                "player_id": pid,
                "series": ts,
            })

        payload = {
            "player_ids": player_id,
            "start_date": start_date,
            "end_date": end_date,
            "players": players,
        }
        return JSONResponse(
            sanitize_response(payload),
            headers={"Cache-Control": "public, max-age=600"},
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
