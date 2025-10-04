from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import List, Optional, Union
from fastapi.responses import PlainTextResponse
from pathlib import Path
from service.nba_fetch import get_daily_leaders, get_player_time_series
from datetime import date, timedelta, datetime
from typing import List, Optional
import math
from service.player_lookup import search_players
from fastapi.encoders import jsonable_encoder
import re


app = FastAPI(openapi_url="/openapi.json", docs_url="/docs")

from fastapi.openapi.utils import get_openapi
from fastapi.responses import PlainTextResponse

##############

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

def _parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date '{s}'. Use YYYY-MM-DD.")

@app.get("/health", operation_id="healthCheck")
def health():
    return {"status": "ok", "service": "nba-gbq-api"}


@app.get("/players_search", operation_id="playersSearch", description="Resolve player names/nicknames to player_id with confidence.")
def players_search(q: str = Query(..., min_length=2), limit: int = Query(5, ge=1, le=10)):
    return {"query": q, "matches": search_players(q, limit=limit)}


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
        content=jsonable_encoder(sanitize_response(payload)),
        headers={"Cache-Control": "public, max-age=600"},
    )

@app.get(
    "/player_timeseries",
    operation_id="getPlayerTimeSeries",
    description="STRICT: one player_id, one start_date, one end_date. Resolve names via /players_search first."
)
def player_timeseries(
    request: Request,
    player_id: str = Query(..., alias="player_id", description="Single player ID (integer)"),
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    limit: Optional[int] = Query(None, ge=1, le=2000),
):
    # Names are not allowed here
    if "player_name" in request.query_params:
        raise HTTPException(
            status_code=400,
            detail="Use /players_search to resolve names → player_id, then call /player_timeseries with player_id only."
        )

    # Extract a single integer ID (tolerates stray characters)
    m = re.search(r"\d+", player_id or "")
    if not m:
        raise HTTPException(status_code=400, detail=f"Invalid player_id '{player_id}'. Must be an integer.")
    pid = int(m.group(0))

    s_date = _parse_date(start_date)
    e_date = _parse_date(end_date)

    ts = get_player_time_series(pid, s_date, e_date)  # your existing service fn (select includes turnovers, z_score)
    if isinstance(ts, list) and limit:
        ts = ts[:limit]

    payload = {
        "player_id": pid,
        "start_date": s_date,
        "end_date": e_date,
        "series": ts,
    }
    return JSONResponse(
        content=jsonable_encoder(sanitize_response(payload)),
        headers={"Cache-Control": "public, max-age=600"},
    )