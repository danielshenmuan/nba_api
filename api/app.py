from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from datetime import date, datetime, timedelta
from typing import Optional
import math
import re

from service.player_lookup import search_players
from service.nba_fetch import (
    get_daily_leaders,
    get_player_roster_pct,
    get_player_time_series,
)
from service.player_baselines import get_player_baselines_v1
app = FastAPI(openapi_url="/openapi.json", docs_url="/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # tighten later
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

v1 = APIRouter(prefix="/v1", tags=["v1"])

# ----------------------------
# helpers
# ----------------------------
def sanitize_response(obj):
    if isinstance(obj, dict):
        return {k: sanitize_response(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_response(v) for v in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj

def _parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date '{s}'. Use YYYY-MM-DD.")


def _parse_optional_int(value: Optional[str], field: str) -> Optional[int]:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    m = re.search(r"\d+", stripped)
    if not m:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {field} '{value}'. Must contain an integer.",
        )
    return int(m.group(0))

# ----------------------------
# unversioned health
# ----------------------------
@app.get("/health", operation_id="healthCheck")
def health():
    return {"status": "ok", "service": "nba-gbq-api"}

# ----------------------------
# /v1 endpoints
# ----------------------------
@v1.get("/players_search", operation_id="playersSearch",
        description="Resolve player names/nicknames to player_id with confidence.")
def players_search_endpoint(
    q: str = Query(..., min_length=2),
    limit: int = Query(5, ge=1, le=10)
):
    return {"query": q, "matches": search_players(q, limit=limit)}

@v1.get("/daily_leaders", operation_id="getDailyLeaders")
def daily_leaders(
    game_date: date = Query(default=date.today() - timedelta(days=1)),
    limit: int = Query(default=10, ge=1, le=50),
    mode: str = Query(default="best", pattern=r"^(best|worst)$"),
    min_minutes: int = Query(default=20, ge=0),
):
    leaders = get_daily_leaders(game_date, limit, mode, min_minutes=min_minutes)

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

@v1.get(
    "/player_timeseries",
    operation_id="getPlayerTimeSeries",
    description="STRICT: one player_id, one start_date, one end_date. Resolve names via /v1/players_search first."
)
def player_timeseries(
    request: Request,
    player_id: str = Query(..., description="Single player ID (integer)"),
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    limit: Optional[int] = Query(None, ge=1, le=2000),
):
    if "player_name" in request.query_params:
        raise HTTPException(
            status_code=400,
            detail="Use /v1/players_search to resolve names → player_id, then call /v1/player_timeseries with player_id only."
        )

    m = re.search(r"\d+", player_id or "")
    if not m:
        raise HTTPException(status_code=400, detail=f"Invalid player_id '{player_id}'. Must be an integer.")
    pid = int(m.group(0))

    s_date = _parse_date(start_date)
    e_date = _parse_date(end_date)

    ts = get_player_time_series(pid, s_date, e_date)
    if isinstance(ts, list) and limit:
        ts = ts[:limit]

    payload = {"player_id": pid, "start_date": s_date, "end_date": e_date, "series": ts}
    return JSONResponse(
        content=jsonable_encoder(sanitize_response(payload)),
        headers={"Cache-Control": "public, max-age=600"},
    )

@v1.get(
    "/player_baselines",
    operation_id="playerBaselinesV1",
    description="Season + last-5 baselines with weighted FG/FT and usage proxy. Single player_id."
)
def player_baselines_v1_endpoint(
    player_id: str = Query(..., description="Single player ID (integer)"),
    season: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    window: int = Query(5, ge=3, le=10),
):
    m = re.search(r"\d+", player_id or "")
    if not m:
        raise HTTPException(status_code=400, detail=f"Invalid player_id '{player_id}'. Must be an integer.")
    pid = int(m.group(0))

    try:
        data = get_player_baselines_v1(pid, season, window)
        if data is None:
            raise HTTPException(status_code=404, detail="No data for player/season.")
    except ValueError as e:
        raise HTTPException(400, str(e))

    return JSONResponse(
        content=jsonable_encoder(sanitize_response(data)),
        headers={"Cache-Control": "public, max-age=3600"},
    )


@v1.get(
    "/player_roster_pct",
    operation_id="getPlayerRosterPct",
    description="Return current or historical Yahoo roster percentage for a player by ID or name.",
)
def player_roster_pct_endpoint(
    mode: str = Query(..., pattern=r"^(current|history)$"),
    player_id: Optional[str] = Query(None, description="Yahoo/NBA player ID"),
    player_name: Optional[str] = Query(None, description="Exact player name"),
):
    pid = _parse_optional_int(player_id, "player_id")
    name = player_name.strip() if player_name else None
    if name == "":
        name = None

    if pid is None and not name:
        raise HTTPException(status_code=400, detail="Provide player_id or player_name.")

    try:
        data = get_player_roster_pct(mode, player_id=pid, player_name=name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if not data:
        raise HTTPException(status_code=404, detail="No results.")

    payload = {
        "mode": mode,
        "player_id": pid,
        "player_name": name,
        "count": len(data),
        "data": data,
    }

    return JSONResponse(
        content=jsonable_encoder(sanitize_response(payload)),
        headers={"Cache-Control": "public, max-age=600"},
    )


# mount versioned router
app.include_router(v1)
