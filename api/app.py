from fastapi import FastAPI, Query
from service.nba_fetch import get_daily_leaders, get_player_time_series
from datetime import date, timedelta
import math

app = FastAPI()

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

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/daily_leaders")
def daily_leaders(
    game_date: date = Query(default=date.today() - timedelta(days=1)),
    limit: int = Query(default=10, le=50),
    mode: str = Query(default="best", regex="^(best|worst)$")
):
    """
    Get top or bottom performers by z_score for a given date.
    - mode="best": top N players
    - mode="worst": bottom N players (with at least 20 minutes played)
    """
    leaders = get_daily_leaders(game_date, limit, mode)
    return sanitize_response({
        "date": str(game_date),
        "limit": limit,
        "mode": mode,
        "leaders": leaders
    })


@app.get("/player_timeseries")
def player_timeseries(
    player_id: int,
    start_date: date = None,
    end_date: date = None
):
    ts = get_player_time_series(player_id, start_date, end_date)
    return sanitize_response({
        "player_id": player_id,
        "start_date": start_date,
        "end_date": end_date,
        "timeseries": ts
    })
