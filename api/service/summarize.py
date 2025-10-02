import logging
from fastapi import HTTPException
import pandas as pd
from .nba_fetch import get_day_player_boxscores, compute_baseline_from_range
from .zscore import attach_zscores, NINE_CAT_ORDER
from .nba_fetch import get_from_cache, set_cache

def daily_top_players(iso_date: str, top_n: int = 10, baseline: dict | None = None) -> dict:
    cache_key = f"daily_summary:{iso_date}:{top_n}:{'with_base' if baseline else 'no_base'}"
    cached = get_from_cache(cache_key)
    if cached is not None:
        return cached

    try:
        raw = get_day_player_boxscores(iso_date)
    except Exception as e:
        logging.error(f"Error fetching boxscores for {iso_date}: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch NBA data for {iso_date} (timeout or connection issue)."
        )

    if raw.empty:
        return {"date": iso_date, "top": [], "games": 0, "players": 0}

    if baseline:
        scored = attach_zscores(raw, means=baseline["mean"], stdevs=baseline["stdev"])
    else:
        scored = attach_zscores(raw)

    cols = ["PLAYER_NAME", "TEAM_ABBREVIATION"] + NINE_CAT_ORDER + ["Z_Score"]
    cols = [c for c in cols if c in scored.columns]

    ranked = scored[cols].sort_values("Z_Score", ascending=False).head(top_n)
    result = {
        "date": iso_date,
        "games": raw["GAME_ID"].nunique() if "GAME_ID" in raw.columns else None,
        "players": len(raw),
        "top": ranked.to_dict(orient="records")
    }

    set_cache(cache_key, result)
    return result

def seasonal_baseline_summary(start: str, end: str) -> dict:
    cache_key = f"seasonal_baseline:{start}:{end}"
    cached = get_from_cache(cache_key)
    if cached is not None:
        return cached

    baseline = compute_baseline_from_range(start, end)
    set_cache(cache_key, baseline)
    return baseline
