"""Waiver wire: who is worth picking up right now, and who is NEW to that list.

Pure functions over DataFrames (no BigQuery in here), so every rule below is unit-tested.

Rules agreed with Daniel on 2026-10-13, from the Yahoo backtest (Nov 2025 - Feb 2026, players owned 5-50%):
  * Rank by recent LEVEL (blend of the last 5 games and the 15 before them), not by ownership change. Level predicted
    next-week Fantasy Value far better than ownership rise or improvement; an ownership rise added nothing on top of it.
  * Ownership change is still the direct measure of the TREND (what managers are doing), so it is shown on every line
    and it can also qualify a player on its own (rise of 5+ points with real minutes and a top-quartile level).
  * "New" = in today's top group but not in the top group three days ago, so the daily post does not repeat itself.
  * No waiver posts in the first week of a season, and a player needs 4+ games.
Nothing here is a forecast.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

import numpy as np
import pandas as pd

OUT_STATUSES = {"OUT", "INJURY_RESERVE", "SUSPENSION", "OUT_FOR_SEASON"}


@dataclass(frozen=True)
class Rules:
    min_games: int = 4              # games this season before a level means anything
    own_min: float = 5.0            # roster % band: below this is deep bench noise
    own_max: float = 50.0           # at or above this he is not a waiver pickup
    min_minutes: float = 24.0       # average over the last 5 games (the gate that carried the backtest signal)
    top_k: int = 15                 # the "top group" (simulated: 15 gives ~3 posts a week; 10 is too tight)
    lag_days: int = 3               # compare with the top group this many days ago
    trend_min_rise: float = 5.0     # ownership points gained over the lag, to qualify on trend alone
    trend_level_quantile: float = 0.75   # ...and a level in the top quartile of the pool
    max_stale_days: int = 10        # ignore players who have not played in this long
    min_days_into_season: int = 7   # no waiver posts in the first week
    min_risers: int = 3             # fewer than this and there is no post
    max_in_post: int = 5
    blend_from_games: int = 10      # below this many games, level is the season-to-date mean


DEFAULT_RULES = Rules()


def player_levels(games: pd.DataFrame, as_of: date, rules: Rules = DEFAULT_RULES) -> pd.DataFrame:
    """One row per player from games played strictly before `as_of`.

    level = mean of (last 5 games' Fantasy Value, the up-to-15 games before them) once a player has 10+ games,
    otherwise the plain mean of his games so far. Columns: n_games, z5, zprior, level, min5, last_game, team,
    position, name, plus last-5 averages of pts/reb/ast/stl/blk/fg3m.
    """
    g = games[games["game_date"] < as_of].copy()
    if g.empty:
        return pd.DataFrame(columns=["n_games", "z5", "zprior", "level", "min5", "last_game"]).rename_axis("player_id")
    g = g.sort_values(["player_id", "game_date"])
    g["rn"] = g.groupby("player_id").cumcount(ascending=False) + 1          # 1 = most recent
    by = g.groupby("player_id")
    last5 = g[g["rn"] <= 5].groupby("player_id")
    prior = g[(g["rn"] > 5) & (g["rn"] <= 20)].groupby("player_id")
    newest = g[g["rn"] == 1].set_index("player_id")
    out = pd.DataFrame({
        "n_games": by.size(),
        "z_all": by["z_score"].mean(),
        "z5": last5["z_score"].mean(),
        "min5": last5["minutes"].mean(),
        "zprior": prior["z_score"].mean().where(prior.size() >= 5),
        "last_game": by["game_date"].max(),
        "name": newest["player_name"], "team": newest["team_abbr"], "position": newest["position"],
    })
    for c in ("pts", "reb", "ast", "stl", "blk", "fg3m"):
        out[c] = last5[c].mean()
    use_blend = (out["n_games"] >= rules.blend_from_games) & out["zprior"].notna()
    out["level"] = np.where(use_blend, (out["z5"] + out["zprior"]) / 2, out["z_all"])
    return out.drop(columns="z_all")


def candidate_pool(levels: pd.DataFrame, own: pd.DataFrame, as_of: date, rules: Rules = DEFAULT_RULES) -> pd.DataFrame:
    """Players who qualify as waiver pickups on `as_of`. `own` needs: player_id, roster_pct, optional injury_status."""
    o = own.drop_duplicates("player_id").set_index("player_id")
    df = levels.join(o[["roster_pct"] + (["injury_status"] if "injury_status" in o else [])], how="inner")
    if "injury_status" not in df:
        df["injury_status"] = None
    stale = df["last_game"].map(lambda d: (as_of - d).days)
    keep = (
        (df["roster_pct"] >= rules.own_min) & (df["roster_pct"] < rules.own_max)
        & (df["n_games"] >= rules.min_games)
        & (df["min5"] >= rules.min_minutes)
        & (stale <= rules.max_stale_days)
        & ~df["injury_status"].fillna("").str.upper().isin(OUT_STATUSES)
    )
    return df[keep]


def top_group(pool: pd.DataFrame, k: int) -> pd.DataFrame:
    return pool.sort_values(["level"], ascending=False, kind="mergesort").head(k)


@dataclass
class WireResult:
    as_of: date
    post_worthy: bool
    skip_reason: str | None
    risers: list[dict] = field(default_factory=list)
    holdovers: list[dict] = field(default_factory=list)
    freshness_basis: str = "ownership_then"

    def to_dict(self) -> dict:
        return {"as_of": str(self.as_of), "post_worthy": self.post_worthy, "skip_reason": self.skip_reason,
                "freshness_basis": self.freshness_basis, "risers": self.risers, "holdovers": self.holdovers}


def _row(pid, r: pd.Series, own3, reasons: list[str]) -> dict:
    f = lambda v, d=1: None if v is None or (isinstance(v, float) and np.isnan(v)) else round(float(v), d)  # noqa: E731
    return {
        "player_id": int(pid), "name": r["name"], "team": r["team"], "position": r["position"],
        "roster_pct": f(r["roster_pct"]), "own_change": f(own3), "games": int(r["n_games"]),
        "level": f(r["level"]), "fv_last5": f(r["z5"]), "minutes_last5": f(r["min5"], 0),
        "pts": f(r["pts"]), "reb": f(r["reb"]), "ast": f(r["ast"]), "stl": f(r["stl"], 1), "blk": f(r["blk"], 1), "fg3m": f(r["fg3m"]),
        "reasons": reasons, "injury_status": r.get("injury_status"),
    }


def waiver_wire(games: pd.DataFrame, own_now: pd.DataFrame, own_then: pd.DataFrame | None, as_of: date,
                season_start: date | None, rules: Rules = DEFAULT_RULES, exclude_ids: frozenset | set = frozenset()) -> WireResult:
    """Today's risers: new to the top group (level), or trending up (ownership) with the level to back it.

    `own_now` / `own_then`: roster % snapshots (player_id, roster_pct, injury_status) from the SAME source, on `as_of` and
    `as_of - lag_days`. If there is no `own_then`, the earlier top group is built with today's ownership (and flagged).
    """
    if season_start is not None and (as_of - season_start).days < rules.min_days_into_season:
        return WireResult(as_of, False, "season_too_young")
    levels_now = player_levels(games, as_of, rules)
    if levels_now.empty:
        return WireResult(as_of, False, "no_games")
    pool_now = candidate_pool(levels_now, own_now, as_of, rules)
    top_now = top_group(pool_now, rules.top_k)

    then_day = as_of - timedelta(days=rules.lag_days)
    basis = "ownership_then"
    if own_then is None or own_then.empty:
        own_then_eff, basis = own_now, "current_ownership"
    else:
        own_then_eff = own_then
    levels_then = player_levels(games, then_day, rules)
    top_then = top_group(candidate_pool(levels_then, own_then_eff, then_day, rules), rules.top_k)

    own3 = pd.Series(dtype=float)
    if own_then is not None and not own_then.empty:
        a = own_now.drop_duplicates("player_id").set_index("player_id")["roster_pct"]
        b = own_then.drop_duplicates("player_id").set_index("player_id")["roster_pct"]
        own3 = (a - b.reindex(a.index)).dropna()

    new_ids = [p for p in top_now.index if p not in set(top_then.index)]
    q = pool_now["level"].quantile(rules.trend_level_quantile) if len(pool_now) else np.inf
    trending_ids = [p for p in pool_now.index
                    if p in own3.index and own3[p] >= rules.trend_min_rise and pool_now.loc[p, "level"] >= q and p not in new_ids]

    rows = []
    for p in [p for p in new_ids + trending_ids if p not in exclude_ids]:      # featured recently: do not repeat him
        reasons = (["new_in_top_group"] if p in new_ids else []) + (["ownership_up"] if p in own3.index and own3[p] >= rules.trend_min_rise else [])
        rows.append(_row(p, pool_now.loc[p], own3.get(p), reasons))
    rows.sort(key=lambda r: -(r["level"] if r["level"] is not None else -1e9))
    risers = rows[: rules.max_in_post]
    holdovers = [_row(p, pool_now.loc[p], own3.get(p), ["still_on_the_wire"]) for p in top_now.index if p in set(top_then.index)]

    worthy = len(risers) >= rules.min_risers
    return WireResult(as_of, worthy, None if worthy else "too_few_new_players", risers, holdovers, basis)


# --------------------------------------------------------------------------- text
SOURCE_LABEL = {"yahoo": "Yahoo", "espn": "ESPN"}


def latest_snapshot(ownership: pd.DataFrame, target: date, tolerance: int = 2) -> pd.DataFrame:
    """Each player's latest roster % on or before `target`, no older than `tolerance` days. Needs a snapshot_date column."""
    lo = target - timedelta(days=tolerance)
    sub = ownership[(ownership["snapshot_date"] <= target) & (ownership["snapshot_date"] >= lo)].sort_values("snapshot_date")
    cols = ["player_id", "roster_pct"] + (["injury_status"] if "injury_status" in sub else [])
    return sub.drop_duplicates("player_id", keep="last")[cols]


def waiver_wire_for_day(games: pd.DataFrame, ownership: pd.DataFrame, as_of: date, season_start: date | None,
                        rules: Rules = DEFAULT_RULES, exclude_ids: frozenset | set = frozenset()) -> WireResult:
    """`waiver_wire` from raw ownership snapshots (player_id, roster_pct, snapshot_date, optional injury_status).

    `exclude_ids` = players already featured recently. They come from the `waiver_wire_featured` ledger, written when a
    post is actually published, so a post you skipped by hand never blocks anyone.
    """
    return waiver_wire(games, latest_snapshot(ownership, as_of), latest_snapshot(ownership, as_of - timedelta(days=rules.lag_days)),
                       as_of, season_start, rules, exclude_ids=exclude_ids)


def _extras(r: dict, zh: bool = True) -> list[str]:
    """Standout steals / blocks / threes (last-5 averages), so a pick that is not obvious from pts/reb/ast is explained."""
    out = []
    if (r.get("blk") or 0) >= 1.2: out.append(f"{r['blk']:.1f}阻攻" if zh else f"{r['blk']:.1f} blk")
    if (r.get("stl") or 0) >= 1.5: out.append(f"{r['stl']:.1f}抄截" if zh else f"{r['stl']:.1f} stl")
    if (r.get("fg3m") or 0) >= 2.5: out.append(f"{r['fg3m']:.1f}三分" if zh else f"{r['fg3m']:.1f} 3PM")
    return out[:2]


def render_zh(res: WireResult, source: str) -> str:
    """Traditional Chinese Threads post. No forecast wording, no internal metrics, and NO ownership percentages
    (ESPN's differ a lot from Yahoo's, so showing them would mislead). Numbers are copied from the data."""
    if not res.post_worthy:
        return ""
    d = res.as_of
    lines = [f"🔥 自由市場推薦名單｜{d.month}/{d.day}", ""]
    for i, r in enumerate(res.risers, 1):
        lines.append(f"{i}. {r['name']}（{r['team']}）")
        lines.append("   近5場平均 " + "｜".join([f"{r['pts']:.0f}分 {r['reb']:.0f}板 {r['ast']:.0f}助"] + _extras(r) + [f"{r['minutes_last5']:.0f}分鐘"]))
    if res.holdovers:
        lines += ["", "📌 持續在榜：" + "、".join(h["name"] for h in res.holdovers[:3])]
    lines += ["", "（只是近況，不是預測）"]
    return "\n".join(lines)


def render_en(res: WireResult, source: str) -> str:
    if not res.post_worthy:
        return ""
    d = res.as_of
    lines = [f"🔥 Waiver Wire: new names | {d.month}/{d.day}", ""]
    for i, r in enumerate(res.risers, 1):
        lines.append(f"{i}. {r['name']} ({r['team']})")
        lines.append("   Last 5 avg: " + " | ".join([f"{r['pts']:.0f} pts, {r['reb']:.0f} reb, {r['ast']:.0f} ast"] + _extras(r, False) + [f"{r['minutes_last5']:.0f} min"]))
    if res.holdovers:
        lines += ["", "Still on the wire: " + ", ".join(h["name"] for h in res.holdovers[:3])]
    lines += ["", "(Recent form, not a forecast.)"]
    return "\n".join(lines)
