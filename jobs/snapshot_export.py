#!/usr/bin/env python3
"""Nightly snapshot for the Fantasy Analyst skill and connector.

Writes ONE json file of derived numbers only: per-season per-game averages for every player who
played in the reference season or the season in progress. No game logs, no ownership. The league
baseline is computed from it at run time because it depends on the user's league size.

Format = `Snapshot` in fantasy-basketball-analyst/engine/src/snapshot.ts (version 1).

Definitions match production (league_pg_stats_by_season) so the numbers agree with the API:
  - a game counts only when minutes > 0
  - averages are AVG per game; fg_pct / ft_pct are the mean of the per-game percentages
  - seasons are newest first; reference_season = the last COMPLETED season (e.g. 2025-26 in Nov 2026)
  - the season in progress and the three before the reference are included: 4 seasons at most

Sources: player_historical_game_stats (2021-22 .. 2024-25) UNION player_daily_game_stats_espn (2025-26 on).

Usage
    python snapshot_export.py --out /path/snapshot.json
    python snapshot_export.py --out gs://fantasy-survivors-analyst-data/snapshot.json   # the nightly Cloud Run job
    python snapshot_export.py --out /path/snapshot.json --today 2026-11-15   # pin the date (tests)
Exit 1 when a sanity gate fails: nothing is written, so a bad snapshot is never published.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import unicodedata
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from google.cloud import bigquery

PROJECT = os.getenv("PROJECT_ID", "fantasy-survivor-app")
LOC = "northamerica-northeast1"
T_HIST = f"{PROJECT}.nba_data.player_historical_game_stats"
T_ESPN = f"{PROJECT}.nba_data.player_daily_game_stats_espn"
ALIASES_FILE = Path(__file__).with_name("data") / "player_aliases.json"
SEASONS_BEFORE_REFERENCE = 2   # reference + 2 earlier = the engine's 3-season window
MIN_REFERENCE_PLAYERS = 400    # a normal season has ~550; far fewer means an ingest problem
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

QUERY = f"""
WITH u AS (
  SELECT season, player_id, player_name, game_date, minute AS minutes,
         points AS pts, rebounds AS reb, assists AS ast, steals AS stl, blocks AS blk,
         three_p_made AS fg3m, fg_pct, ft_pct, turnovers, fg_attempts AS fga, ft_attempts AS fta,
         fg_made AS fgm, ft_made AS ftm
  FROM `{T_HIST}` WHERE season IN UNNEST(@seasons)
  UNION ALL
  SELECT season, player_id, player_name, game_date, minutes,
         pts, reb, ast, stl, blk, fg3m, fg_pct, ft_pct, turnovers, fga, fta, fgm, ftm
  FROM `{T_ESPN}` WHERE season IN UNNEST(@seasons)
)
SELECT season, player_id,
       ARRAY_AGG(player_name ORDER BY game_date DESC NULLS LAST LIMIT 1)[OFFSET(0)] AS name,
       COUNT(*) AS gp, AVG(minutes) AS mpg,
       AVG(pts) AS pts, AVG(reb) AS reb, AVG(ast) AS ast, AVG(stl) AS stl, AVG(blk) AS blk, AVG(fg3m) AS fg3m,
       AVG(fg_pct) AS fg_pct, AVG(ft_pct) AS ft_pct, AVG(turnovers) AS turnovers,
       AVG(COALESCE(fga, 0)) AS fga, AVG(COALESCE(fta, 0)) AS fta,
       AVG(COALESCE(fgm, 0)) AS fgm, AVG(COALESCE(ftm, 0)) AS ftm
FROM u
WHERE minutes > 0 AND player_id IS NOT NULL
GROUP BY season, player_id
"""

# Current team per player (latest game in the seasons exported). Used to count a player's games in a week.
TEAM_QUERY = f"""
SELECT player_id, ARRAY_AGG(team_abbr ORDER BY game_date DESC LIMIT 1)[OFFSET(0)] AS team
FROM `{T_ESPN}` WHERE season IN UNNEST(@seasons) AND player_id IS NOT NULL AND team_abbr IS NOT NULL
GROUP BY player_id
"""

# fgm / ftm are made shots per game. A team's FG% / FT% is made / attempted, which is how leagues score it;
# fg_pct / ft_pct (mean of per-game percentages) are kept for compatibility but understate players with 0-attempt games.
STATS = ["pts", "reb", "ast", "stl", "blk", "fg3m", "fg_pct", "ft_pct", "turnovers", "fga", "fta", "fgm", "ftm"]

ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_UA = {"User-Agent": "fantasy-survivors-ingest/1.0 (non-commercial hobby project)"}
SCHEDULE_DAYS = 28   # four fantasy weeks from this Monday: the matchup week plus three ahead
# ESPN's scoreboard abbreviations differ from the game tables for six teams.
TEAM_ALIAS = {"GS": "GSW", "NO": "NOP", "NY": "NYK", "SA": "SAS", "UTAH": "UTA", "WSH": "WAS"}


def season_label(start_year: int) -> str:
    return f"{start_year}-{(start_year + 1) % 100:02d}"


def seasons_for(today: date) -> tuple[str, list[str]]:
    """(reference, seasons to export newest first). The season starts in October."""
    current = today.year if today.month >= 10 else today.year - 1
    reference = current - 1
    years = [current] + [reference - i for i in range(SEASONS_BEFORE_REFERENCE + 1)]
    return season_label(reference), [season_label(y) for y in years]


def name_key(s: str) -> str:
    """Same rule as nameTokens() in the engine: no accents, punctuation or generational suffix."""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch)).lower()
    for ch in ".'’,":
        s = s.replace(ch, "")
    toks = s.replace("-", " ").replace("_", " ").split()
    return " ".join(t for i, t in enumerate(toks) if not (t in SUFFIXES and i > 0))


def monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())


def fetch_schedule(today: date, days: int = SCHEDULE_DAYS) -> tuple[dict | None, list[str]]:
    """Games per team per fantasy week (Monday to Sunday), from ESPN's scoreboard, one request per day.

    Returns (schedule, warnings). A failed day leaves that whole week out rather than reporting a wrong count;
    if nothing could be fetched the schedule is None and the snapshot is published without it (the engine then
    assumes an average week and says so)."""
    import requests
    start = monday_of(today)
    per_week: dict[str, dict[str, int]] = {}
    bad_weeks: set[str] = set()
    warnings: list[str] = []
    for i in range(days):
        d = start + timedelta(days=i)
        wk = monday_of(d).isoformat()
        try:
            r = requests.get(ESPN_SCOREBOARD, params={"dates": d.strftime("%Y%m%d")}, headers=ESPN_UA, timeout=30)
            r.raise_for_status()
            events = r.json()["events"]
        except Exception as e:  # noqa: BLE001 - any failure invalidates that week only
            bad_weeks.add(wk)
            warnings.append(f"schedule: {d} failed ({type(e).__name__}); week of {wk} left out")
            continue
        games = per_week.setdefault(wk, {})
        for ev in events:
            for c in ev["competitions"][0]["competitors"]:
                ab = c["team"]["abbreviation"]
                ab = TEAM_ALIAS.get(ab, ab)
                games[ab] = games.get(ab, 0) + 1
    weeks = [{"start": wk, "games": dict(sorted(g.items()))} for wk, g in sorted(per_week.items()) if wk not in bad_weeks and g]
    if not weeks:
        return None, warnings + ["schedule: nothing fetched; publishing without it"]
    return {"source": "espn", "weeks": weeks}, warnings


def build(rows: list[dict], reference: str, season_order: list[str], aliases: dict[str, str],
          teams: dict[int, str] | None = None) -> tuple[dict, list[str]]:
    """Returns (snapshot without generated_at, warnings)."""
    warnings: list[str] = []
    teams = teams or {}
    rank = {s: i for i, s in enumerate(season_order)}
    by_id: dict[int, dict] = {}
    for r in sorted(rows, key=lambda r: rank[r["season"]]):  # newest first, so the first name seen is the latest
        p = by_id.setdefault(int(r["player_id"]), {"id": int(r["player_id"]), "name": r["name"], "seasons": []})
        p["seasons"].append({
            "season": r["season"], "gp": int(r["gp"]), "mpg": round(float(r["mpg"]), 2),
            "averages": {k: round(float(r[k] or 0), 4) for k in STATS},
        })
    # Only players with a game in the reference season or the season in progress can be asked about.
    recent = {season_order[0], reference}
    players = sorted((p for p in by_id.values() if any(s["season"] in recent for s in p["seasons"])), key=lambda p: p["id"])
    for p in players:
        if p["id"] in teams:
            p["team"] = teams[p["id"]]

    # Names that two ids share would make the engine report "ambiguous". Report, never merge by guess.
    seen: dict[str, list[dict]] = {}
    for p in players:
        seen.setdefault(name_key(p["name"]), []).append(p)
    for k, ps in seen.items():
        if len(ps) > 1:
            warnings.append(f"same name under {len(ps)} ids: {[(p['id'], p['name']) for p in ps]}")

    for alias, full in aliases.items():
        hits = seen.get(name_key(full), [])
        if len(hits) != 1:
            warnings.append(f"alias {alias!r} -> {full!r}: {'not found' if not hits else 'ambiguous'}; skipped")
            continue
        hits[0].setdefault("aliases", []).append(alias)

    return {"version": 1, "reference_season": reference, "players": players}, warnings


def gates(snap: dict, reference: str) -> list[str]:
    bad = []
    n_ref = sum(1 for p in snap["players"] if any(s["season"] == reference and s["gp"] > 0 for s in p["seasons"]))
    if n_ref < MIN_REFERENCE_PLAYERS:
        bad.append(f"only {n_ref} players with games in {reference} (need {MIN_REFERENCE_PLAYERS})")
    for p in snap["players"]:
        for s in p["seasons"]:
            if any(v != v or abs(v) > 200 for v in s["averages"].values()):
                bad.append(f"implausible average for {p['name']} {s['season']}")
    return bad


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True, help="path of the json file to write")
    ap.add_argument("--today", help="ISO date to pretend it is (tests); default: today in US Eastern")
    args = ap.parse_args()

    today = date.fromisoformat(args.today) if args.today else datetime.now(ZoneInfo("America/New_York")).date()
    reference, season_order = seasons_for(today)
    aliases = json.loads(ALIASES_FILE.read_text()) if ALIASES_FILE.exists() else {}

    bq = bigquery.Client(project=PROJECT, location=LOC)
    job = bq.query(QUERY, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("seasons", "STRING", season_order)]))
    rows = [dict(r) for r in job.result()]
    team_job = bq.query(TEAM_QUERY, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("seasons", "STRING", season_order)]))
    teams = {int(r["player_id"]): r["team"] for r in team_job.result()}

    snap, warnings = build(rows, reference, season_order, aliases, teams)
    schedule, sched_warnings = fetch_schedule(today)
    warnings += sched_warnings
    problems = gates(snap, reference)
    for w in warnings:
        print("WARN", w)
    if problems:
        for p in problems:
            print("FAIL", p)
        return 1
    out_snap = {"version": 1, "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "reference_season": snap["reference_season"], "players": snap["players"]}
    if schedule:
        out_snap["schedule"] = schedule
    snap = out_snap

    body = json.dumps(snap, ensure_ascii=False, separators=(",", ":"))
    if args.out.startswith("gs://"):
        from google.cloud import storage
        bucket, _, name = args.out[5:].partition("/")
        blob = storage.Client(project=PROJECT).bucket(bucket).blob(name)
        blob.cache_control = "public, max-age=300"   # a fresh file is visible within 5 minutes
        blob.upload_from_string(body, content_type="application/json")  # one object write: readers never see half a file
    else:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(out.suffix + ".tmp")
        tmp.write_text(body)
        tmp.replace(out)  # atomic
    neg = sum(1 for p in snap["players"] if p["id"] < 0)
    print(f"OK {len(snap['players'])} players ({neg} with surrogate ids), seasons {season_order}, "
          f"reference {reference}, {len(body) / 1024:.0f} KB -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
