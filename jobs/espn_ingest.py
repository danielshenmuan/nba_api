#!/usr/bin/env python3
"""ESPN box score + fantasy ownership ingest.

Produces rows shaped exactly like the existing player_daily_game_stats tables
(see infra/bq/ddl/nba_data.player_daily_game_stats_espn.sql) so downstream
consumers (views, the FastAPI service, n8n) need no changes.

IMPORTANT - these are ESPN's *unofficial* endpoints, the ones its own site and
app call. They have no SLA and Disney's terms prohibit scripted access. Keep
volume tiny, never retry on 403, and alert on failure. See docs/DATA-SOURCES.md.

Subcommands
    games           box scores for one date or a range
    ownership       ESPN Fantasy percentOwned snapshot (current values only)
    daily           yesterday's games + today's ownership (the Cloud Run job)
    seed-universe   (re)build nba_data.nba_player_universe (needs nba_api, local)

Examples
    python espn_ingest.py games --date 2026-01-04 --dry-run
    python espn_ingest.py games --start 2025-10-01 --end 2026-04-20
    python espn_ingest.py ownership
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from google.cloud import bigquery

PROJECT = os.getenv("PROJECT_ID", "fantasy-survivor-app")
DATASET = "nba_data"
BQ_LOCATION = "northamerica-northeast1"
T_GAMES = f"{PROJECT}.{DATASET}.player_daily_game_stats_espn"
T_OWN = f"{PROJECT}.{DATASET}.player_ownership_espn"
T_MAP = f"{PROJECT}.{DATASET}.espn_player_map"
T_UNIV = f"{PROJECT}.{DATASET}.nba_player_universe"

SITE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
FANTASY = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/{year}/segments/0/leaguedefaults/3"
USER_AGENT = "fantasy-survivors-ingest/1.0 (non-commercial hobby project)"
THROTTLE_SECONDS = float(os.getenv("ESPN_THROTTLE_SECONDS", "0.4"))
CACHE_DIR = Path(os.environ["ESPN_CACHE_DIR"]) if os.getenv("ESPN_CACHE_DIR") else None
REGULAR_SEASON = 2  # ESPN season.type: 1 pre, 2 regular, 3 post, 5 play-in
ET = ZoneInfo("America/New_York")

# Same constants as daily_ingest_sportsblaze.py, so z_score is directly comparable.
# Order: PTS, REB, AST, STL, BLK, FG3M, FG%, FT%, TO
WEIGHTED_MEAN = [11.69, 4.32, 2.76, 0.75, 0.50, 1.28, 0.47, 0.75, 1.33]
WEIGHTED_STD = [7.23, 2.51, 2.09, 0.38, 0.45, 0.95, 0.082, 0.124, 0.85]

NBA_TEAM_ID = {
    "ATL": 1610612737, "BOS": 1610612738, "CLE": 1610612739, "NOP": 1610612740,
    "CHI": 1610612741, "DAL": 1610612742, "DEN": 1610612743, "GSW": 1610612744,
    "HOU": 1610612745, "LAC": 1610612746, "LAL": 1610612747, "MIA": 1610612748,
    "MIL": 1610612749, "MIN": 1610612750, "BKN": 1610612751, "NYK": 1610612752,
    "ORL": 1610612753, "IND": 1610612754, "PHI": 1610612755, "PHX": 1610612756,
    "POR": 1610612757, "SAC": 1610612758, "SAS": 1610612759, "OKC": 1610612760,
    "TOR": 1610612761, "UTA": 1610612762, "MEM": 1610612763, "WAS": 1610612764,
    "DET": 1610612765, "CHA": 1610612766,
}
ESPN_ABBR_FIX = {"GS": "GSW", "NY": "NYK", "SA": "SAS", "NO": "NOP", "UTAH": "UTA", "WSH": "WAS", "PHO": "PHX"}
SLUG_FIX = {"trail blazers": "blazers", "76ers": "sixers"}
NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

GAME_COLUMNS = [
    "game_date", "game_id", "player_id", "player_name", "team_id", "team_abbr", "team_city",
    "team_name", "team_slug", "position", "comment", "jersey_num", "minutes",
    "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct",
    "pts", "reb", "ast", "stl", "blk", "turnovers", "pf", "dreb", "oreb", "plus_minus",
    "z_score", "season",
]
INT_COLS = ["game_id", "player_id", "team_id", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
            "pts", "reb", "ast", "stl", "blk", "turnovers", "pf", "dreb", "oreb", "plus_minus"]


class BlockedError(RuntimeError):
    """ESPN answered 403. Stop immediately; never hammer a block."""


# --------------------------------------------------------------------------- fetch
_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})


def get_json(url: str, params: dict | None = None, headers: dict | None = None, cache_key: str | None = None):
    if CACHE_DIR and cache_key:
        p = CACHE_DIR / f"{cache_key}.json"
        if p.exists():
            return json.loads(p.read_text())
    for attempt in range(5):
        time.sleep(THROTTLE_SECONDS)
        r = _session.get(url, params=params, headers=headers, timeout=30)
        if r.status_code == 403:
            raise BlockedError(f"ESPN returned 403 for {url}")
        if r.status_code == 404:
            return None
        if r.status_code == 429 or r.status_code >= 500:
            wait = 2 ** attempt
            print(f"  HTTP {r.status_code}, backing off {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        r.raise_for_status()
        data = r.json()
        if CACHE_DIR and cache_key:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            (CACHE_DIR / f"{cache_key}.json").write_text(json.dumps(data))
        return data
    raise RuntimeError(f"ESPN kept failing for {url}")


# --------------------------------------------------------------------------- helpers
def season_from_date(d: date) -> str:
    return f"{d.year}-{(d.year + 1) % 100:02d}" if d.month >= 10 else f"{d.year - 1}-{d.year % 100:02d}"


def norm_name(name: str) -> str:
    s = unicodedata.normalize("NFKD", name or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = s.replace(".", "").replace("'", "").replace("’", "").replace("-", " ")
    return " ".join(t for t in re.split(r"\s+", s.strip()) if t and t not in NAME_SUFFIXES)


def split_made_att(s: str) -> tuple[int, int]:
    try:
        made, att = s.split("-")
        return int(made), int(att)
    except Exception:
        return 0, 0


def to_int(s, default: int = 0) -> int:
    try:
        return int(float(str(s).replace("+", "")))
    except Exception:
        return default


# --------------------------------------------------------------------------- games
def list_events(d: date) -> list[dict]:
    data = get_json(f"{SITE}/scoreboard", params={"dates": d.strftime("%Y%m%d")},
                    cache_key=f"scoreboard_{d:%Y%m%d}")
    return (data or {}).get("events", [])


def parse_summary(d: date, event_id: str, s: dict) -> list[dict]:
    comp = s["header"]["competitions"][0]
    if not comp["status"]["type"].get("completed"):
        return []
    rows: list[dict] = []
    for team_block in s.get("boxscore", {}).get("players", []):
        team = team_block["team"]
        abbr = ESPN_ABBR_FIX.get(team.get("abbreviation", ""), team.get("abbreviation", ""))
        name = team.get("name", "")
        for group in team_block.get("statistics", [])[:1]:
            idx = {label: i for i, label in enumerate(group["labels"])}
            for a in group.get("athletes", []):
                st = a.get("stats") or []
                if a.get("didNotPlay") or not st:
                    continue
                g = lambda k: st[idx[k]] if k in idx and idx[k] < len(st) else ""
                minutes = float(to_int(g("MIN")))
                if minutes <= 0:
                    continue
                fgm, fga = split_made_att(g("FG"))
                fg3m, fg3a = split_made_att(g("3PT"))
                ftm, fta = split_made_att(g("FT"))
                ath = a["athlete"]
                rows.append({
                    "game_date": d, "game_id": int(event_id), "espn_id": int(ath["id"]),
                    "player_name": ath.get("displayName"),
                    "team_id": NBA_TEAM_ID.get(abbr), "team_abbr": abbr,
                    "team_city": team.get("location"), "team_name": name,
                    "team_slug": SLUG_FIX.get(name.lower(), name.lower().replace(" ", "")),
                    "position": (ath.get("position") or {}).get("abbreviation"),
                    "comment": None, "jersey_num": ath.get("jersey"), "minutes": minutes,
                    "fgm": fgm, "fga": fga, "fg_pct": fgm / fga if fga else 0.0,
                    "fg3m": fg3m, "fg3a": fg3a, "fg3_pct": fg3m / fg3a if fg3a else 0.0,
                    "ftm": ftm, "fta": fta, "ft_pct": ftm / fta if fta else 0.0,
                    "pts": to_int(g("PTS")), "reb": to_int(g("REB")), "ast": to_int(g("AST")),
                    "stl": to_int(g("STL")), "blk": to_int(g("BLK")), "turnovers": to_int(g("TO")),
                    "pf": to_int(g("PF")), "dreb": to_int(g("DREB")), "oreb": to_int(g("OREB")),
                    "plus_minus": to_int(g("+/-")), "season": season_from_date(d),
                })
    return rows


def fetch_games(d: date, season_types=(REGULAR_SEASON,)) -> list[dict]:
    rows: list[dict] = []
    for ev in list_events(d):
        stype = (ev.get("season") or {}).get("type")
        if stype not in season_types or not ev.get("status", {}).get("type", {}).get("completed"):
            continue
        s = get_json(f"{SITE}/summary", params={"event": ev["id"]}, cache_key=f"summary_{ev['id']}")
        if s:
            rows.extend(parse_summary(d, ev["id"], s))
    return rows


def add_zscores(df: pd.DataFrame) -> pd.DataFrame:
    """Identical math to daily_ingest_sportsblaze.compute_zscores, vectorised."""
    vals = df[["pts", "reb", "ast", "stl", "blk", "fg3m", "fg_pct", "ft_pct", "turnovers"]].astype(float).to_numpy()
    z = (vals - np.array(WEIGHTED_MEAN)) / np.array(WEIGHTED_STD)
    mult = np.column_stack([np.ones((len(df), 6)), df["fga"].astype(float) / 20.0,
                            df["fta"].astype(float) / 8.0, -np.ones(len(df))])
    df["z_score"] = np.round((z * mult).sum(axis=1), 3)
    return df


# --------------------------------------------------------------------------- id resolution
def load_universe(bq: bigquery.Client) -> dict[str, list[dict]]:
    q = f"SELECT nba_id, norm_name, is_active, in_bq_history FROM `{T_UNIV}`"
    out: dict[str, list[dict]] = {}
    for r in bq.query(q, location=BQ_LOCATION).result():
        out.setdefault(r.norm_name, []).append(dict(r))
    return out


def resolve_one(name: str, universe: dict[str, list[dict]]) -> int | None:
    cands = universe.get(norm_name(name), [])
    for pred in (lambda c: c["in_bq_history"], lambda c: c["is_active"], lambda c: True):
        tier = {c["nba_id"] for c in cands if pred(c)}
        if len(tier) == 1:
            return tier.pop()
        if len(tier) > 1:
            return None  # ambiguous: refuse to guess
    return None


def resolve_players(bq: bigquery.Client, pairs: dict[int, str], persist: bool = True) -> dict[int, int | None]:
    """espn_id -> nba_id (None when unresolved).

    Only RESOLVED mappings are persisted to espn_player_map. Unresolved players are
    re-attempted every run, so seeding or improving the universe later takes effect
    without clearing anything. A dry run persists nothing.
    """
    ids = list(pairs)
    known: dict[int, int | None] = {}
    if ids:
        q = f"SELECT espn_player_id, nba_player_id FROM `{T_MAP}` WHERE espn_player_id IN UNNEST(@ids)"
        cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", ids)])
        for r in bq.query(q, job_config=cfg, location=BQ_LOCATION).result():
            known[r.espn_player_id] = r.nba_player_id
    new = {i: pairs[i] for i in ids if i not in known}
    if new:
        universe = load_universe(bq)
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for eid, nm in new.items():
            nba = resolve_one(nm, universe)
            known[eid] = nba
            if nba:
                rows.append({"espn_player_id": eid, "nba_player_id": nba, "espn_name": nm,
                             "method": "exact_norm_name", "updated_at": now})
        if persist and rows:
          # Load job, not streaming insert: streamed rows cannot be UPDATEd for ~an hour,
        # and fixing a mapping with a plain UPDATE is the whole point of this table.
          bq.load_table_from_json(rows, T_MAP, location=BQ_LOCATION,
              job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()
    return known


def apply_ids(df: pd.DataFrame, bq: bigquery.Client, persist: bool = True) -> pd.DataFrame:
    mapping = resolve_players(bq, dict(zip(df["espn_id"], df["player_name"])), persist=persist)
    df["player_id"] = [mapping.get(e) if mapping.get(e) else -int(e) for e in df["espn_id"]]
    return df


# --------------------------------------------------------------------------- BigQuery io
def games_schema() -> list[bigquery.SchemaField]:
    fields = []
    for c in GAME_COLUMNS:
        if c == "game_date":
            t = "DATE"
        elif c in INT_COLS:
            t = "INT64"
        elif c in ("minutes", "fg_pct", "fg3_pct", "ft_pct", "z_score"):
            t = "FLOAT64"
        else:
            t = "STRING"
        fields.append(bigquery.SchemaField(c, t))
    return fields


def write_games(bq: bigquery.Client, df: pd.DataFrame, dates: list[date]) -> None:
    """Idempotent: delete the dates being written, then append."""
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("d", "DATE", dates)])
    bq.query(f"DELETE FROM `{T_GAMES}` WHERE game_date IN UNNEST(@d)", job_config=cfg, location=BQ_LOCATION).result()
    if df.empty:
        return
    out = df[GAME_COLUMNS].copy()
    out["game_date"] = pd.to_datetime(out["game_date"]).dt.date
    job = bq.load_table_from_dataframe(
        out, T_GAMES, location=BQ_LOCATION,
        job_config=bigquery.LoadJobConfig(schema=games_schema(), write_disposition="WRITE_APPEND"))
    job.result()


def run_games(start: date, end: date, dry_run: bool, flush_days: int = 14) -> pd.DataFrame:
    bq = bigquery.Client(project=PROJECT, location=BQ_LOCATION)
    pending: list[dict] = []
    pending_dates: list[date] = []
    total = 0
    frames: list[pd.DataFrame] = []

    def flush():
        nonlocal pending, pending_dates, total
        if not pending_dates:
            return
        df = pd.DataFrame(pending, columns=None) if pending else pd.DataFrame(columns=GAME_COLUMNS + ["espn_id"])
        if not df.empty:
            df = add_zscores(apply_ids(df, bq, persist=not dry_run))
            frames.append(df)
        if not dry_run:
            write_games(bq, df if not df.empty else pd.DataFrame(columns=GAME_COLUMNS), pending_dates)
        total += len(df)
        print(f"  flushed {pending_dates[0]}..{pending_dates[-1]}: {len(df)} rows (running total {total})", flush=True)
        pending, pending_dates = [], []

    d = start
    while d <= end:
        rows = fetch_games(d)
        pending.extend(rows)
        pending_dates.append(d)
        print(f"{d}: {len({r['game_id'] for r in rows})} games, {len(rows)} player lines", flush=True)
        if len(pending_dates) >= flush_days:
            flush()
        d += timedelta(days=1)
    flush()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# --------------------------------------------------------------------------- ownership
def fetch_ownership(season_year: int) -> list[dict]:
    flt = {"players": {"limit": 1000, "sortPercOwned": {"sortPriority": 1, "sortAsc": False},
                       "filterSlotIds": {"value": list(range(0, 12))}}}
    data = get_json(FANTASY.format(year=season_year), params={"view": "kona_player_info"},
                    headers={"X-Fantasy-Filter": json.dumps(flt)})
    return (data or {}).get("players", [])


def run_ownership(season_year: int | None, dry_run: bool) -> pd.DataFrame:
    today = datetime.now(ET).date()
    if season_year is None:
        season_year = today.year + 1 if today.month >= 10 else today.year
    players = fetch_ownership(season_year)
    rows = []
    for p in players:
        pl, own = p.get("player", {}), p.get("player", {}).get("ownership", {})
        if pl.get("id") is None or own.get("percentOwned") is None:
            continue
        rows.append({"espn_id": int(pl["id"]), "player_name": pl.get("fullName"),
                     "roster_pct": float(own["percentOwned"]), "snapshot_date": today,
                     "pct_change": own.get("percentChange"), "pct_started": own.get("percentStarted"),
                     "injury_status": pl.get("injuryStatus")})
    df = pd.DataFrame(rows)
    print(f"ownership season={season_year}: {len(df)} players, snapshot_date={today}")
    if df.empty:
        return df
    bq = bigquery.Client(project=PROJECT, location=BQ_LOCATION)
    mapping = resolve_players(bq, dict(zip(df["espn_id"], df["player_name"])), persist=not dry_run)
    df["player_id"] = [mapping.get(e) if mapping.get(e) else -int(e) for e in df["espn_id"]]
    df["espn_player_id"] = df["espn_id"]
    out = df[["player_id", "player_name", "roster_pct", "snapshot_date", "pct_change",
              "pct_started", "injury_status", "espn_player_id"]]
    if not dry_run:
        cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("d", "DATE", today)])
        bq.query(f"DELETE FROM `{T_OWN}` WHERE snapshot_date = @d", job_config=cfg, location=BQ_LOCATION).result()
        bq.load_table_from_dataframe(out, T_OWN, location=BQ_LOCATION,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()
    return out


# --------------------------------------------------------------------------- universe seed
def seed_universe() -> None:
    from nba_api.stats.static import players  # offline static list, local use only
    bq = bigquery.Client(project=PROJECT, location=BQ_LOCATION)
    hist = bq.query(f"""
        SELECT DISTINCT player_id AS nba_id, player_name FROM `{PROJECT}.{DATASET}.player_daily_game_stats` WHERE player_name IS NOT NULL
        UNION DISTINCT SELECT DISTINCT player_id, player_name FROM `{PROJECT}.{DATASET}.player_ownership` WHERE player_name IS NOT NULL
        UNION DISTINCT SELECT DISTINCT player_id, player_name FROM `{PROJECT}.{DATASET}.player_historical_game_stats` WHERE player_name IS NOT NULL
    """, location=BQ_LOCATION).to_dataframe()
    hist = hist.dropna(subset=["nba_id"])
    rows = {}
    for p in players.get_players():
        rows[p["id"]] = {"nba_id": p["id"], "full_name": p["full_name"], "is_active": bool(p["is_active"]), "in_bq_history": False}
    for _, r in hist.iterrows():
        rec = rows.setdefault(int(r.nba_id), {"nba_id": int(r.nba_id), "full_name": r.player_name, "is_active": True})
        rec["in_bq_history"] = True
        rec["full_name"] = r.player_name
    df = pd.DataFrame(rows.values())
    df["norm_name"] = df["full_name"].map(norm_name)
    df = df[["nba_id", "full_name", "norm_name", "is_active", "in_bq_history"]]
    bq.load_table_from_dataframe(df, T_UNIV, location=BQ_LOCATION,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
    print(f"universe seeded: {len(df)} ids ({int(df.in_bq_history.sum())} seen in BQ history)")


# --------------------------------------------------------------------------- cli
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("games"); g.add_argument("--date"); g.add_argument("--start"); g.add_argument("--end")
    g.add_argument("--dry-run", action="store_true")
    o = sub.add_parser("ownership"); o.add_argument("--season-year", type=int); o.add_argument("--dry-run", action="store_true")
    dly = sub.add_parser("daily"); dly.add_argument("--dry-run", action="store_true")
    sub.add_parser("seed-universe")
    a = ap.parse_args()
    parse = lambda s: datetime.strptime(s, "%Y-%m-%d").date()
    try:
        if a.cmd == "games":
            start = parse(a.date or a.start); end = parse(a.date or a.end or a.start)
            df = run_games(start, end, a.dry_run)
            print(f"done: {len(df)} player lines, {int((df.player_id < 0).sum()) if len(df) else 0} unresolved ids")
        elif a.cmd == "ownership":
            run_ownership(a.season_year, a.dry_run)
        elif a.cmd == "daily":
            yday = datetime.now(ET).date() - timedelta(days=1)
            run_games(yday, yday, a.dry_run)
            run_ownership(None, a.dry_run)
        elif a.cmd == "seed-universe":
            seed_universe()
    except BlockedError as e:
        print(f"BLOCKED: {e}", file=sys.stderr)
        sys.exit(3)


if __name__ == "__main__":
    main()
