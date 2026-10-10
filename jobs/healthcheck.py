#!/usr/bin/env python3
"""Pipeline health check. Exits 1 if anything is wrong, so a scheduled Cloud Run
job running this is itself visible as FAILED. Optionally posts to Discord.

Checks
  1. every public API endpoint returns 200
  2. ESPN is reachable (not blocked) - the source we depend on
  3. data freshness: games (in season only) and the daily ownership snapshot
  4. identity health: share of recent lines with unresolved (negative) player ids

Env
  DISCORD_WEBHOOK_URL   optional; failures and warnings are posted here
  POST_SUMMARY=1        also post an "all clear" when everything passes (off by default)
  SEASON_START / SEASON_END   ISO dates; games freshness is only enforced in between
"""
from __future__ import annotations
import os, sys, json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from google.cloud import bigquery

PROJECT = os.getenv("PROJECT_ID", "fantasy-survivor-app")
LOC = "northamerica-northeast1"
API = os.getenv("API_BASE", "https://nba-gbq-api-896368614747.us-central1.run.app")
SEASON_START = date.fromisoformat(os.getenv("SEASON_START", "2026-10-20"))
SEASON_END = date.fromisoformat(os.getenv("SEASON_END", "2027-04-12"))
GAMES_MAX_AGE_DAYS = int(os.getenv("GAMES_MAX_AGE_DAYS", "3"))  # All-Star break is the longest legit gap
TODAY = datetime.now(ZoneInfo("America/New_York")).date()
IN_SEASON = SEASON_START <= TODAY <= SEASON_END

failures: list[str] = []
warnings: list[str] = []   # reported, but do not fail the run (e.g. the SportsBlaze fallback)
notes: list[str] = []


def check(ok: bool, msg: str, fail_msg: str) -> None:
    (notes if ok else failures).append(("OK  " if ok else "FAIL ") + (msg if ok else fail_msg))


# 1. API endpoints
for name, path in {
    "health": "/health",
    "players_search": "/v1/players_search?q=jokic&limit=1",
    "daily_leaders": "/v1/daily_leaders?limit=1",
    "player_baselines": "/v1/player_baselines?player_id=203999&season=2025-26",
    "player_timeseries": "/v1/player_timeseries?player_id=203999&start_date=2026-03-01&end_date=2026-03-05&limit=1",
    "player_roster_pct": "/v1/player_roster_pct?player_id=203999",
}.items():
    try:
        code = requests.get(API + path, timeout=45).status_code
    except Exception as e:
        code = f"{type(e).__name__}"
    check(code == 200, f"api {name} 200", f"api {name} returned {code}")

# 2. ESPN reachable
try:
    r = requests.get("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
                     headers={"User-Agent": "fantasy-survivors-ingest/1.0 (non-commercial hobby project)"}, timeout=30)
    check(r.status_code == 200, "espn scoreboard reachable", f"ESPN scoreboard returned {r.status_code} (blocked? 403)")
except Exception as e:
    check(False, "", f"ESPN unreachable: {type(e).__name__}")

# 3 + 4. BigQuery freshness and identity
bq = bigquery.Client(project=PROJECT, location=LOC)
q = lambda sql: list(bq.query(sql, location=LOC).result())[0]

own = q(f"SELECT MAX(snapshot_date) d FROM `{PROJECT}.nba_data.player_ownership_espn`").d
check(own is not None and own >= TODAY - timedelta(days=1),
      f"ownership snapshot fresh ({own})", f"ownership snapshot stale: latest {own}, today {TODAY}")

for label, table in (("espn", "player_daily_game_stats_espn"), ("sportsblaze", "player_daily_game_stats_p")):
    last = q(f"SELECT MAX(game_date) d FROM `{PROJECT}.nba_data.{table}`").d
    if IN_SEASON:
        ok = last is not None and (TODAY - last).days <= GAMES_MAX_AGE_DAYS
        stale = f"games[{label}] stale: latest {last}, {(TODAY - last).days if last else '?'} days old"
        if label == "sportsblaze" and not ok:
            # Kept only as a fallback; its API key expires monthly. Warn, do not fail the run.
            warnings.append("WARN " + stale + " (fallback only; key expired?)")
        else:
            check(ok, f"games[{label}] fresh (latest {last})", stale)
    else:
        notes.append(f"SKIP games[{label}] freshness (off-season; latest {last})")

row = q(f"""SELECT COUNT(*) n, COUNTIF(player_id < 0) neg
            FROM `{PROJECT}.nba_data.player_daily_game_stats_espn`
            WHERE game_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 14 DAY)""")
if row.n:
    rate = row.neg / row.n
    check(rate < 0.05, f"unresolved ids {rate:.1%} of {row.n} recent lines",
          f"unresolved ids {rate:.1%} of {row.n} recent lines (>5%): refresh nba_player_universe")

print(f"{TODAY} in_season={IN_SEASON}")
print("\n".join(notes + warnings + failures))
hook = os.getenv("DISCORD_WEBHOOK_URL")
body = None
if failures or warnings:
    head = "🚨 **Fantasy pipeline health check failed**" if failures else "⚠️ **Fantasy pipeline health check: warnings**"
    body = head + "\n" + "\n".join(f"• {m}" for m in failures + warnings)
elif os.getenv("POST_SUMMARY") == "1":
    # Optional "all clear" message. Off by default so a healthy pipeline stays silent.
    body = f"✅ **Fantasy pipeline health check passed** ({len(notes)} checks, {TODAY})"
if hook and body:
    hook = hook.strip()
    if not hook.startswith(("https://discord.com/api/webhooks/", "https://discordapp.com/api/webhooks/")):
        # Never print the value itself: it may contain a token.
        print("(DISCORD_WEBHOOK_URL is set but is not a Discord webhook URL. Re-create the secret with the URL itself.)")
    else:
        try:
            r = requests.post(hook, json={"content": body[:1900]}, timeout=20)
            print(f"Discord post: HTTP {r.status_code}")  # Discord returns 204 on success
        except Exception as e:
            # Log the error TYPE only: request errors can echo the URL, and the URL contains a token.
            print(f"(could not post to Discord: {type(e).__name__})")
if failures:
    sys.exit(1)
