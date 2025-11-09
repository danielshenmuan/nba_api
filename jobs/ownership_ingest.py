# jobs/ownership_ingest.py
import argparse
import os
import time
from datetime import date
from pathlib import Path

import google.auth
import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from requests.exceptions import HTTPError
from yfpy.exceptions import YahooFantasySportsDataNotFound
from yfpy.models import Player
from yfpy.query import YahooFantasySportsQuery

PROJECT = "fantasy-survivor-app"
DATASET = "nba_data"
TABLE   = f"{PROJECT}.{DATASET}.player_ownership"
BQ_LOCATION = "northamerica-northeast1"  # set to your dataset location

RATE_LIMIT_DELAY_SECONDS = float(os.getenv("YAHOO_API_DELAY_SECONDS", "0.5"))
RATE_LIMIT_MAX_RETRIES = int(os.getenv("YAHOO_API_MAX_RETRIES", "3"))
RATE_LIMIT_BACKOFF = float(os.getenv("YAHOO_API_BACKOFF", "2.0"))
PERCENT_OWNED_BATCH_SIZE = int(os.getenv("YAHOO_PERCENT_BATCH_SIZE", "25"))

# ---------- ENV (load root .env explicitly) ----------
# ROOT_ENV = Path(__file__).resolve().parents[1] / ".env"
# vals = dotenv_values(ROOT_ENV)
# need = ["YAHOO_CONSUMER_KEY","YAHOO_CONSUMER_SECRET","YAHOO_ACCESS_TOKEN","YAHOO_ACCESS_TOKEN_SECRET","YAHOO_LEAGUE_ID"]
# miss = [k for k in need if not vals.get(k)]
# if miss:
#     raise RuntimeError(f"Missing in {ROOT_ENV}: {miss}")
# os.environ.update({k: v for k, v in vals.items() if v})
#
# GAME_ID = os.environ.get("YAHOO_GAME_ID")  # strongly recommended to avoid prompts

ROOT_ENV = Path(__file__).resolve().parents[1] / ".env"
if ROOT_ENV.exists():
    try:
        from dotenv import dotenv_values
        os.environ.update({k: v for k, v in dotenv_values(ROOT_ENV).items() if v})
        print(f"[env] loaded {ROOT_ENV}")
    except Exception:
        pass  # ignore if python-dotenv not installed in the image

# Required in any environment (Cloud Run: provided via --set-env-vars)
REQ = [
    "YAHOO_CONSUMER_KEY",
    "YAHOO_CONSUMER_SECRET",
    "YAHOO_ACCESS_TOKEN",
    "YAHOO_REFRESH_TOKEN",
    # optional but nice to have:
    # "YAHOO_TOKEN_TYPE", "YAHOO_TOKEN_EXPIRES_AT"
]
missing = [k for k in REQ if not os.getenv(k)]
if missing:
    raise SystemExit(f"Missing required env vars: {missing}")

TMP_ENV = Path("/tmp/yahoo_tokens.env")
lines = [
    f"YAHOO_CONSUMER_KEY={os.environ['YAHOO_CONSUMER_KEY']}",
    f"YAHOO_CONSUMER_SECRET={os.environ['YAHOO_CONSUMER_SECRET']}",
    f"YAHOO_ACCESS_TOKEN={os.environ['YAHOO_ACCESS_TOKEN']}",
    f"YAHOO_REFRESH_TOKEN={os.environ['YAHOO_REFRESH_TOKEN']}",
    f"YAHOO_LEAGUE_ID={os.environ['YAHOO_LEAGUE_ID']}",
    f"YAHOO_GAME_ID={os.getenv('YAHOO_GAME_ID','')}",
]
# optional extras if you have them
if os.getenv("YAHOO_TOKEN_TYPE"):      lines.append(f"YAHOO_TOKEN_TYPE={os.environ['YAHOO_TOKEN_TYPE']}")
# if os.getenv("YAHOO_TOKEN_EXPIRES_AT"): lines.append(f"YAHOO_TOKEN_EXPIRES_AT={os.environ['YAHOO_TOKEN_EXPIRES_AT']}")
TMP_ENV.write_text("\n".join(lines))

# ---------- Helpers ----------
def _bq() -> bigquery.Client:
    return bigquery.Client(project=PROJECT, location=BQ_LOCATION)

def _player_dim_map(client: bigquery.Client) -> pd.DataFrame:
    # latest season name->id from your fact table (may miss brand-new players)
    sql = f"""
    WITH latest AS (
      SELECT season
      FROM `{PROJECT}.{DATASET}.player_daily_game_stats_p`
      WHERE season IS NOT NULL
      ORDER BY season DESC
      LIMIT 1
    )
    SELECT DISTINCT player_name, player_id
    FROM `{PROJECT}.{DATASET}.player_daily_game_stats_p`
    WHERE season = (SELECT season FROM latest)
    """
    df = client.query(sql, location=BQ_LOCATION).to_dataframe()
    df["name_lc"] = df["player_name"].str.strip().str.lower()
    return df[["name_lc", "player_id", "player_name"]]

def _yahoo_query():
    kwargs = dict(
        league_id=os.environ["YAHOO_LEAGUE_ID"],
        game_code="nba",
        yahoo_consumer_key=os.environ["YAHOO_CONSUMER_KEY"],
        yahoo_consumer_secret=os.environ["YAHOO_CONSUMER_SECRET"],
        env_file_location=TMP_ENV,  # writable in Cloud Run
    )
    gid = os.getenv("YAHOO_GAME_ID")
    if gid:
        kwargs["game_id"] = int(gid)
    return YahooFantasySportsQuery(**kwargs)

def _name_of(p) -> str | None:
    return (getattr(getattr(p, "name", None), "full", None) or getattr(p, "full_name", None))

def _extract_percent_value(po_obj) -> float | None:
    """Return float percent from Yahoo payloads that expose a ``value`` field."""

    if po_obj is None:
        return None

    val = None
    if isinstance(po_obj, dict):
        val = po_obj.get("value")
    else:
        val = getattr(po_obj, "value", None)
        if val is None and hasattr(po_obj, "get"):
            try:
                val = po_obj.get("value")
            except Exception:
                pass

    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _normalize_player_payload(item):
    """Coerce Yahoo player payload wrappers to the raw dict Player expects."""

    if item is None:
        return None

    # Objects coming from yfpy responses sometimes expose a ``player`` attr.
    if hasattr(item, "player") and getattr(item, "player") is not None:
        item = getattr(item, "player")

    # Dict wrapper: {"player": {...}}
    if isinstance(item, dict) and "player" in item:
        item = item["player"]

    # Lists can look like ["player", {...}] or [{...}]
    if isinstance(item, list):
        if len(item) == 2 and isinstance(item[1], dict):
            item = item[1]
        elif item and isinstance(item[0], dict):
            item = item[0]

    return item


def _player_from_payload(item) -> Player | None:
    """Return a Player instance from a raw Yahoo payload item."""

    normalized = _normalize_player_payload(item)
    if normalized is None:
        return None

    if isinstance(normalized, Player):
        return normalized

    if hasattr(normalized, "as_dict"):
        try:
            normalized = normalized.as_dict()
        except Exception:
            return None

    try:
        return Player(normalized)
    except Exception:
        return None


def _call_with_retries(fn, *args, **kwargs):
    """Call Yahoo endpoints with basic retry/backoff to survive rate limiting."""

    delay = RATE_LIMIT_DELAY_SECONDS
    for attempt in range(1, RATE_LIMIT_MAX_RETRIES + 1):
        try:
            result = fn(*args, **kwargs)
        except HTTPError as err:
            if attempt == RATE_LIMIT_MAX_RETRIES:
                raise

            wait = delay * (RATE_LIMIT_BACKOFF ** (attempt - 1))
            print(f"[Yahoo] rate limited ({err}); retrying in {wait:.2f}s (attempt {attempt}/{RATE_LIMIT_MAX_RETRIES})")
            time.sleep(wait)
            continue

        if RATE_LIMIT_DELAY_SECONDS > 0:
            time.sleep(RATE_LIMIT_DELAY_SECONDS)
        return result

    return None


def _rows_exist_for_snapshot(client: bigquery.Client, snapshot_date: date) -> bool:
    """Return True when the ownership table already has rows for ``snapshot_date``."""

    sql = f"SELECT 1 FROM `{TABLE}` WHERE snapshot_date = @snapshot LIMIT 1"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot", "DATE", snapshot_date),
        ]
    )

    try:
        job = client.query(sql, job_config=job_config, location=BQ_LOCATION)
    except NotFound:
        return False

    for _ in job.result():
        return True
    return False

def _iter_player_pool(
    q: YahooFantasySportsQuery,
    statuses: list[str | None] | None = None,
    batch_size: int = 25,
    league_key: str | None = None,
):
    """Yield Player models across rostered + available pools for the league."""

    statuses = statuses or [None, "A", "FA", "W"]
    seen_keys: set[str] = set()
    league_key = league_key or q.get_league_key()

    for status in statuses:
        start = 0

        while True:
            clause_parts = []
            if status:
                clause_parts.append(f"status={status}")
            clause_parts.append(f"start={start}")
            clause_parts.append(f"count={batch_size}")
            url = (
                f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;"
                f"{';'.join(clause_parts)}"
            )

            try:
                payload = _call_with_retries(q.query, url, ["league", "players"], None)
            except YahooFantasySportsDataNotFound:
                break

            if not payload:
                break

            players = payload if isinstance(payload, list) else [payload]
            batch_count = len(players)

            for raw_player in players:
                player = _player_from_payload(raw_player)
                if player is None:
                    continue
                pkey = getattr(player, "player_key", None)
                if pkey and pkey in seen_keys:
                    continue
                if pkey:
                    seen_keys.add(pkey)
                yield player

            if batch_count < batch_size:
                break

            start += batch_count


def _percent_owned_batch_values(
    q: YahooFantasySportsQuery,
    league_key: str,
    player_keys: list[str],
    week: int | str = "current",
) -> dict[str, float | None]:
    if not player_keys:
        return {}

    joined_keys = ",".join(player_keys)
    url = (
        f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;"
        f"player_keys={joined_keys}/percent_owned;type=week;week={week}"
    )

    try:
        payload = _call_with_retries(q.query, url, ["league", "players"], None)
    except YahooFantasySportsDataNotFound:
        return {}

    if not payload:
        return {}

    players = payload if isinstance(payload, list) else [payload]
    results: dict[str, float | None] = {}
    for raw_player in players:
        player = _player_from_payload(raw_player)
        if player is None:
            continue
        key = getattr(player, "player_key", None)
        if not key:
            continue
        results[key] = _extract_percent_value(getattr(player, "percent_owned", None))

    return results


def fetch_yahoo_roster_df(
    q: YahooFantasySportsQuery,
    *,
    week: int | str = "current",
) -> pd.DataFrame:
    """Pull Yahoo player pool and source roster_pct directly from percent_owned.value."""

    league_key = q.get_league_key()
    players = list(_iter_player_pool(q, league_key=league_key))
    total = len(players)
    print(f"[Yahoo] player pool fetched: {total} players")

    keys = [getattr(p, "player_key", None) for p in players if getattr(p, "player_key", None)]
    unique_keys: list[str] = []
    seen_keys: set[str] = set()
    for key in keys:
        if key not in seen_keys:
            seen_keys.add(key)
            unique_keys.append(key)

    percent_owned_map: dict[str, float | None] = {}
    for start in range(0, len(unique_keys), PERCENT_OWNED_BATCH_SIZE):
        batch = unique_keys[start : start + PERCENT_OWNED_BATCH_SIZE]
        batch_map = _percent_owned_batch_values(q, league_key, batch, week=week)
        percent_owned_map.update(batch_map)
        if len(unique_keys) and ((start + len(batch)) % 100 == 0 or (start + len(batch)) == len(unique_keys)):
            processed = start + len(batch)
            pct = round(processed * 100 / len(unique_keys), 1)
            print(f"[Yahoo] percent-owned fetched for {processed}/{len(unique_keys)} keys ({pct}%)")

    rows = []
    for i, p in enumerate(players, 1):
        if total and (i % 100 == 0 or i == total):
            print(f"[Yahoo] processed {i}/{total} ({round(i * 100 / total, 1)}%)")

        key = getattr(p, "player_key", None)
        name = (getattr(p, "full_name", None) or _name_of(p) or "").strip()
        if not name:
            continue

        val = percent_owned_map.get(key) if key else None

        rows.append({"player_key": key, "player_name": name, "roster_pct": val})

    df = pd.DataFrame(rows).dropna(subset=["player_name"])
    if "roster_pct" in df.columns:
        df["roster_pct"] = pd.to_numeric(df["roster_pct"], errors="coerce")
    if df.empty:
        return df

    if "player_key" in df.columns and df["player_key"].notna().any():
        df = (
            df.sort_values(["player_key", "roster_pct"], ascending=[True, False])
            .drop_duplicates(subset=["player_key"], keep="first")
        )

    df["name_lc"] = df["player_name"].str.strip().str.lower()
    df = (
        df.sort_values(["name_lc", "roster_pct"], ascending=[True, False])
        .drop_duplicates(subset=["name_lc"], keep="first")
    )
    return df

def run(
    snapshot: date | None = None,
    *,
    week: int | str | None = None,
    force: bool = False,
) -> pd.DataFrame:
    snapshot = snapshot or date.today()
    client = _bq()
    q = _yahoo_query()

    if not force and _rows_exist_for_snapshot(client, snapshot):
        print(f"[BQ] {TABLE} already has rows for {snapshot}; skipping ingestion.")
        return pd.DataFrame()

    week_value: int | str = week if week is not None else "current"

    yahoo_df = fetch_yahoo_roster_df(q, week=week_value)
    if yahoo_df.empty:
        print("[Yahoo] no rows; aborting")
        return yahoo_df

    dim = _player_dim_map(client)
    merged = yahoo_df.merge(dim, on="name_lc", how="left", suffixes=("_yahoo", "_dim")).drop(columns=["name_lc"])

    # unify player_name
    if "player_name_yahoo" in merged.columns and "player_name_dim" in merged.columns:
        merged["player_name"] = merged["player_name_yahoo"].fillna(merged["player_name_dim"])
        merged = merged.drop(columns=["player_name_yahoo", "player_name_dim"])
    elif "player_name_yahoo" in merged.columns:
        merged = merged.rename(columns={"player_name_yahoo": "player_name"})
    elif "player_name_dim" in merged.columns:
        merged = merged.rename(columns={"player_name_dim": "player_name"})
    for col in ["player_name", "roster_pct", "player_id"]:
        if col not in merged.columns:
            merged[col] = None

    merged.insert(0, "snapshot_date", pd.to_datetime(snapshot).date())

    load_df = merged[["player_id", "player_name", "snapshot_date", "roster_pct"]]
    print(f"[BQ] loading {len(load_df)} rows to {TABLE} …")
    job = client.load_table_from_dataframe(
        load_df,
        TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        location=BQ_LOCATION,
    )
    job.result()
    print(f"[BQ] done. {len(load_df)} rows → {TABLE} ({snapshot}, week={week_value})")
    return merged

print("[AUTH] project:", google.auth.default()[1])

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Yahoo percent-owned data into BigQuery.")
    parser.add_argument(
        "--snapshot-date",
        dest="snapshot_date",
        help="Snapshot date (YYYY-MM-DD). Defaults to today when omitted.",
    )
    parser.add_argument(
        "--week",
        dest="week",
        help=(
            "Yahoo percent-owned week identifier (int week number or 'current')."
            " Defaults to 'current'."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Load data even if rows already exist for the snapshot date.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    snapshot_value: date | None = None
    if args.snapshot_date:
        try:
            snapshot_value = date.fromisoformat(args.snapshot_date)
        except ValueError as exc:
            raise SystemExit(f"Invalid --snapshot-date value: {args.snapshot_date}") from exc

    week_arg: int | str | None = None
    if args.week:
        week_arg = args.week
        try:
            week_arg = int(args.week)
        except ValueError:
            week_arg = args.week

    run(snapshot=snapshot_value, week=week_arg, force=args.force)
