# jobs/ownership_ingest.py
import os
from pathlib import Path
from datetime import date
import pandas as pd
from google.cloud import bigquery
from dotenv import dotenv_values
from yfpy.exceptions import YahooFantasySportsDataNotFound
from yfpy.query import YahooFantasySportsQuery
import google.auth

PROJECT = "fantasy-survivor-app"
DATASET = "nba_data"
TABLE   = f"{PROJECT}.{DATASET}.player_ownership"
BQ_LOCATION = "northamerica-northeast1"  # set to your dataset location

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

def _iter_player_pool(
    q: YahooFantasySportsQuery,
    statuses: list[str | None] | None = None,
    batch_size: int = 25,
):
    """Yield Player models across rostered + available pools for the league."""

    statuses = statuses or [None, "A", "FA", "W"]
    seen_keys: set[str] = set()
    league_key = q.get_league_key()

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
                payload = q.query(url, ["league", "players"])
            except YahooFantasySportsDataNotFound:
                break

            if not payload:
                break

            players = payload if isinstance(payload, list) else [payload]
            batch_count = len(players)

            for player in players:
                pkey = getattr(player, "player_key", None)
                if pkey and pkey in seen_keys:
                    continue
                if pkey:
                    seen_keys.add(pkey)
                yield player

            if batch_count < batch_size:
                break

            start += batch_count


def _percent_owned_value(q: YahooFantasySportsQuery, player_key: str) -> float | None:
    try:
        res = q.get_player_percent_owned_by_week(player_key, "current")
    except YahooFantasySportsDataNotFound:
        return None

    po = None
    if hasattr(res, "as_dict"):
        try:
            po = res.as_dict().get("percent_owned")
        except Exception:
            po = None
    if po is None:
        po = getattr(res, "percent_owned", None)

    return _extract_percent_value(po)


def fetch_yahoo_roster_df(q: YahooFantasySportsQuery) -> pd.DataFrame:
    """Pull Yahoo player pool and source roster_pct directly from percent_owned.value."""

    players = list(_iter_player_pool(q))
    total = len(players)
    print(f"[Yahoo] player pool fetched: {total} players")

    rows = []
    for i, p in enumerate(players, 1):
        if total and (i % 100 == 0 or i == total):
            print(f"[Yahoo] processed {i}/{total} ({round(i * 100 / total, 1)}%)")

        key = getattr(p, "player_key", None)
        name = (getattr(p, "full_name", None) or _name_of(p) or "").strip()
        if not name:
            continue

        val = _percent_owned_value(q, key) if key else None

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

def run(snapshot: date | None = None) -> pd.DataFrame:
    snapshot = snapshot or date.today()
    client = _bq()
    q = _yahoo_query()

    yahoo_df = fetch_yahoo_roster_df(q)
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
    print(f"[BQ] done. {len(load_df)} rows → {TABLE} ({snapshot})")
    return merged

print("[AUTH] project:", google.auth.default()[1])

if __name__ == "__main__":
    run()
