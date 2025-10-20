# jobs/ownership_ingest.py
import os
from pathlib import Path
from datetime import date
import pandas as pd
from google.cloud import bigquery
from dotenv import dotenv_values
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
REQ = ["YAHOO_CONSUMER_KEY","YAHOO_CONSUMER_SECRET",
       "YAHOO_ACCESS_TOKEN","YAHOO_ACCESS_TOKEN_SECRET","YAHOO_LEAGUE_ID"]
missing = [k for k in REQ if not os.getenv(k)]
if missing:
    raise SystemExit(f"Missing required env vars: {missing}")

# Writable token file for YFPY in Cloud Run
TMP_ENV = Path("/tmp/yahoo_tokens.env")
TMP_ENV.write_text("\n".join([
    f"YAHOO_CONSUMER_KEY={os.environ['YAHOO_CONSUMER_KEY']}",
    f"YAHOO_CONSUMER_SECRET={os.environ['YAHOO_CONSUMER_SECRET']}",
    f"YAHOO_ACCESS_TOKEN={os.environ['YAHOO_ACCESS_TOKEN']}",
    f"YAHOO_ACCESS_TOKEN_SECRET={os.environ['YAHOO_ACCESS_TOKEN_SECRET']}",
    f"YAHOO_LEAGUE_ID={os.environ['YAHOO_LEAGUE_ID']}",
    f"YAHOO_GAME_ID={os.getenv('YAHOO_GAME_ID','')}",
]))

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

def _pct_value(po) -> float | None:
    # Accept YFPY PercentOwned or wrapped forms; return float 0..100
    if po is None:
        return None
    # PercentOwned directly
    if hasattr(po, "value"):
        try:
            return float(po.value)
        except (TypeError, ValueError):
            return None
    # PlayerOwnership model with .percent_owned
    if hasattr(po, "percent_owned") and hasattr(po.percent_owned, "value"):
        try:
            return float(po.percent_owned.value)
        except (TypeError, ValueError):
            return None
    # plain number/string fallback
    if isinstance(po, (int, float)):
        return float(po)
    if isinstance(po, str):
        try:
            return float(po.strip().replace("%", ""))
        except ValueError:
            return None
    return None

def fetch_yahoo_roster_df(q: YahooFantasySportsQuery) -> pd.DataFrame:
    """
    Preferred: per-player universal PercentOwned via get_player_percent_owned_by_week(..., 'current').
    Fallback: league payload if needed.
    """
    players = q.get_league_players()
    total = len(players)
    print(f"[Yahoo] league roster fetched: {total} players")

    rows = []
    for i, p in enumerate(players, 1):
        if i % 100 == 0 or i == total:
            print(f"[Yahoo] processed {i}/{total} ({round(i*100/total,1)}%)")

        name = _name_of(p)
        if not name:
            continue

        val = None
        # 1) universal percent-owned (weekly coverage, 'current')
        try:
            ply = q.get_player_percent_owned_by_week(p.player_key, "current")  # returns Player model
            po = getattr(ply, "percent_owned", None)
            val = _pct_value(po)
        except Exception as e:
            # keep val None and try fallbacks
            pass

        # 2) fallback to league payload if universal missing
        if val is None:
            league_po = getattr(p, "percent_owned", None) or getattr(getattr(p, "ownership", None), "percent_owned", None)
            val = _pct_value(league_po)

        rows.append({"player_name": name, "roster_pct": val})

    df = pd.DataFrame(rows, columns=["player_name", "roster_pct"]).dropna(subset=["player_name"])
    if not df.empty:
        df["name_lc"] = df["player_name"].str.strip().str.lower()
        df = (df.sort_values(["name_lc", "roster_pct"], ascending=[True, False])
                .drop_duplicates(subset=["name_lc"], keep="first"))
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
print("[BQ  ] client.project:", client.project, "client.location:", client.location)

if __name__ == "__main__":
    run()
