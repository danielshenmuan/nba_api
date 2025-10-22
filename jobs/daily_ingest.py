# daily_ingest.py
import time
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd
from google.cloud import bigquery
from nba_api.stats.endpoints import LeagueGameLog, BoxScoreTraditionalV2
from requests.exceptions import ReadTimeout

# ----------------------------
# Baseline stats (update each season if needed)
# ----------------------------
WEIGHTED_MEAN = [11.69, 4.32, 2.76, 0.75, 0.50, 1.28, 0.47, 0.75, 1.33]
WEIGHTED_STD  = [7.23,  2.51, 2.09, 0.38, 0.45, 0.95, 0.082, 0.124, 0.85]

# ----------------------------
# Helpers
# ----------------------------
def mmddyyyy(dt: datetime) -> str:
    return dt.strftime("%m/%d/%Y")

def _season_from_date(d: datetime.date) -> str:
    y = d.year
    return f"{y}-{(y+1)%100:02d}" if d.month >= 10 else f"{y-1}-{y%100:02d}"

def get_game_ids_for_date(target_date: datetime, season: str = "2024-25") -> list[str]:
    log = LeagueGameLog(
        season=season,
        season_type_all_star="Regular Season",
        player_or_team_abbreviation="T",  # team logs -> one row per game
        date_from_nullable=mmddyyyy(target_date),
        date_to_nullable=mmddyyyy(target_date),
        timeout=15,
    )
    df = log.get_data_frames()[0]
    return df["GAME_ID"].drop_duplicates().astype(str).tolist()

def fetch_boxscore(game_id: str, retries: int = 3, timeout: int = 15) -> pd.DataFrame:
    for attempt in range(retries):
        try:
            box = BoxScoreTraditionalV2(game_id=game_id, timeout=timeout)
            return box.get_data_frames()[0]
        except ReadTimeout:
            time.sleep(2 * (attempt + 1))
    return pd.DataFrame()

def compute_zscores(box: pd.DataFrame) -> pd.DataFrame:
    box = box.copy()

    # Minutes -> integer minutes (strip mm:ss)
    def _min_to_int(x):
        if pd.isna(x):
            return None
        if isinstance(x, str) and ":" in x:
            try:
                return int(float(x.split(":")[0]))
            except ValueError:
                return None
        if isinstance(x, (int, float)):
            return int(x)
        return None

    box["MIN_INT"] = box["MIN"].apply(_min_to_int)

    nine = box[["PLAYER_NAME", "PTS", "REB", "AST", "STL", "BLK", "FG3M", "FG_PCT", "FT_PCT", "TO"]].fillna(0)

    z_list: list[float] = []
    for i in range(len(nine)):
        vals = nine.iloc[i].tolist()[1:]
        diff = np.subtract(vals, WEIGHTED_MEAN)
        z = np.divide(diff, WEIGHTED_STD)
        fga = box["FGA"].iloc[i] if pd.notnull(box["FGA"].iloc[i]) else 0
        fta = box["FTA"].iloc[i] if pd.notnull(box["FTA"].iloc[i]) else 0
        adj = np.multiply(z, [1, 1, 1, 1, 1, 1, (fga / 20.0), (fta / 8.0), -1])
        z_list.append(round(float(np.sum(adj)), 3))

    box["Z_SCORE"] = z_list
    return box

def run_ingestion(target_date: datetime | None = None, season: str = "2024-25") -> pd.DataFrame:
    if target_date is None:
        target_date = datetime.today() - timedelta(days=1)

    game_ids = get_game_ids_for_date(target_date, season=season)
    if not game_ids:
        print(f"No games on {target_date.date()}")
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for gid in game_ids:
        df = fetch_boxscore(gid)
        if df.empty:
            continue
        # Keep only the columns we need; BoxScoreTraditionalV2 provides these names
        cols = [
            "GAME_ID", "PLAYER_ID", "PLAYER_NAME", "TEAM_ABBREVIATION", "MIN",
            "FGM", "FGA", "FG_PCT",
            "FG3M", "FG3A", "FG3_PCT",
            "FTM", "FTA", "FT_PCT",
            "OREB", "DREB", "REB",
            "AST", "STL", "BLK", "TO", "PF", "PTS",
        ]
        df = df[cols].copy()
        df["GAME_ID"] = df["GAME_ID"].astype(str)
        frames.append(df)
        time.sleep(0.4)  # be polite to the stats API

    if not frames:
        return pd.DataFrame()

    all_df = pd.concat(frames, ignore_index=True)
    all_df = compute_zscores(all_df)

    # Build final frame matching BigQuery table schema
    all_df["game_date"] = target_date.date()
    all_df["season"] = all_df["game_date"].apply(_season_from_date)

    out = pd.DataFrame({
        "game_date": all_df["game_date"],
        "game_id": all_df["GAME_ID"].astype(str),
        "player_id": all_df["PLAYER_ID"].astype("Int64"),
        "player_name": all_df["PLAYER_NAME"].astype(str),
        "team_abbr": all_df["TEAM_ABBREVIATION"].astype(str),
        "minutes": all_df["MIN_INT"].astype("Int64"),
        "pts": all_df["PTS"].astype(float),
        "reb": all_df["REB"].astype(float),
        "ast": all_df["AST"].astype(float),
        "stl": all_df["STL"].astype(float),
        "blk": all_df["BLK"].astype(float),
        "fg3m": all_df["FG3M"].astype(float),
        "fg_pct": all_df["FG_PCT"].astype(float),
        "ft_pct": all_df["FT_PCT"].astype(float),
        "turnovers": all_df["TO"].astype(float),
        "z_score": all_df["Z_SCORE"].astype(float),
    })

    # Drop DNP rows (no minutes parsed)
    out = out[out["minutes"].notna()].reset_index(drop=True)
    return out

from pathlib import Path
from google.cloud import bigquery

def refresh_league_pg_stats():
    client = bigquery.Client(project="fantasy-survivor-app")
    sql_path = Path(__file__).resolve().parents[1] / "infra" / "bq" / "sql" / "create_league_pg_stats_by_season.sql"
    job = client.query(sql_path.read_text(), location="northamerica-northeast1")
    job.result()
    print("Refreshed league_pg_stats_by_season ✅")

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    target_date = datetime.today() - timedelta(days=1)
    df = run_ingestion(target_date)

    if df.empty:
        print("No rows to load.")
    else:
        client = bigquery.Client(project="fantasy-survivor-app")
        table = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"

        # Load (df columns must match table schema)
        job = client.load_table_from_dataframe(
            df, table,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        )
        job.result()
        print(f"Loaded {len(df)} rows into {table} for {target_date.date()}")

        # Update precomputed league stats
        refresh_league_pg_stats()
