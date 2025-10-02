# daily_ingest.py
import os, time, numpy as np, pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from nba_api.stats.endpoints import LeagueGameLog, BoxScoreTraditionalV2
from requests.exceptions import ReadTimeout

# ----------------------------
# Baseline stats (update each season)
# ----------------------------
WEIGHTED_MEAN = [11.69, 4.32, 2.76, 0.75, 0.50, 1.28, 0.47, 0.75, 1.33]
WEIGHTED_STD  = [7.23,  2.51, 2.09, 0.38, 0.45, 0.95, 0.082, 0.124, 0.85]

# ----------------------------
# Helpers
# ----------------------------
def mmddyyyy(dt): return dt.strftime("%m/%d/%Y")

def get_game_ids_for_date(target_date, season="2024-25"):
    log = LeagueGameLog(
        season=season,
        season_type_all_star="Regular Season",
        player_or_team_abbreviation="T",
        date_from_nullable=mmddyyyy(target_date),
        date_to_nullable=mmddyyyy(target_date),
        timeout=15
    )
    df = log.get_data_frames()[0]
    return df["GAME_ID"].drop_duplicates().tolist()

def fetch_boxscore(game_id, retries=3, timeout=15):
    for attempt in range(retries):
        try:
            box = BoxScoreTraditionalV2(game_id=game_id, timeout=timeout)
            return box.get_data_frames()[0]
        except ReadTimeout:
            time.sleep(2 * (attempt + 1))
    return pd.DataFrame()

def compute_zscores(box):
    box = box.copy()
    box["MIN"] = box["MIN"].apply(
        lambda x: int(float(x.split(":")[0])) if pd.notnull(x) and isinstance(x, str) else None
    )
    nine = box[['PLAYER_NAME','PTS','REB','AST','STL','BLK','FG3M','FG_PCT','FT_PCT','TO']].fillna(0)

    z_list = []
    for i in range(len(nine)):
        diff = np.subtract(nine.iloc[i].tolist()[1:], WEIGHTED_MEAN)
        z = np.divide(diff, WEIGHTED_STD)
        fga = box["FGA"].iloc[i] if pd.notnull(box["FGA"].iloc[i]) else 0
        fta = box["FTA"].iloc[i] if pd.notnull(box["FTA"].iloc[i]) else 0
        adj = np.multiply(z, [1,1,1,1,1,1,(fga/20.0),(fta/8.0),-1])
        z_list.append(round(float(np.sum(adj)), 3))
    box["Z_SCORE"] = z_list
    return box

def run_ingestion(target_date=None, season="2024-25"):
    if target_date is None:
        target_date = datetime.today() - timedelta(days=1)

    game_ids = get_game_ids_for_date(target_date, season=season)
    if not game_ids:
        print(f"No games on {target_date.date()}")
        return pd.DataFrame()

    frames = []
    for gid in game_ids:
        df = fetch_boxscore(gid)
        if df.empty:
            continue
        df = df[['GAME_ID','PLAYER_ID','PLAYER_NAME','MIN','FGM','FGA','FG_PCT',
                 'FG3M','FG3A','FG3_PCT','FTM','FTA','FT_PCT','OREB','DREB','REB',
                 'AST','STL','BLK','TO','PF','PTS']].copy()
        frames.append(df)
        time.sleep(0.4)
    if not frames:
        return pd.DataFrame()

    all_df = pd.concat(frames, ignore_index=True)
    all_df = compute_zscores(all_df)
    all_df["game_date"] = target_date.date()
    return all_df

if __name__ == "__main__":
    target_date = datetime.today() - timedelta(days=1)
    df = run_ingestion(target_date)

    if df.empty:
        print("No rows to load.")
    else:
        client = bigquery.Client()
        table = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"
        job = client.load_table_from_dataframe(
            df, table,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        )
        job.result()
        print(f"Loaded {len(df)} rows into {table} for {target_date.date()}")
