from google.cloud import bigquery
import os
import pandas as pd
import numpy as np

PROJECT_ID = os.getenv("PROJECT_ID", "fantasy-survivor-app")
DATASET = "nba_data"
TABLE = "player_daily_game_stats_p"

def get_client():
    return bigquery.Client(project=PROJECT_ID)

def safe_records(df: pd.DataFrame):
    """
    Convert NaN/Inf values into None so JSON serialization won't fail.
    """
    df = df.replace([np.inf, -np.inf], np.nan)       # replace +/- inf with NaN
    return df.where(df.notnull(), None).to_dict("records")  # NaN -> None

def get_daily_leaders(date, limit=10, mode="best", min_minutes: float = 0):
    client = get_client()

    order = "DESC" if mode == "best" else "ASC"

    query = f"""
    SELECT
      player_id,
      player_name,
      game_id,
      game_date,
      team_id,
      team_abbr,
      team_city,
      team_name,
      team_slug,
      position,
      jersey_num,
      minutes,
      fgm, fga, fg_pct,
      fg3m, fg3a, fg3_pct,
      ftm, fta, ft_pct,
      pts, reb, dreb, oreb, ast, stl, blk,
      turnovers, pf, plus_minus,
      z_score
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE game_date = @date
      AND minutes >= @min_minutes
    ORDER BY z_score {order}
    LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date", "DATE", date),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("min_minutes", "FLOAT64", float(min_minutes)),
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    return safe_records(df)

def get_player_time_series(player_id, start_date=None, end_date=None):
    client = get_client()
    conditions = ["player_id = @pid"]
    params = [bigquery.ScalarQueryParameter("pid", "INT64", player_id)]

    if start_date:
        conditions.append("game_date >= @start")
        params.append(bigquery.ScalarQueryParameter("start", "DATE", start_date))
    if end_date:
        conditions.append("game_date <= @end")
        params.append(bigquery.ScalarQueryParameter("end", "DATE", end_date))

    where_clause = " AND ".join(conditions)

    query = f"""
    SELECT
      game_date,
      game_id,
      team_id,
      team_abbr,
      team_name,
      team_slug,
      position,
      jersey_num,
      minutes,
      fgm,
      fga,
      fg_pct,
      fg3m,
      fg3a,
      fg3_pct,
      ftm,
      fta,
      ft_pct,
      pts,
      reb,
      dreb,
      oreb,
      ast,
      stl,
      blk,
      turnovers,
      pf,
      plus_minus,
      z_score
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {where_clause}
    ORDER BY game_date ASC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = client.query(query, job_config=job_config).to_dataframe()
    return safe_records(df)
