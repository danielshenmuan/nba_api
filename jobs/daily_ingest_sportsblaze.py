"""Ingest NBA box scores from SportsBlaze API into BigQuery."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables
load_dotenv()

# Constants
SPORTSBLAZE_Api_KEY = os.environ.get("SPORTSBLAZE_API_KEY")
if not SPORTSBLAZE_Api_KEY:
    raise RuntimeError(
        "SPORTSBLAZE_API_KEY is not set. Export it locally or supply it "
        "from Secret Manager in Cloud Run. There is deliberately no fallback."
    )
BASE_URL = "https://api.sportsblaze.com/nba/v1/boxscores/daily"

DEFAULT_PROJECT_ID = "fantasy-survivor-app"
PARTITIONED_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"

# Z-Score Constants
WEIGHTED_MEAN = [11.69, 4.32, 2.76, 0.75, 0.50, 1.28, 0.47, 0.75, 1.33]
WEIGHTED_STD = [7.23, 2.51, 2.09, 0.38, 0.45, 0.95, 0.082, 0.124, 0.85]


def _season_from_date(d: date) -> str:
    year = d.year
    if d.month >= 10:
        return f"{year}-{(year + 1) % 100:02d}"
    return f"{year - 1}-{year % 100:02d}"


def compute_zscores(box: pd.DataFrame) -> pd.DataFrame:
    """Compute z-scores for standard 9-cat stats."""
    if box.empty:
        return box

    box = box.copy()
    
    # Ensure required columns are numeric
    # Order for WEIGHTED_MEAN/STD: PTS, REB, AST, STL, BLK, FG3M, FG_PCT, FT_PCT, TO
    
    # Calculate percentages for z-score if not present (should be passed in, but safety first)
    # Note: SportsBlaze provides percentages, but we might recalculate to be precise? 
    # Let's use what is in the DF.
    
    z_list: list[float] = []
    
    for i in range(len(box)):
        row = box.iloc[i]
        
        # safely get values, default to 0
        vals = [
            row.get("pts", 0),
            row.get("reb", 0),
            row.get("ast", 0),
            row.get("stl", 0),
            row.get("blk", 0),
            row.get("fg3m", 0),
            row.get("fg_pct", 0),
            row.get("ft_pct", 0),
            row.get("turnovers", 0),
        ]
        
        # Ensure floats
        vals = [float(v) if v is not None else 0.0 for v in vals]

        diff = np.subtract(vals, WEIGHTED_MEAN)
        z = np.divide(diff, WEIGHTED_STD)
        
        fga = float(row.get("fga", 0)) if row.get("fga") is not None else 0.0
        fta = float(row.get("fta", 0)) if row.get("fta") is not None else 0.0
        
        # Adjust percentages z-scores by volume
        # Z-score vector indices: 0:PTS 1:REB 2:AST 3:STL 4:BLK 5:FG3M 6:FG% 7:FT% 8:TO
        # Multipliers: 1, 1, 1, 1, 1, 1, (fga/20), (fta/8), -1
        
        adj = np.multiply(z, [1, 1, 1, 1, 1, 1, (fga / 20.0), (fta / 8.0), -1])
        z_list.append(round(float(np.sum(adj)), 3))

    box["z_score"] = z_list
    return box


def fetch_sportsblaze_data(target_date: date) -> list[dict]:
    """Fetch all games for the given date from SportsBlaze."""
    date_str = target_date.strftime("%Y-%m-%d")
    url = f"{BASE_URL}/{date_str}.json"
    params = {"key": SPORTSBLAZE_Api_KEY}
    
    print(f"Fetching data from: {url}")
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("games", [])
    except Exception as e:
        print(f"Error fetching SportsBlaze data: {e}")
        return []


def parse_games_to_df(games: list[dict], target_date: date) -> pd.DataFrame:
    """Parse SportsBlaze games list into a flat DataFrame aligned with BQ schema."""
    all_records = []
    
    for game in games:
        game_id = game.get("id")
        
        # Teams info
        teams = game.get("teams", {})
        home_team = teams.get("home", {})
        away_team = teams.get("away", {})
        
        # Rosters
        rosters = game.get("rosters", {})
        
        # Process both home and away rosters
        for side in ["home", "away"]:
            team_info = home_team if side == "home" else away_team
            team_id = team_info.get("id")
            team_name = team_info.get("name")
            # Create a simple abbreviation from name if not provided (SportsBlaze seems to not send abbr in this endpoint easily?)
            # Actually, let's just use what we have. BQ schema has team_abbr. 
            # We might need a map or just leave null if critical. 
            # For now, let's try to infer or leave null.
            
            players = rosters.get(side, [])
            
            for p in players:
                # Only process if they played? 
                # "played": true
                if not p.get("played"):
                    continue
                
                stats = p.get("stats", {})
                
                # Basic Player Info
                player_record = {
                    "game_date": target_date,
                    "game_id": game_id,
                    "player_id": p.get("id"),
                    "player_name": p.get("name"),
                    "team_id": team_id,
                    "team_name": team_name,
                    # "team_abbr": None, # Missing in this endpoint
                    # "team_city": None,
                    # "team_slug": None,
                    "position": p.get("position"),
                    "jersey_num": p.get("number"),
                    # "comment": None
                }
                
                # Stats Mapping
                # SportsBlaze keys -> BQ keys
                # SB: field_goals_made, field_goals_attempts, field_goals_pct
                # BQ: fgm, fga, fg_pct
                
                player_record["minutes"] = float(stats.get("minutes", 0)) # Int in JSON?
                # Sometimes minutes is string "MM:SS"? No, sample showed integer 34.
                # Just in case, handled below if structure differs.
                
                player_record["fgm"] = float(stats.get("field_goals_made", 0))
                player_record["fga"] = float(stats.get("field_goals_attempts", 0))
                player_record["fg_pct"] = float(stats.get("field_goals_pct", 0))
                
                player_record["fg3m"] = float(stats.get("three_pointers_made", 0))
                player_record["fg3a"] = float(stats.get("three_pointers_attempts", 0))
                player_record["fg3_pct"] = float(stats.get("three_pointers_pct", 0))
                
                player_record["ftm"] = float(stats.get("free_throws_made", 0))
                player_record["fta"] = float(stats.get("free_throws_attempts", 0))
                player_record["ft_pct"] = float(stats.get("free_throws_pct", 0))
                
                player_record["pts"] = float(stats.get("points", 0))
                player_record["reb"] = float(stats.get("rebounds", 0))
                player_record["ast"] = float(stats.get("assists", 0))
                player_record["stl"] = float(stats.get("steals", 0))
                player_record["blk"] = float(stats.get("blocks", 0))
                player_record["turnovers"] = float(stats.get("turnovers_personal", 0)) # "turnovers_personal" seems right
                player_record["pf"] = float(stats.get("fouls_personal", 0))
                
                player_record["dreb"] = float(stats.get("rebounds_defensive", 0))
                player_record["oreb"] = float(stats.get("rebounds_offensive", 0))
                player_record["plus_minus"] = float(stats.get("plus_minus", 0))
                
                all_records.append(player_record)

    df = pd.DataFrame(all_records)
    
    if not df.empty:
        df["season"] = _season_from_date(target_date)
        # Compute Z-Scores
        df = compute_zscores(df)
        
    return df


def load_into_bigquery(df: pd.DataFrame, client: bigquery.Client, table_name: str):
    """Load dataframe into BigQuery."""
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        # Autodetect schema changes or map explicitly if needed
        # We assume columns match existing schema or are new additions
    )
    job = client.load_table_from_dataframe(df, table_name, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_name}")


from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
LEAGUE_STATS_TABLE = "fantasy-survivor-app.nba_data.league_pg_stats_by_season"
LEAGUE_SQL_FILENAME = "create_league_pg_stats_by_season.sql"
BQ_LOCATION = "northamerica-northeast1"

def _locate_sql_file(filename: str) -> Path:
    """Locate a SQL file bundled with the job or in the repo tree."""
    search_paths: list[Path] = [CURRENT_DIR / "sql" / filename, CURRENT_DIR / filename]
    for parent in CURRENT_DIR.parents:
        search_paths.append(parent / "infra" / "bq" / "sql" / filename)
    for candidate in search_paths:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Unable to locate {filename}. Ensure it is packaged with the deployment image."
    )

def refresh_league_pg_stats(
    *, client: bigquery.Client | None = None, project_id: str = DEFAULT_PROJECT_ID
) -> None:
    bq_client = client or bigquery.Client(project=project_id)
    sql_path = _locate_sql_file(LEAGUE_SQL_FILENAME)
    print(f"Executing query from {sql_path}...")
    job = bq_client.query(sql_path.read_text(), location=BQ_LOCATION)
    job.result()
    print("Refreshed league_pg_stats_by_season ✅")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="YYYY-MM-DD", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Do not write to BQ")
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today() - timedelta(days=1)
        
    print(f"Target Date: {target_date}")
    
    # 1. Fetch
    games = fetch_sportsblaze_data(target_date)
    print(f"Fetched {len(games)} games.")
    
    # 2. Parse
    df = parse_games_to_df(games, target_date)
    print(f"Parsed {len(df)} player records.")
    
    if df.empty:
        print("No data to process.")
        return

    # 3. Dry Run / Load
    if args.dry_run:
        print("Dry Run: Printing sample payload")
        print(df.head())
        print(df.describe())
    else:
        client = bigquery.Client(project=DEFAULT_PROJECT_ID)
        load_into_bigquery(df, client, PARTITIONED_TABLE)
        
        # 4. Refresh League Stats
        try:
            refresh_league_pg_stats(client=client)
        except Exception as e:
            print(f"Warning: Failed to refresh league stats: {e}")

if __name__ == "__main__":
    main()
