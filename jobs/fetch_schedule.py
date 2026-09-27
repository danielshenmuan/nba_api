"""Fetch NBA schedule from SportsBlaze and load into BigQuery."""
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from google.cloud import bigquery

# Project Configuration
PROJECT_ID = "fantasy-survivor-app"
DATASET_ID = "nba_data"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.nba_schedule"
BQ_LOCATION = "northamerica-northeast1"

# API Configuration
SPORTSBLAZE_API_KEY = os.getenv("SPORTSBLAZE_API_KEY")
BASE_URL = "https://api.sportsblaze.com/nba/v1/schedule"
DELAY_BETWEEN_REQUESTS = 2.1 # 30 req/min = 2s delay

def get_current_season():
    """Determine current season year (e.g., 2024 for 2024-25)."""
    now = datetime.now()
    if now.month >= 10:
        return now.year
    return now.year - 1

def fetch_games(start_date: date, end_date: date) -> pd.DataFrame:
    """Fetch games from SportsBlaze API and filter by date range."""
    # Try to load .env if key is missing
    if not SPORTSBLAZE_API_KEY:
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
            
    api_key = os.getenv("SPORTSBLAZE_API_KEY")
    if not api_key:
        print("Error: SPORTSBLAZE_API_KEY not found in environment.")
        return pd.DataFrame()
    
    season_year = get_current_season()
    params = {"key": api_key}
    
    all_games_data = []
    
    # Try current and previous season as fallback
    for year in [season_year, season_year - 1]:
        url = f"{BASE_URL}/season/{year}.json"
        print(f"Fetching season {year} schedule from SportsBlaze...")
        try:
            response = requests.get(url, params=params, timeout=20)
            if response.status_code == 403:
                print(f"Access forbidden for season {year} (403).")
                continue
            response.raise_for_status()
            data = response.json()
            all_games_data.extend(data.get("games", []))
            break # Success
        except requests.RequestException as e:
            print(f"Error fetching season {year} from SportsBlaze: {e}")
            continue
            
    # Fallback to daily endpoints if no season data found
    if not all_games_data:
        print("Falling back to daily schedule endpoints...")
        current_date = start_date
        while current_date <= end_date:
            url = f"{BASE_URL}/daily/{current_date.isoformat()}.json"
            print(f"Fetching daily schedule for {current_date}...")
            try:
                response = requests.get(url, params=params, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    all_games_data.extend(data.get("games", []))
                else:
                    print(f"Failed to fetch {current_date}: {response.status_code}")
            except requests.RequestException as e:
                print(f"Error fetching daily {current_date}: {e}")
            
            current_date += timedelta(days=1)
            time.sleep(DELAY_BETWEEN_REQUESTS)

    if not all_games_data:
        print("No schedule data found after all attempts.")
        return pd.DataFrame()
        
    all_games = []
    print(f"Processing {len(all_games_data)} games...")
    
    for g in all_games_data:
        # SportsBlaze date format: "2025-04-11T22:30:00Z"
        game_dt_str = g.get("date")
        if not game_dt_str:
            continue
            
        # Parse ISO format and handle UTC
        try:
            game_dt = datetime.fromisoformat(game_dt_str.replace("Z", "+00:00")).date()
        except ValueError:
            print(f"Skipping game with invalid date: {game_dt_str}")
            continue
        
        if start_date <= game_dt <= end_date:
            all_games.append({
                "game_id": g["id"],
                "game_date": game_dt.isoformat(),
                "home_team_id": g["teams"]["home"]["id"],
                "away_team_id": g["teams"]["away"]["id"],
                "home_team_name": g["teams"]["home"]["name"],
                "away_team_name": g["teams"]["away"]["name"],
            })
            
    return pd.DataFrame(all_games)

def upsert_to_bigquery(df: pd.DataFrame):
    """Upsert data into BigQuery to ensure idempotency."""
    if df.empty:
        print("No games in the requested window to load.")
        return

    client = bigquery.Client(project=PROJECT_ID)
    
    # 1. Load data into a temporary/staging table
    staging_table_id = f"{TABLE_ID}_staging"
    
    # Ensure IDs are strings and game_date is DATE type for BQ
    df["game_id"] = df["game_id"].astype(str)
    df["home_team_id"] = df["home_team_id"].astype(str)
    df["away_team_id"] = df["away_team_id"].astype(str)
    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date
    df["updated_at"] = datetime.now(timezone.utc)
    
    # Use staging table
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into staging table {staging_table_id}.")
    
    # 2. Perform MERGE (Upsert)
    merge_sql = f"""
    MERGE `{TABLE_ID}` T
    USING `{staging_table_id}` S
    ON T.game_id = S.game_id
    WHEN MATCHED THEN
      UPDATE SET
        game_date = S.game_date,
        home_team_id = S.home_team_id,
        away_team_id = S.away_team_id,
        home_team_name = S.home_team_name,
        away_team_name = S.away_team_name,
        updated_at = S.updated_at
    WHEN NOT MATCHED THEN
      INSERT (game_id, game_date, home_team_id, away_team_id, home_team_name, away_team_name, updated_at)
      VALUES (S.game_id, S.game_date, S.home_team_id, S.away_team_id, S.home_team_name, S.away_team_name, S.updated_at)
    """
    
    query_job = client.query(merge_sql, location=BQ_LOCATION)
    query_job.result()
    print(f"Upserted {len(df)} games into {TABLE_ID}.")
    
    # Clean up staging table
    client.delete_table(staging_table_id, not_found_ok=True)

if __name__ == "__main__":
    # Fetch for next 14 days (including today)
    today = date.today()
    end_date = today + timedelta(days=14)
    
    df = fetch_games(today, end_date)
    upsert_to_bigquery(df)
