"""Ingest NBA box scores into player_daily_game_stats_p using yfpy."""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from yfpy.query import YahooFantasySportsQuery

# Load environment variables
load_dotenv()

# Universal constants
WEIGHTED_MEAN = [11.69, 4.32, 2.76, 0.75, 0.50, 1.28, 0.47, 0.75, 1.33]
WEIGHTED_STD = [7.23, 2.51, 2.09, 0.38, 0.45, 0.95, 0.082, 0.124, 0.85]

DEFAULT_PROJECT_ID = "fantasy-survivor-app"
PARTITIONED_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"
MIRROR_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats"
BQ_LOCATION = "northamerica-northeast1"

# Yahoo constants
YAHOO_LEAGUE_ID = os.environ.get("YAHOO_LEAGUE_ID")
YAHOO_GAME_ID = os.environ.get("YAHOO_GAME_ID")
GAME_CODE = "nba"


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
    stat_columns = [
        "PTS",
        "REB",
        "AST",
        "STL",
        "BLK",
        "FG3M",
        "FG_PCT",
        "FT_PCT",
        "TO",
    ]
    
    # Fill NAs with 0 for calculation
    nine = box[stat_columns].fillna(0)

    z_list: list[float] = []
    for i in range(len(nine)):
        vals = nine.iloc[i].tolist()[1:]  # Skip PLAYER_NAME if it was first, but here we just take columns
        # Actually in original code: values = nine.iloc[i].tolist()[1:] implied first col was skipped.
        # Let's be safer: get the specific values in order of WEIGHTED_MEAN
        
        # WEIGHTED_MEAN order from original code implies:
        # PTS, REB, AST, STL, BLK, FG3M, FG_PCT, FT_PCT, TO
        # Wait, let's verify original code.
        # Original: 
        # stat_columns = ["PLAYER_NAME", "PTS", "REB", "AST", "STL", "BLK", "FG3M", "FG_PCT", "FT_PCT", "TO"]
        # nine = box[stat_columns].fillna(0)
        # vals = nine.iloc[i].tolist()[1:] -> Skips PLAYER_NAME.
        
        # In my cleaned DF, I might not have PLAYER_NAME in the list first.
        # Let's construct `vals` explicitly to match WEIGHTED_MEAN order.
        # Order assumed: PTS, REB, AST, STL, BLK, FG3M, FG_PCT, FT_PCT, TO
        
        row = nine.iloc[i]
        vals = [
            row["PTS"],
            row["REB"],
            row["AST"],
            row["STL"],
            row["BLK"],
            row["FG3M"],
            row["FG_PCT"],
            row["FT_PCT"],
            row["TO"],
        ]

        diff = np.subtract(vals, WEIGHTED_MEAN)
        z = np.divide(diff, WEIGHTED_STD)
        
        fga = box["FGA"].iloc[i] if pd.notnull(box["FGA"].iloc[i]) else 0
        fta = box["FTA"].iloc[i] if pd.notnull(box["FTA"].iloc[i]) else 0
        
        # Adjust percentages z-scores by volume
        # Z-score vector indices: 0:PTS 1:REB 2:AST 3:STL 4:BLK 5:FG3M 6:FG% 7:FT% 8:TO
        # Multipliers: 1, 1, 1, 1, 1, 1, (fga/20), (fta/8), -1
        # Note: TO is negative impact
        
        adj = np.multiply(z, [1, 1, 1, 1, 1, 1, (fga / 20.0), (fta / 8.0), -1])
        z_list.append(round(float(np.sum(adj)), 3))

    box["Z_SCORE"] = z_list
    return box


def get_stat_map(query: YahooFantasySportsQuery) -> dict[str, str]:
    """Retrieve mapping of Stat ID to Stat Name (e.g. '12' -> 'PTS')."""
    print("Fetching game info for stat categories...")
    game_info = query.get_current_game_info()
    stat_map = {}
    
    if game_info and hasattr(game_info, 'stat_categories'):
        # stat_categories might be an object with a 'stats' attribute or a list
        stats_data = getattr(game_info.stat_categories, 'stats', game_info.stat_categories)
        
        for stat in stats_data:
            s_id = None
            s_name = None
            
            # Handle object vs dict
            if hasattr(stat, 'stat_id'):
                s_id = str(stat.stat_id)
                s_name = getattr(stat, 'name', getattr(stat, 'display_name', 'Unknown'))
            elif isinstance(stat, dict):
                # Nested {stat: {...}} structure sometimes returned by yfpy
                if 'stat' in stat:
                    inner = stat['stat']
                    s_id = str(inner.get('stat_id'))
                    s_name = inner.get('name', inner.get('display_name'))
                else:
                    s_id = str(stat.get('stat_id'))
                    s_name = stat.get('name', stat.get('display_name'))
            
            if s_id:
                stat_map[s_id] = s_name
                
    # Manual overrides/normalizations for common NBA stats if needed
    # Yahoo typically returns: 'Points', 'Rebounds', 'Assists', etc.
    # We map them to our schema keys.
    return stat_map


def map_stat_name_to_schema(yahoo_name: str) -> str | None:
    """Map Yahoo stat names to our internal schema column names."""
    name_lower = yahoo_name.lower().replace("3-pt", "3pt").strip()
    
    mapping = {
        "points": "PTS",
        "pts": "PTS",
        "rebounds": "REB",
        "reb": "REB",
        "assists": "AST",
        "ast": "AST",
        "steals": "STL",
        "stl": "STL",
        "blocks": "BLK",
        "blk": "BLK",
        "turnovers": "TO",
        "to": "TO",
        "field goals made": "FGM",
        "fgm": "FGM",
        "field goal attempts": "FGA",
        "fga": "FGA",
        "field goal percentage": "FG_PCT",
        "fg%": "FG_PCT",
        "fg_pct": "FG_PCT",
        "free throws made": "FTM",
        "ftm": "FTM",
        "free throw attempts": "FTA",
        "fta": "FTA",
        "free throw percentage": "FT_PCT",
        "ft%": "FT_PCT",
        "ft_pct": "FT_PCT",
        "3-point shots made": "FG3M",
        "3pt shots made": "FG3M",
        "3ptm": "FG3M",
        "fg3m": "FG3M",
        "3-point shot attempts": "FG3A",
        "3pt shot attempts": "FG3A",
        "3pta": "FG3A",
        "fg3a": "FG3A",
        "3-point percentage": "FG3_PCT",
        "3pt percentage": "FG3_PCT",
        "3pt%": "FG3_PCT",
        "fg3_pct": "FG3_PCT",
        "personal fouls": "PF",
        "pf": "PF",
        "minutes played": "MINUTES",
        "minutes": "MINUTES",
        "min": "MINUTES"
    }
    
    for k, v in mapping.items():
        if k in name_lower:
            return v
    return None


def batch_fetch_player_stats(
    query: YahooFantasySportsQuery,
    player_keys: list[str],
    date_str: str
) -> list[Any]:
    """Fetch stats for multiple players in one request."""
    keys_str = ",".join(player_keys)
    league_key = query.get_league_key()
    
    # Construct URL for bulk fetch
    # Note: Yahoo API structure for players collection
    url = (
        f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;"
        f"player_keys={keys_str}/stats;type=date;date={date_str}"
    )
    
    # We ask yfpy to return the 'players' dict.
    # Yahoo returns: {"league": {"players": {"0": {"player": ...}, "1": {...}, "count": N}}}
    try:
        response_json = query.query(url, ["league", "players"])
        
        # response_json should be a dict or list. 
        # yfpy's 'unpack_data' might have already transformed it?
        # Actually yfpy's query method unpacks based on YahooFantasyObject if we pass a class, 
        # but here we didn't pass a class, so it returns dict/list.
        
        # If it's a dict with numeric keys, iterate them
        players_data = []
        if isinstance(response_json, dict):
            for k, v in response_json.items():
                if k == "count":
                    continue
                if "player" in v:
                    # v["player"] is usually a list of [metadata, stats, etc] 
                    # or it's implicitly flattened by yfpy utils?
                    # Let's assume standard structure: v['player'] could be the object we want
                    # But yfpy might return it as a list of dicts.
                    # Safest is to use yfpy's `unpack_data` or `serialization` if we could, 
                    # but since we are raw, let's just inspect.
                    
                    # Actually, let's look at `query` implementation. 
                    # It calls `reformat_json_list`.
                    # If we just get "players", it returns the dict value of "players".
                    
                    p_entry = v["player"]
                    players_data.append(p_entry)
        elif isinstance(response_json, list):
             # sometimes it returns list
             players_data = response_json
             
        return players_data
        
    except Exception as e:
        print(f"Batch query failed: {e}")
        return []

def fetch_daily_stats(
    target_date: date, 
    query: YahooFantasySportsQuery,
    stat_map: dict[str, str],
    player_filter: str | None = None
) -> pd.DataFrame:
    """Fetch all player stats for the target date."""
    
    print(f"Fetching all league players to get keys...")
    # Get all players
    players = query.get_league_players(player_count_limit=2000)
    
    if not players:
        print("No players found in league.")
        return pd.DataFrame()

    if player_filter:
        print(f"Filtering players by name: '{player_filter}'")
        players = [p for p in players if player_filter.lower() in p.name.full.lower()]
        
    player_keys = [p.player_key for p in players]
    print(f"Found {len(player_keys)} players. Fetching stats (batch)...")
    
    date_str = target_date.strftime("%Y-%m-%d")
    all_player_data = []
    
    # Batch size
    CHUNK_SIZE = 25
    
    for i in range(0, len(player_keys), CHUNK_SIZE):
        chunk = player_keys[i:i+CHUNK_SIZE]
        print(f"Fetching batch {i}-{i+len(chunk)}...")
        
        # Use custom batch fetch
        raw_players = batch_fetch_player_stats(query, chunk, date_str)
        
        for p_raw in raw_players:
            # p_raw is likely a list of dicts (metadata, stats) or a dict
            # We need to parse it carefully.
            
            # yfpy structure for a player is usually a list of dicts like:
            # [ [{"player_key":...}, ...], {"player_stats": ...} ]
            # OR if unpacked, it becomes an object.
            # Since we didn't use `unpack_data`, we have raw JSON-like dicts/lists.
            
            p_data = {}
            p_data["game_date"] = target_date
            
            # Helper to find key in nested list/dict structure
            def get_val(data, key):
                if isinstance(data, dict):
                    return data.get(key)
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and key in item:
                            return item[key]
                        # Flattened list?
                        res = get_val(item, key)
                        if res: return res
                return None

            # 1. Identity
            p_key = get_val(p_raw, "player_key")
            p_id = get_val(p_raw, "player_id")
            
            p_data["player_key"] = p_key
            p_data["player_id"] = p_id
            
            # 2. Name
            name_obj = get_val(p_raw, "name")
            if name_obj and "full" in name_obj:
                p_data["PLAYER_NAME"] = name_obj["full"]
                
            # 3. Team
            p_data["TEAM_ABBREVIATION"] = get_val(p_raw, "editorial_team_abbr")
            p_data["TEAM_NAME"] = get_val(p_raw, "editorial_team_full_name")
            tk = get_val(p_raw, "editorial_team_key")
            if tk:
                 p_data["TEAM_ID"] = tk.split(".")[-1]
            
            p_data["POSITION"] = get_val(p_raw, "display_position")
            p_data["JERSEY_NUM"] = get_val(p_raw, "uniform_number")
            
            # 4. Stats
            p_stats_obj = get_val(p_raw, "player_stats")
            stats_list = []
            if p_stats_obj and "stats" in p_stats_obj:
                stats_list = p_stats_obj["stats"]
                
            has_stats = False
            for s_entry in stats_list:
                # s_entry: {"stat": {"stat_id": "...", "value": "..."}}
                if "stat" in s_entry:
                    stat = s_entry["stat"]
                    s_id = str(stat.get("stat_id"))
                    val = stat.get("value")
                    
                    if s_id in stat_map:
                        stat_name = stat_map[s_id]
                        schema_col = map_stat_name_to_schema(stat_name)
                        if schema_col:
                            fval = 0.0
                            if val != "-":
                                if schema_col == "MINUTES" and ":" in str(val):
                                    try:
                                        parts = str(val).split(":")
                                        fval = float(parts[0]) + float(parts[1])/60.0
                                    except: fval = 0.0
                                else:
                                    try:
                                        fval = float(val)
                                    except: fval = 0.0
                            
                            p_data[schema_col] = fval
                            if fval > 0 and schema_col != "MINUTES":
                                has_stats = True
            
            if p_data.get("MINUTES", 0) > 0 or has_stats:
                all_player_data.append(p_data)
                
        time.sleep(1.0) # Pause between batches
            
    print(f"Finished fetching. Total rows: {len(all_player_data)}")
    return pd.DataFrame(all_player_data)


def build_bq_payload(frame: pd.DataFrame, target_date: date) -> pd.DataFrame:
    """Shape the DataFrame for BigQuery."""
    if frame.empty:
        return pd.DataFrame()
        
    df = frame.copy()
    
    # Fill missing standard columns with 0
    std_cols = ["PTS", "REB", "AST", "STL", "BLK", "FG3M", "FGA", "FTA", "TO", "MINUTES", "FGM", "FTM", "FG3A", "PF"]
    for c in std_cols:
        if c not in df.columns:
            df[c] = 0.0
            
    # Calculate percentages
    if "FG_PCT" not in df.columns:
        df["FG_PCT"] = df.apply(lambda x: x["FGM"]/x["FGA"] if x["FGA"] > 0 else 0, axis=1)
    if "FT_PCT" not in df.columns:
        df["FT_PCT"] = df.apply(lambda x: x["FTM"]/x["FTA"] if x["FTA"] > 0 else 0, axis=1)
    if "FG3_PCT" not in df.columns:
        df["FG3_PCT"] = df.apply(lambda x: x["FG3M"]/x["FG3A"] if x["FG3A"] > 0 else 0, axis=1)

    # Derived columns
    df["season"] = _season_from_date(target_date)
    
    # Z-scores
    df = compute_zscores(df)
    
    df["game_id"] = None
    df["player_name"] = df.get("PLAYER_NAME")
    df["player_id"] = df.get("player_id") 
    
    df["team_id"] = df.get("TEAM_ID")
    df["team_abbr"] = df.get("TEAM_ABBREVIATION")
    df["team_name"] = df.get("TEAM_NAME")
    df["position"] = df.get("POSITION")
    df["jersey_num"] = df.get("JERSEY_NUM")
    df["minutes"] = df.get("MINUTES")
    
    df["fgm"] = df.get("FGM")
    df["fga"] = df.get("FGA")
    df["fg_pct"] = df.get("FG_PCT")
    
    df["fg3m"] = df.get("FG3M")
    df["fg3a"] = df.get("FG3A")
    df["fg3_pct"] = df.get("FG3_PCT")
    
    df["ftm"] = df.get("FTM")
    df["fta"] = df.get("FTA")
    df["ft_pct"] = df.get("FT_PCT")
    
    df["pts"] = df.get("PTS")
    df["reb"] = df.get("REB")
    df["ast"] = df.get("AST")
    df["stl"] = df.get("STL")
    df["blk"] = df.get("BLK")
    df["turnovers"] = df.get("TO")
    df["pf"] = df.get("PF")
    
    df["z_score"] = df.get("Z_SCORE")
    
    # Nulls for missing
    for col in ["team_city", "team_slug", "comment", "dreb", "oreb", "plus_minus"]:
        df[col] = None
        
    final_cols = [
        "game_date", "game_id", "player_id", "player_name", "team_id", "team_abbr",
        "team_city", "team_name", "team_slug", "position", "comment", "jersey_num",
        "minutes", "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct", "ftm", "fta",
        "ft_pct", "pts", "reb", "ast", "stl", "blk", "turnovers", "pf", "dreb",
        "oreb", "plus_minus", "z_score", "season"
    ]
    
    return df[final_cols]


def load_into_bigquery(df: pd.DataFrame, client: bigquery.Client, table_name: str):
    """Load dataframe into BigQuery."""
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = client.load_table_from_dataframe(df, table_name, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="YYYY-MM-DD", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Do not write to BQ")
    parser.add_argument("--player", type=str, help="Filter by player name (partial match)", default=None)
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today() - timedelta(days=1)
        
    print(f"Target Date: {target_date}, League ID: {YAHOO_LEAGUE_ID}")
    
    try:
        query = YahooFantasySportsQuery(
            league_id=YAHOO_LEAGUE_ID,
            game_id=YAHOO_GAME_ID,
            game_code=GAME_CODE
        )
    except Exception as e:
        print(f"Failed to initialize Yahoo Query: {e}")
        sys.exit(1)
        
    # 1. Get Stat Map
    stat_map = get_stat_map(query)
    print(f"Mapped {len(stat_map)} stat categories.")
    
    # 2. Fetch Stats
    df = fetch_daily_stats(target_date, query, stat_map, player_filter=args.player)
    print(f"Fetched {len(df)} player records.")
    
    # 3. Process
    payload = build_bq_payload(df, target_date)
    
    if args.dry_run:
        print("Dry Run: Printing sample payload")
        if not payload.empty:
            print(payload.head())
            print(payload.describe())
        else:
            print("Payload is empty.")
        return
        
    # 4. Load
    if not payload.empty:
        client = bigquery.Client(project=DEFAULT_PROJECT_ID)
        load_into_bigquery(payload, client, PARTITIONED_TABLE)
    else:
        print("No data to load.")

if __name__ == "__main__":
    main()
