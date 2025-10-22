# daily_ingest.py
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery
from nba_api.live.nba.endpoints import boxscore as live_boxscore
from nba_api.live.nba.endpoints import scoreboard as live_scoreboard
from requests.exceptions import RequestException

# ----------------------------
# Baseline stats (update each season if needed)
# ----------------------------
WEIGHTED_MEAN = [11.69, 4.32, 2.76, 0.75, 0.50, 1.28, 0.47, 0.75, 1.33]
WEIGHTED_STD  = [7.23,  2.51, 2.09, 0.38, 0.45, 0.95, 0.082, 0.124, 0.85]

# ----------------------------
# Helpers
# ----------------------------
def _season_from_date(d: datetime.date) -> str:
    y = d.year
    return f"{y}-{(y + 1) % 100:02d}" if d.month >= 10 else f"{y - 1}-{y % 100:02d}"


LIVE_TODAY_ENDPOINT = "scoreboard/todaysScoreboard_00.json"


def _scoreboard_endpoint_for_date(target_date: datetime) -> str:
    if target_date.date() == datetime.today().date():
        return LIVE_TODAY_ENDPOINT
    return f"scoreboard/scoreboard_{target_date.strftime('%Y%m%d')}.json"


def load_games(target_date: datetime, timeout: int = 15, retries: int = 3) -> list[dict]:
    board = live_scoreboard.ScoreBoard(get_request=False, timeout=timeout)
    board.endpoint_url = _scoreboard_endpoint_for_date(target_date)

    for attempt in range(retries):
        try:
            board.get_request()
            data = board.get_dict()
            games = data.get("scoreboard", {}).get("games", [])
            return games or []
        except RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(1)

    return []


def fetch_boxscore(game_id: str, timeout: int = 15, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            box = live_boxscore.BoxScore(game_id=game_id, timeout=timeout)
            return box.get_dict().get("game", {})
        except RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(1)

    return {}


def _game_date_from_local_tip(game: dict, fallback: datetime.date) -> datetime.date:
    raw = game.get("gameTimeLocal")
    if not raw:
        return fallback
    try:
        return datetime.fromisoformat(raw).date()
    except ValueError:
        return fallback


_MINUTES_ISO_PATTERN = re.compile(r"PT(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?")


def _normalize_minutes(value: str | None) -> str | None:
    if not value:
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.startswith("PT"):
            match = _MINUTES_ISO_PATTERN.fullmatch(s)
            if match:
                minutes = int(match.group("minutes") or 0)
                raw_seconds = match.group("seconds") or "0"
                seconds = int(float(raw_seconds))
                return f"{minutes:d}:{seconds:02d}"
        if ":" in s:
            mins, secs = s.split(":", 1)
            try:
                minutes = int(float(mins))
                seconds_part = secs.split(".")[0]
                seconds = int(float(seconds_part))
                return f"{minutes:d}:{seconds:02d}"
            except ValueError:
                return None
    if isinstance(value, (int, float)):
        minutes = int(value)
        return f"{minutes:d}:00"
    return None


def _safe_number(val, default: float = 0.0) -> float:
    if val in (None, "", " "):
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_percentage(val) -> float:
    if val in (None, "", " "):
        return float("nan")
    try:
        return float(val) / 100.0
    except (TypeError, ValueError):
        return float("nan")

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

def run_ingestion(target_date: datetime | None = None, season: str | None = None) -> pd.DataFrame:
    if target_date is None:
        target_date = datetime.today() - timedelta(days=1)

    try:
        games = load_games(target_date)
    except RequestException as exc:
        print(f"Failed to load live scoreboard for {target_date.date()}: {exc}")
        return pd.DataFrame()

    if not games:
        print(f"No games on {target_date.date()}")
        return pd.DataFrame()

    rows: list[dict] = []
    for game in games:
        game_id = game.get("gameId")
        if not game_id:
            continue

        try:
            payload = fetch_boxscore(game_id)
        except RequestException as exc:
            print(f"Failed to fetch live box score for {game_id}: {exc}")
            continue

        if not payload:
            continue

        game_date = _game_date_from_local_tip(game, fallback=target_date.date())

        for side in ("homeTeam", "awayTeam"):
            team = payload.get(side) or {}
            team_abbr = team.get("teamTricode")
            players = team.get("players") or []
            for player in players:
                stats = player.get("statistics") or {}
                row = {
                    "GAME_ID": str(game_id),
                    "PLAYER_ID": player.get("personId"),
                    "PLAYER_NAME": player.get("name"),
                    "TEAM_ABBREVIATION": team_abbr,
                    "MIN": _normalize_minutes(stats.get("minutes")),
                    "FGM": _safe_number(stats.get("fieldGoalsMade")),
                    "FGA": _safe_number(stats.get("fieldGoalsAttempted")),
                    "FG_PCT": _safe_percentage(stats.get("fieldGoalsPercentage")),
                    "FG3M": _safe_number(stats.get("threePointersMade")),
                    "FG3A": _safe_number(stats.get("threePointersAttempted")),
                    "FG3_PCT": _safe_percentage(stats.get("threePointersPercentage")),
                    "FTM": _safe_number(stats.get("freeThrowsMade")),
                    "FTA": _safe_number(stats.get("freeThrowsAttempted")),
                    "FT_PCT": _safe_percentage(stats.get("freeThrowsPercentage")),
                    "OREB": _safe_number(stats.get("reboundsOffensive")),
                    "DREB": _safe_number(stats.get("reboundsDefensive")),
                    "REB": _safe_number(stats.get("reboundsTotal")),
                    "AST": _safe_number(stats.get("assists")),
                    "STL": _safe_number(stats.get("steals")),
                    "BLK": _safe_number(stats.get("blocks")),
                    "TO": _safe_number(stats.get("turnovers")),
                    "PF": _safe_number(stats.get("foulsPersonal")),
                    "PTS": _safe_number(stats.get("points")),
                    "GAME_DATE": game_date,
                }
                rows.append(row)

        time.sleep(0.3)

    if not rows:
        return pd.DataFrame()

    all_df = pd.DataFrame(rows)
    all_df = compute_zscores(all_df)

    # Build final frame matching BigQuery table schema
    all_df["game_date"] = all_df["GAME_DATE"]
    if season:
        all_df["season"] = season
    else:
        all_df["season"] = all_df["game_date"].apply(_season_from_date)

    all_df["PLAYER_ID"] = pd.to_numeric(all_df["PLAYER_ID"], errors="coerce")

    out = pd.DataFrame({
        "game_date": all_df["game_date"],
        "game_id": all_df["GAME_ID"].astype(str),
        "player_id": all_df["PLAYER_ID"].astype("Int64"),
        "player_name": all_df["PLAYER_NAME"].astype(str),
        "team_abbr": all_df["TEAM_ABBREVIATION"].astype(str),
        "minutes": all_df["MIN_INT"].astype(float),
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
        "season": all_df["season"].astype(str),
    })

    # Drop DNP rows (no minutes parsed)
    out = out[out["minutes"].notna() & (out["minutes"] > 0)].reset_index(drop=True)
    return out

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
