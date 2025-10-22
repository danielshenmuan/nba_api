"""Ad hoc backfill runner for player_daily_game_stats_p using stats.nba.com feeds."""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
from google.cloud import bigquery
from nba_api.stats.endpoints import BoxScoreTraditionalV3, ScoreboardV2
from requests.exceptions import RequestException

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.daily_ingest import compute_zscores, refresh_league_pg_stats

DEFAULT_PROJECT = "fantasy-survivor-app"
DEFAULT_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"


def _season_from_date(day: date) -> str:
    year = day.year
    if day.month >= 10:
        return f"{year}-{(year + 1) % 100:02d}"
    return f"{year - 1}-{year % 100:02d}"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill player daily game stats for a specific date using the stats.nba.com "
            "BoxScoreTraditionalV3 endpoint."
        )
    )
    parser.add_argument(
        "--date",
        required=True,
        type=lambda value: datetime.strptime(value, "%Y-%m-%d"),
        help="Date to backfill in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--season",
        default=None,
        help=(
            "Optional season override (e.g. 2024-25). Defaults to deriving the season "
            "from the target date."
        ),
    )
    parser.add_argument(
        "--project",
        default=DEFAULT_PROJECT,
        help="Google Cloud project for the BigQuery client.",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=(
            "Fully qualified BigQuery table to load (defaults to the production "
            "player_daily_game_stats_p table)."
        ),
    )
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Skip refreshing the league_pg_stats_by_season materialized data.",
    )
    return parser.parse_args()


def _fetch_game_ids(target_date: datetime, retries: int = 3, timeout: int = 30) -> list[str]:
    game_date_str = target_date.strftime("%m/%d/%Y")

    for attempt in range(retries):
        try:
            board = ScoreboardV2(game_date=game_date_str, league_id="00", day_offset="0", timeout=timeout)
            header = board.get_data_frames()[0]
            if header.empty:
                return []
            ids = header["GAME_ID"].dropna().astype(str).unique().tolist()
            return ids
        except Exception as exc:  # noqa: BLE001
            if isinstance(exc, KeyboardInterrupt):
                raise
            if attempt == retries - 1:
                raise RequestException(f"Failed to load ScoreboardV2 data: {exc}") from exc
            time.sleep(1)

    return []


def _fetch_traditional_boxscore(game_id: str, retries: int = 3, timeout: int = 30) -> pd.DataFrame:
    for attempt in range(retries):
        try:
            box = BoxScoreTraditionalV3(game_id=game_id, timeout=timeout)
            frames = box.get_data_frames()
            if not frames:
                return pd.DataFrame()
            df = frames[0].copy()
            return df
        except Exception as exc:  # noqa: BLE001
            if isinstance(exc, KeyboardInterrupt):
                raise
            if attempt == retries - 1:
                raise RequestException(f"Failed to load box score for {game_id}: {exc}") from exc
            time.sleep(1)

    return pd.DataFrame()


def _build_ingestion_frame(game_ids: Iterable[str], game_date: date) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for game_id in game_ids:
        try:
            box_df = _fetch_traditional_boxscore(game_id)
        except RequestException as exc:
            print(f"Skipping {game_id}: {exc}")
            continue

        if box_df.empty:
            continue

        # Filter out team total rows and ensure player identifiers exist
        cleaned = box_df.copy()
        cleaned = cleaned[~cleaned["PLAYER_NAME"].str.contains("totals", case=False, na=False)]
        cleaned = cleaned[cleaned["PLAYER_ID"].notna()]
        if cleaned.empty:
            continue

        cleaned["GAME_ID"] = str(game_id)
        cleaned["GAME_DATE"] = pd.to_datetime(game_date)

        numeric_cols = [
            "PTS",
            "REB",
            "AST",
            "STL",
            "BLK",
            "FG3M",
            "FG3A",
            "FGM",
            "FGA",
            "FG_PCT",
            "FTM",
            "FTA",
            "FT_PCT",
            "TO",
        ]
        for col in numeric_cols:
            if col in cleaned.columns:
                cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")

        frames.append(cleaned)
        time.sleep(0.3)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = compute_zscores(combined)

    combined["PLAYER_ID"] = pd.to_numeric(combined["PLAYER_ID"], errors="coerce")
    combined = combined[combined["PLAYER_ID"].notna()]

    return combined


def main() -> int:
    args = _parse_args()
    target_date: datetime = args.date

    print(f"Running stats backfill for {target_date.date()}...")

    try:
        game_ids = _fetch_game_ids(target_date)
    except RequestException as exc:
        print(f"Failed to discover games for {target_date.date()}: {exc}")
        return 1

    if not game_ids:
        print(f"No games found on {target_date.date()}.")
        return 0

    player_frame = _build_ingestion_frame(game_ids, target_date.date())

    if player_frame.empty:
        print("No player stats returned; nothing to load.")
        return 0

    if args.season:
        season_value = args.season
    else:
        season_value = _season_from_date(target_date.date())

    player_frame["game_date"] = pd.to_datetime(target_date.date())
    player_frame["season"] = season_value

    out = pd.DataFrame({
        "game_date": player_frame["game_date"],
        "game_id": player_frame["GAME_ID"].astype(str),
        "player_id": player_frame["PLAYER_ID"].astype("Int64"),
        "player_name": player_frame["PLAYER_NAME"].astype(str),
        "team_abbr": player_frame["TEAM_ABBREVIATION"].astype(str),
        "minutes": player_frame["MIN_INT"].astype(float),
        "pts": player_frame["PTS"].astype(float),
        "reb": player_frame["REB"].astype(float),
        "ast": player_frame["AST"].astype(float),
        "stl": player_frame["STL"].astype(float),
        "blk": player_frame["BLK"].astype(float),
        "fg3m": player_frame["FG3M"].astype(float),
        "fg_pct": player_frame["FG_PCT"].astype(float),
        "ft_pct": player_frame["FT_PCT"].astype(float),
        "turnovers": player_frame["TO"].astype(float),
        "z_score": player_frame["Z_SCORE"].astype(float),
        "season": player_frame["season"].astype(str),
    })

    out = out[out["minutes"].notna() & (out["minutes"] > 0)].reset_index(drop=True)

    if out.empty:
        print("All rows filtered out due to zero minutes; nothing to load.")
        return 0

    client = bigquery.Client(project=args.project)
    job = client.load_table_from_dataframe(
        out,
        args.table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
    print(f"Loaded {len(out)} rows into {args.table}.")

    if not args.skip_refresh:
        refresh_league_pg_stats()

    print("Backfill complete ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
