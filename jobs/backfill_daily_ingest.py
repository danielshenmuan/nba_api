"""Ad hoc helper to backfill daily player stats into BigQuery."""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
from google.cloud import bigquery
from nba_api.stats.endpoints import BoxScoreTraditionalV3
from nba_api.stats.library.http import NBAStatsHTTP
from requests.exceptions import RequestException

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.boxscore_v3_utils import map_traditional_boxscore
from jobs.daily_ingest import compute_zscores, refresh_league_pg_stats

DEFAULT_PROJECT = "fantasy-survivor-app"
DEFAULT_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3


def _season_from_date(day: datetime) -> str:
    year = day.year
    if day.month >= 10:
        return f"{year}-{(year + 1) % 100:02d}"
    return f"{year - 1}-{year % 100:02d}"


def _discover_game_ids(
    target_date: datetime,
    *,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
) -> list[str]:
    params = {
        "GameDate": target_date.strftime("%m/%d/%Y"),
        "DayOffset": 0,
        "LeagueID": "00",
    }

    for attempt in range(retries):
        try:
            response = NBAStatsHTTP().send_api_request(
                endpoint="scoreboardv2",
                parameters=params,
                timeout=timeout,
            )
            data = response.get_normalized_dict()
            headers = data.get("GameHeader", []) if data else []
            ids = [str(row["GAME_ID"]) for row in headers if row.get("GAME_ID")]
            return sorted(set(ids))
        except Exception as exc:  # noqa: BLE001 - stats API raises generic exceptions
            if isinstance(exc, KeyboardInterrupt):
                raise
            if attempt == retries - 1:
                raise RequestException(
                    f"Failed to load ScoreboardV2 data: {exc}"
                ) from exc
            time.sleep(1)

    return []


def _load_boxscore(
    game_id: str,
    *,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    for attempt in range(retries):
        try:
            box = BoxScoreTraditionalV3(game_id=game_id, timeout=timeout)
            frames = box.get_data_frames()
            if not frames:
                return pd.DataFrame()
            return frames[0].copy()
        except Exception as exc:  # noqa: BLE001 - SDK raises generic exceptions
            if isinstance(exc, KeyboardInterrupt):
                raise
            if attempt == retries - 1:
                raise RequestException(
                    f"Failed to load BoxScoreTraditionalV3 for {game_id}: {exc}"
                ) from exc
            time.sleep(1)

    return pd.DataFrame()


def _build_bq_frame(
    game_ids: Iterable[str],
    target_date: datetime,
    season_value: str,
    *,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for game_id in game_ids:
        try:
            raw = _load_boxscore(game_id, retries=retries, timeout=timeout)
        except RequestException as exc:
            print(f"Skipping {game_id}: {exc}")
            continue

        if raw.empty:
            print(f"Skipping {game_id}: box score not available.")
            continue

        mapped = map_traditional_boxscore(raw, game_id, target_date.date())
        if mapped.empty:
            print(f"Skipping {game_id}: box score missing required player data.")
            continue

        frames.append(mapped)
        time.sleep(0.25)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = compute_zscores(combined)
    combined["season"] = season_value

    combined["PLAYER_ID"] = pd.to_numeric(combined["PLAYER_ID"], errors="coerce")
    combined["GAME_ID_INT"] = pd.to_numeric(combined["GAME_ID"], errors="coerce")
    
    combined = combined[combined["PLAYER_ID"].notna()]
    combined = combined[combined["GAME_ID_INT"].notna()]

    out = pd.DataFrame(
        {
            "game_date": pd.to_datetime(combined["GAME_DATE"]).dt.date,
            "game_id": combined["GAME_ID_INT"].astype("Int64"),
            "player_id": combined["PLAYER_ID"].astype("Int64"),
            "player_name": combined["PLAYER_NAME"].astype(str),
            "minutes": combined["MIN_INT"].astype(float),
            "pts": combined["PTS"].astype(float),
            "reb": combined["REB"].astype(float),
            "ast": combined["AST"].astype(float),
            "stl": combined["STL"].astype(float),
            "blk": combined["BLK"].astype(float),
            "fg3m": combined["FG3M"].astype(float),
            "fg_pct": combined["FG_PCT"].astype(float),
            "ft_pct": combined["FT_PCT"].astype(float),
            "turnovers": combined["TO"].astype(float),
            "z_score": combined["Z_SCORE"].astype(float),
            "season": combined["season"].astype(str),
        }
    )

    out = out[out["minutes"].notna() & (out["minutes"] > 0)].reset_index(drop=True)
    return out


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill player_daily_game_stats_p rows for a specific date by pulling "
            "ScoreboardV2 game IDs and BoxScoreTraditionalV3 stats."
        )
    )
    parser.add_argument(
        "--date",
        required=True,
        type=lambda value: datetime.strptime(value, "%Y-%m-%d"),
        help="Target date in YYYY-MM-DD format.",
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
        "--game-ids",
        nargs="*",
        help="Optional explicit list of GAME_ID values to process (skips scoreboard lookup).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional maximum number of rows to display.",
    )
    parser.add_argument(
        "--project",
        default=DEFAULT_PROJECT,
        help="Google Cloud project for the BigQuery client.",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help="Fully-qualified BigQuery table to append results to.",
    )
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Skip refreshing league_pg_stats_by_season after loading data.",
    )
    return parser.parse_args()


def _display_frame(df: pd.DataFrame, max_rows: int | None = None) -> None:
    if df.empty:
        print("No rows returned for the requested date.")
        return

    row_count = len(df)
    print(f"Retrieved {row_count} rows.")

    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        if max_rows is not None and row_count > max_rows:
            print(df.head(max_rows).to_string(index=False))
            print(f"... (showing first {max_rows} of {row_count} rows)")
        else:
            print(df.to_string(index=False))


def main() -> int:
    args = _parse_args()
    target_date: datetime = args.date

    season_value = args.season or _season_from_date(target_date)

    if args.game_ids:
        game_ids = [str(gid) for gid in args.game_ids]
    else:
        try:
            game_ids = _discover_game_ids(target_date)
        except RequestException as exc:
            print(f"Failed to discover games for {target_date.date()}: {exc}")
            return 0

    if not game_ids:
        print(f"No games found on {target_date.date()}.")
        return 0

    print(f"Fetching stats for {target_date.date()} across {len(game_ids)} games...")
    df = _build_bq_frame(game_ids, target_date, season_value)

    if df.empty:
        print("No rows returned; nothing to load.")
        return 0

    _display_frame(df, args.max_rows)

    client = bigquery.Client(project=args.project)
    job = client.load_table_from_dataframe(
        df,
        args.table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
    print(f"Loaded {len(df)} rows into {args.table} for {target_date.date()}.")

    if not args.skip_refresh:
        refresh_league_pg_stats()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
