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
from requests.exceptions import RequestException

try:
    from jobs.boxscore_v3_utils import (
        DEFAULT_RETRIES as BOX_DEFAULT_RETRIES,
        DEFAULT_TIMEOUT as BOX_DEFAULT_TIMEOUT,
        discover_game_ids,
        load_traditional_boxscore,
        map_traditional_boxscore,
    )
    from jobs.daily_ingest import (
        build_bq_payload,
        compute_zscores,
        load_into_bigquery_tables,
        refresh_league_pg_stats,
    )
except ModuleNotFoundError as exc:
    if exc.name != "jobs":
        raise
    CURRENT_DIR = Path(__file__).resolve().parent
    if str(CURRENT_DIR) not in sys.path:
        sys.path.insert(0, str(CURRENT_DIR))
    from boxscore_v3_utils import (  # type: ignore
        DEFAULT_RETRIES as BOX_DEFAULT_RETRIES,
        DEFAULT_TIMEOUT as BOX_DEFAULT_TIMEOUT,
        discover_game_ids,
        load_traditional_boxscore,
        map_traditional_boxscore,
    )
    from daily_ingest import (  # type: ignore
        build_bq_payload,
        compute_zscores,
        load_into_bigquery_tables,
        refresh_league_pg_stats,
    )

DEFAULT_PROJECT = "fantasy-survivor-app"
DEFAULT_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"
DEFAULT_MIRROR_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats"
DEFAULT_TIMEOUT = BOX_DEFAULT_TIMEOUT
DEFAULT_RETRIES = BOX_DEFAULT_RETRIES


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
        "--mirror-table",
        default=DEFAULT_MIRROR_TABLE,
        help="Optional non-partitioned table to mirror results into.",
    )
    parser.add_argument(
        "--skip-mirror",
        action="store_true",
        help="Skip loading rows into the mirror table.",
    )
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Skip refreshing the league_pg_stats_by_season materialized data.",
    )
    return parser.parse_args()

def _build_ingestion_frame(
    game_ids: Iterable[str],
    game_date: date,
    season_value: str | None,
    *,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for game_id in game_ids:
        try:
            box_df = load_traditional_boxscore(
                game_id, retries=retries, timeout=timeout
            )
        except RequestException as exc:
            print(f"Skipping {game_id}: {exc}")
            continue

        if box_df.empty:
            continue

        mapped = map_traditional_boxscore(box_df, game_id, game_date)
        if mapped.empty:
            print(f"Skipping {game_id}: box score missing required player data.")
            continue

        frames.append(mapped)
        time.sleep(0.3)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = compute_zscores(combined)
    return build_bq_payload(combined, season_value)


def main() -> int:
    args = _parse_args()
    target_date: datetime = args.date

    print(f"Running stats backfill for {target_date.date()}...")

    if args.season:
        season_value = args.season
    else:
        season_value = _season_from_date(target_date.date())

    try:
        game_ids = discover_game_ids(
            target_date, retries=DEFAULT_RETRIES, timeout=DEFAULT_TIMEOUT
        )
    except RequestException as exc:
        print(f"Failed to discover games for {target_date.date()}: {exc}")
        return 1

    if not game_ids:
        print(f"No games found on {target_date.date()}.")
        return 0

    player_frame = _build_ingestion_frame(
        game_ids,
        target_date.date(),
        season_value,
        retries=DEFAULT_RETRIES,
        timeout=DEFAULT_TIMEOUT,
    )

    if player_frame.empty:
        print("No player stats returned; nothing to load.")
        return 0

    mirror_table = None if args.skip_mirror else args.mirror_table

    client = bigquery.Client(project=args.project)
    load_into_bigquery_tables(
        player_frame,
        client=client,
        project_id=args.project,
        partitioned_table=args.table,
        mirror_table=mirror_table,
    )

    if not args.skip_refresh:
        refresh_league_pg_stats()

    print("Backfill complete ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
