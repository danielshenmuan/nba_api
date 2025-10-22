"""Ad hoc backfill runner for player_daily_game_stats_p."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.daily_ingest import refresh_league_pg_stats, run_ingestion

DEFAULT_PROJECT = "fantasy-survivor-app"
DEFAULT_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill player daily game stats for a specific date using the live "
            "ingestion pipeline."
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


def main() -> int:
    args = _parse_args()
    target_date: datetime = args.date

    print(f"Running backfill for {target_date.date()}...")
    df = run_ingestion(target_date, season=args.season)

    if df.empty:
        print("No rows returned from ingestion; nothing to load.")
        return 0

    client = bigquery.Client(project=args.project)
    job = client.load_table_from_dataframe(
        df,
        args.table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
    print(f"Loaded {len(df)} rows into {args.table}.")

    if not args.skip_refresh:
        refresh_league_pg_stats()

    print("Backfill complete ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())