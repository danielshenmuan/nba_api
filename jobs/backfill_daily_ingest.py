"""Ad hoc helper to backfill daily player stats into BigQuery."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
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
            "Backfill player_daily_game_stats_p rows for a specific date using the "
            "live ingestion pipeline."
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

    print(f"Fetching stats for {target_date.date()}...")
    df = run_ingestion(target_date, season=args.season)

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
