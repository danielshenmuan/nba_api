"""Fetch sample rows shaped like ``player_daily_game_stats_p`` for a date."""
from __future__ import annotations

from argparse import ArgumentParser
from datetime import datetime

import pandas as pd

from jobs.daily_ingest import run_ingestion


def main() -> None:
    parser = ArgumentParser(
        description=(
            "Check NBA live stats availability by materializing the rows that would "
            "be loaded into player_daily_game_stats_p."
        )
    )
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        default=datetime.today(),
        help="Target game date in YYYY-MM-DD format (defaults to today).",
    )
    args = parser.parse_args()

    target_date = args.date
    print(f"Fetching live data for {target_date.date()}...")

    df = run_ingestion(target_date)
    if df.empty:
        print("No rows returned – confirm there were games and the live API is publishing stats.")
        return

    print(f"Returned {len(df)} rows shaped like player_daily_game_stats_p.")

    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print(df)


if __name__ == "__main__":
    main()
