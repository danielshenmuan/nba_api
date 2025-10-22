"""Fetch sample rows shaped like ``player_daily_game_stats_p`` for a date."""
from __future__ import annotations

from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from jobs.daily_ingest import run_ingestion

# Set this to a YYYY-MM-DD string to force a specific date without
# providing the ``--date`` flag (e.g., MANUAL_DATE = "2021-01-15").
# Leave as ``None`` to default to today's games unless ``--date`` is passed.
MANUAL_DATE: str | None = None
MANUAL_DATE = "2025-10-22"

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
        default=(
            datetime.strptime(MANUAL_DATE, "%Y-%m-%d")
            if MANUAL_DATE
            else datetime.today()
        ),
        help=(
            "Target game date in YYYY-MM-DD format (defaults to MANUAL_DATE if set "
            "or today otherwise)."
        ),
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