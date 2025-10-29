"""Ad hoc backfill for player_daily_game_stats using BoxScoreTraditionalV3."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = Path(__file__).resolve().parent

for path in (REPO_ROOT, JOBS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from jobs import daily_ingest  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill player_daily_game_stats tables for a specific date using "
            "BoxScoreTraditionalV3 data."
        )
    )
    parser.add_argument(
        "--date",
        required=True,
        type=lambda value: datetime.strptime(value, "%Y-%m-%d"),
        help="Target date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--project",
        default=daily_ingest.DEFAULT_PROJECT,
        help="Google Cloud project for BigQuery loads.",
    )
    parser.add_argument(
        "--table",
        default=daily_ingest.PARTITIONED_TABLE,
        help="Fully-qualified partitioned table (player_daily_game_stats_p).",
    )
    parser.add_argument(
        "--mirror-table",
        default=daily_ingest.MIRROR_TABLE,
        help="Optional non-partitioned table to mirror results (player_daily_game_stats).",
    )
    parser.add_argument(
        "--skip-mirror",
        action="store_true",
        help="Skip loading into the mirror table.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and shape data without loading into BigQuery.",
    )
    parser.add_argument(
        "--season",
        default=None,
        help="Optional season override (e.g. 2024-25).",
    )
    parser.add_argument(
        "--game-ids",
        nargs="*",
        help="Optional explicit GAME_ID values to process (skips game discovery).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=daily_ingest.DEFAULT_TIMEOUT,
        help="Timeout in seconds for NBA Stats API requests.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=daily_ingest.DEFAULT_RETRIES,
        help="Number of additional retries for Scoreboard and box score requests.",
    )
    return parser.parse_args()


def _display_payload(payload: pd.DataFrame) -> None:
    if payload.empty:
        print("No rows returned for the requested date.")
        return

    print(f"Prepared {len(payload)} rows. Preview:")
    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print(payload)


def main() -> None:
    args = _parse_args()

    payload = daily_ingest.run_ingestion(
        args.date,
        project_id=args.project,
        partitioned_table=args.table,
        mirror_table=args.mirror_table,
        skip_mirror=args.skip_mirror,
        dry_run=args.dry_run,
        game_ids=args.game_ids,
        season_override=args.season,
        timeout=args.timeout,
        retries=args.retries,
    )

    if args.dry_run:
        _display_payload(payload)
    elif payload.empty:
        print("Backfill completed with no rows loaded.")
    else:
        print(f"Backfill completed. Loaded {len(payload)} rows.")


if __name__ == "__main__":
    main()
