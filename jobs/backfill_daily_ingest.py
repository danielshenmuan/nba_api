"""Ad hoc helper to backfill daily player stats into BigQuery."""
from __future__ import annotations

import argparse
import importlib
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
from google.cloud import bigquery
from requests.exceptions import RequestException

CURRENT_DIR = Path(__file__).resolve().parent


def _import_module(*names: tuple[str, bool]):  # pragma: no cover - import helper
    """Attempt to import the first available module.

    Each tuple is (module_name, ensure_current_dir)."""

    for module_name, add_current in names:
        try:
            if add_current and str(CURRENT_DIR) not in sys.path:
                sys.path.insert(0, str(CURRENT_DIR))
            return importlib.import_module(module_name)
        except ModuleNotFoundError:
            continue
    raise ModuleNotFoundError(
        f"Unable to import any of: {[name for name, _ in names]}. Ensure they are packaged."
    )


_box_utils = _import_module(("jobs.boxscore_v3_utils", False), ("boxscore_v3_utils", True))
_ingest_module = _import_module(("jobs.daily_ingest", False), ("daily_ingest", True))

DEFAULT_RETRIES = _box_utils.DEFAULT_RETRIES
DEFAULT_TIMEOUT = _box_utils.DEFAULT_TIMEOUT
discover_game_ids = _box_utils.discover_game_ids
load_traditional_boxscore = _box_utils.load_traditional_boxscore
map_traditional_boxscore = _box_utils.map_traditional_boxscore

build_bq_payload = _ingest_module.build_bq_payload
compute_zscores = _ingest_module.compute_zscores
load_into_bigquery_tables = _ingest_module.load_into_bigquery_tables
refresh_league_pg_stats = _ingest_module.refresh_league_pg_stats

DEFAULT_PROJECT = "fantasy-survivor-app"
DEFAULT_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"
DEFAULT_MIRROR_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats"


def _season_from_date(day: datetime) -> str:
    year = day.year
    if day.month >= 10:
        return f"{year}-{(year + 1) % 100:02d}"
    return f"{year - 1}-{year % 100:02d}"

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
            raw = load_traditional_boxscore(game_id, retries=retries, timeout=timeout)
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
    return build_bq_payload(combined, season_value)


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
        "--mirror-table",
        default=DEFAULT_MIRROR_TABLE,
        help=(
            "Optional non-partitioned table to mirror (defaults to player_daily_game_stats)."
        ),
    )
    parser.add_argument(
        "--skip-mirror",
        action="store_true",
        help="Skip loading into the mirror table.",
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
            game_ids = discover_game_ids(
                target_date, retries=DEFAULT_RETRIES, timeout=DEFAULT_TIMEOUT
            )
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

    mirror_table = None if args.skip_mirror else args.mirror_table

    client = bigquery.Client(project=args.project)
    load_into_bigquery_tables(
        df,
        client=client,
        project_id=args.project,
        partitioned_table=args.table,
        mirror_table=mirror_table,
    )

    if not args.skip_refresh:
        refresh_league_pg_stats(client=client, project_id=args.project)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
