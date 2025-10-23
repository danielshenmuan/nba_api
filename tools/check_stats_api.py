"""Materialize player_daily_game_stats_p-shaped rows via BoxScoreTraditionalV3."""
from __future__ import annotations

import importlib
import sys
from argparse import ArgumentParser
from datetime import date, datetime
from pathlib import Path
import time
from typing import Iterable

import pandas as pd
from requests.exceptions import RequestException

REPO_ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = REPO_ROOT / "jobs"

for path in (REPO_ROOT, JOBS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def _import_module(*names: str):  # pragma: no cover - import helper
    for module_name in names:
        try:
            return importlib.import_module(module_name)
        except ModuleNotFoundError:
            continue
    raise ModuleNotFoundError(f"Unable to import any of: {names}")


_box_utils = _import_module("jobs.boxscore_v3_utils", "boxscore_v3_utils")
_ingest_module = _import_module("jobs.daily_ingest", "daily_ingest")

DEFAULT_TIMEOUT = _box_utils.DEFAULT_TIMEOUT
DEFAULT_RETRIES = _box_utils.DEFAULT_RETRIES
discover_game_ids = _box_utils.discover_game_ids
load_traditional_boxscore = _box_utils.load_traditional_boxscore
map_traditional_boxscore = _box_utils.map_traditional_boxscore
build_bq_payload = _ingest_module.build_bq_payload
compute_zscores = _ingest_module.compute_zscores

# Set this to a YYYY-MM-DD string to force a specific date without passing --date.
MANUAL_DATE: str | None = None


def _season_from_date(day: date) -> str:
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
            raw_df = load_traditional_boxscore(game_id, retries=retries, timeout=timeout)
        except RequestException as exc:
            print(f"Skipping {game_id}: {exc}")
            continue

        if raw_df.empty:
            print(f"Skipping {game_id}: box score payload not available yet.")
            continue

        mapped = map_traditional_boxscore(raw_df, game_id, target_date.date())
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


def main() -> None:
    parser = ArgumentParser(
        description=(
            "Check stats.nba.com availability by shaping BoxScoreTraditionalV3 data into "
            "player_daily_game_stats_p rows."
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
            "Target game date in YYYY-MM-DD format (defaults to MANUAL_DATE if set or "
            "today otherwise)."
        ),
    )
    parser.add_argument(
        "--game-ids",
        nargs="*",
        help="Optional explicit GAME_ID values to inspect (skips ScoreboardV2 lookup).",
    )
    parser.add_argument(
        "--season",
        default=None,
        help="Optional season override (e.g. 2024-25).",
    )
    args = parser.parse_args()

    target_date = args.date
    season_value = args.season or _season_from_date(target_date.date())

    print(
        "Fetching ScoreboardV2 game IDs and BoxScoreTraditionalV3 player stats for "
        f"{target_date.date()}..."
    )

    if args.game_ids:
        game_ids = [str(gid) for gid in args.game_ids]
    else:
        try:
            game_ids = discover_game_ids(
                target_date, retries=DEFAULT_RETRIES, timeout=DEFAULT_TIMEOUT
            )
        except RequestException as exc:
            print(f"Failed to discover games for {target_date.date()}: {exc}")
            return

        if not game_ids:
            print(f"No games found on {target_date.date()}.")
            return

    frame = _build_bq_frame(game_ids, target_date, season_value)
    if frame.empty:
        print("No rows returned – confirm box scores are published for that date.")
        return

    print(f"Returned {len(frame)} rows shaped like player_daily_game_stats_p.")
    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print(frame)


if __name__ == "__main__":
    main()
