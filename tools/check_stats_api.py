"""Materialize player_daily_game_stats_p-shaped rows via BoxScoreTraditionalV3."""
from __future__ import annotations

from argparse import ArgumentParser
from datetime import date, datetime
from pathlib import Path
import sys
import time
from typing import Iterable

import pandas as pd
from nba_api.stats.endpoints import BoxScoreTraditionalV3
from nba_api.stats.library.http import NBAStatsHTTP
from requests.exceptions import RequestException

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from jobs.daily_ingest import compute_zscores

# Set this to a YYYY-MM-DD string to force a specific date without passing --date.
MANUAL_DATE: str | None = None
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3


def _season_from_date(day: date) -> str:
    year = day.year
    if day.month >= 10:
        return f"{year}-{(year + 1) % 100:02d}"
    return f"{year - 1}-{year % 100:02d}"


def _fetch_game_ids(
    target_date: datetime,
    *,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
) -> list[str]:
    """Return game IDs for the date using the ScoreboardV2 endpoint."""

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
                raise RequestException(f"Failed to load ScoreboardV2 data: {exc}") from exc
            time.sleep(1)

    return []


def _fetch_boxscore(
    game_id: str,
    *,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    for attempt in range(retries):
        try:
            box = BoxScoreTraditionalV3(game_id=game_id, timeout=timeout)
            try:
                frames = box.get_data_frames()
            except AttributeError as attr_err:
                # The stats API occasionally responds with an empty payload, causing the
                # SDK to call ``.keys()`` on ``None`` while building DataFrames. Treat
                # that as "no data yet" instead of bubbling an opaque exception.
                if "'NoneType' object has no attribute 'keys'" in str(attr_err):
                    return pd.DataFrame()
                raise
            if not frames:
                return pd.DataFrame()
            return frames[0].copy()
        except Exception as exc:  # noqa: BLE001 - stats API raises generic exceptions
            if isinstance(exc, KeyboardInterrupt):
                raise
            if attempt == retries - 1:
                raise RequestException(f"Failed to load box score for {game_id}: {exc}") from exc
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
            raw_df = _fetch_boxscore(game_id, retries=retries, timeout=timeout)
        except RequestException as exc:
            print(f"Skipping {game_id}: {exc}")
            continue

        if raw_df.empty:
            print(f"Skipping {game_id}: box score payload not available yet.")
            continue

        cleaned = raw_df.copy()

        required_columns = {
            "PLAYER_ID",
            "PLAYER_NAME",
            "TEAM_ABBREVIATION",
            "MIN",
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
            "FG3_PCT",
            "FTM",
            "FTA",
            "FT_PCT",
            "TO",
        }
        missing = [col for col in required_columns if col not in cleaned.columns]
        if missing:
            missing_str = ", ".join(sorted(missing))
            print(
                "Skipping "
                f"{game_id}: box score payload missing required columns: {missing_str}."
            )
            continue

        if "TEAM_ABBREVIATION" in cleaned.columns:
            cleaned = cleaned[
                cleaned["TEAM_ABBREVIATION"].astype(str).str.upper() != "TOT"
            ]
        cleaned = cleaned[cleaned["PLAYER_ID"].notna()]
        if cleaned.empty:
            continue

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
            "FG3_PCT",
            "FTM",
            "FTA",
            "FT_PCT",
            "TO",
        ]
        for col in numeric_cols:
            cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")

        cleaned["GAME_ID"] = str(game_id)
        cleaned["GAME_DATE"] = pd.to_datetime(target_date.date())
        frames.append(cleaned)
        time.sleep(0.3)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = compute_zscores(combined)

    combined["PLAYER_ID"] = pd.to_numeric(combined["PLAYER_ID"], errors="coerce")
    combined = combined[combined["PLAYER_ID"].notna()]

    combined["game_date"] = pd.to_datetime(target_date.date())
    combined["season"] = season_value

    out = pd.DataFrame({
        "game_date": combined["game_date"],
        "game_id": combined["GAME_ID"].astype(str),
        "player_id": combined["PLAYER_ID"].astype("Int64"),
        "player_name": combined["PLAYER_NAME"].astype(str),
        "team_abbr": combined["TEAM_ABBREVIATION"].astype(str),
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
    })

    out = out[out["minutes"].notna() & (out["minutes"] > 0)].reset_index(drop=True)
    return out


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
            game_ids = _fetch_game_ids(target_date)
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
