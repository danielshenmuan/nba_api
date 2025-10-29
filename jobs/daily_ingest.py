"""Daily ingestion of NBA player box scores into BigQuery."""
from __future__ import annotations

import argparse
import re
from datetime import date, datetime, timedelta
from typing import Iterable, List

import numpy as np
import pandas as pd
from google.cloud import bigquery
from nba_api.stats.endpoints import BoxScoreTraditionalV3, LeagueGameLog, ScoreboardV2
from requests.exceptions import RequestException

DEFAULT_PROJECT = "fantasy-survivor-app"
PARTITIONED_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"
MIRROR_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats"

DEFAULT_TIMEOUT = 10
DEFAULT_RETRIES = 0

DEFAULT_MEAN = [12.44, 4.71, 2.81, 0.90, 0.60, 1.40, 0.46, 0.77, 1.46]
DEFAULT_STDEV = [6.44, 2.51, 2.08, 0.37, 0.41, 0.92, 0.075, 0.11, 0.90]
FGA_NORM = 10.213
FTA_NORM = 2.575
NINE_CAT_ORDER = ["PTS", "REB", "AST", "STL", "BLK", "FG3M", "FG_PCT", "FT_PCT", "TO"]

_MINUTES_PATTERN = re.compile(r"PT(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?")

INT_SOURCE_MAP = {
    "fgm": "fieldGoalsMade",
    "fga": "fieldGoalsAttempted",
    "fg3m": "threePointersMade",
    "fg3a": "threePointersAttempted",
    "ftm": "freeThrowsMade",
    "fta": "freeThrowsAttempted",
    "pts": "points",
    "reb": "reboundsTotal",
    "ast": "assists",
    "stl": "steals",
    "blk": "blocks",
    "turnovers": "turnovers",
    "pf": "foulsPersonal",
    "dreb": "reboundsDefensive",
    "oreb": "reboundsOffensive",
    "plus_minus": "plusMinusPoints",
}

FLOAT_SOURCE_MAP = {
    "fg_pct": "fieldGoalsPercentage",
    "fg3_pct": "threePointersPercentage",
    "ft_pct": "freeThrowsPercentage",
}

ZSCORE_SOURCE_MAP = {
    "PTS": "points",
    "REB": "reboundsTotal",
    "AST": "assists",
    "STL": "steals",
    "BLK": "blocks",
    "FG3M": "threePointersMade",
    "FG_PCT": "fieldGoalsPercentage",
    "FT_PCT": "freeThrowsPercentage",
    "TO": "turnovers",
}

STRING_SOURCE_MAP = {
    "team_abbr": "teamTricode",
    "team_city": "teamCity",
    "team_name": "teamName",
    "team_slug": "teamSlug",
    "position": "position",
    "comment": "comment",
    "jersey_num": "jerseyNum",
}

BQ_COLUMNS = [
    "game_date",
    "game_id",
    "player_id",
    "player_name",
    "team_id",
    "team_abbr",
    "team_city",
    "team_name",
    "team_slug",
    "position",
    "comment",
    "jersey_num",
    "minutes",
    "fgm",
    "fga",
    "fg_pct",
    "fg3m",
    "fg3a",
    "fg3_pct",
    "ftm",
    "fta",
    "ft_pct",
    "pts",
    "reb",
    "ast",
    "stl",
    "blk",
    "turnovers",
    "pf",
    "dreb",
    "oreb",
    "plus_minus",
    "z_score",
    "season",
]


def _season_from_date(day: date) -> str:
    year = day.year
    if day.month >= 10:
        return f"{year}-{(year + 1) % 100:02d}"
    return f"{year - 1}-{year % 100:02d}"


def _normalize_game_id(raw_value) -> str | None:
    if raw_value is None or (isinstance(raw_value, float) and np.isnan(raw_value)):
        return None
    gid_str = str(raw_value).strip()
    if not gid_str:
        return None
    if "." in gid_str:
        try:
            gid_str = f"{int(float(gid_str)):010d}"
        except (TypeError, ValueError):
            return None
    elif gid_str.isdigit() and len(gid_str) < 10:
        gid_str = gid_str.zfill(10)
    return gid_str


def discover_game_ids(
    target_date: datetime,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
) -> list[str]:
    formatted = target_date.strftime("%m/%d/%Y")
    errors: list[Exception] = []

    for attempt in range(retries + 1):
        try:
            board = ScoreboardV2(
                game_date=formatted,
                day_offset=0,
                league_id="00",
                timeout=timeout,
            )
            frames = board.get_data_frames()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        else:
            if frames:
                header = frames[0]
                if "GAME_ID" in header.columns:
                    game_ids = {
                        _normalize_game_id(value)
                        for value in header["GAME_ID"].dropna().tolist()
                    }
                    game_ids.discard(None)
                    if game_ids:
                        return sorted(game_ids)
        if attempt < retries:
            continue

    season = _season_from_date(target_date.date())
    try:
        log = LeagueGameLog(
            counter=0,
            date_from_nullable=formatted,
            date_to_nullable=formatted,
            league_id_nullable="00",
            player_or_team="P",
            season=season,
            season_type_all_star="Regular Season",
            timeout=timeout,
        )
        frames = log.get_data_frames()
        if frames:
            frame = frames[0]
            if "GAME_ID" in frame.columns:
                game_ids = {
                    _normalize_game_id(value)
                    for value in frame["GAME_ID"].dropna().unique().tolist()
                }
                game_ids.discard(None)
                if game_ids:
                    return sorted(game_ids)
    except Exception as exc:  # noqa: BLE001
        errors.append(exc)

    if errors:
        raise RequestException(f"Failed to discover games for {formatted}: {errors[-1]}")
    return []


def _minutes_to_float(value) -> float:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return 0.0
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return 0.0
        if s.startswith("PT"):
            match = _MINUTES_PATTERN.fullmatch(s)
            if match:
                minutes = int(match.group("minutes") or 0)
                seconds = float(match.group("seconds") or 0)
                return minutes + seconds / 60
            return 0.0
        if ":" in s:
            mins, secs = s.split(":", 1)
            try:
                minutes = float(mins)
                seconds = float(secs)
                return minutes + seconds / 60
            except ValueError:
                return 0.0
        try:
            return float(s)
        except ValueError:
            return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def load_traditional_boxscore(game_id: str, *, timeout: int = DEFAULT_TIMEOUT) -> pd.DataFrame:
    try:
        box = BoxScoreTraditionalV3(game_id=game_id, timeout=timeout)
        frames = box.get_data_frames()
    except Exception as exc:  # noqa: BLE001
        raise RequestException(
            f"Failed to load BoxScoreTraditionalV3 for {game_id}: {exc}"
        ) from exc

    if not frames:
        return pd.DataFrame()

    frame = frames[0]
    if frame is None or frame.empty:
        return pd.DataFrame()
    return frame.copy()


def collect_boxscores(
    game_ids: Iterable[str],
    target_date: datetime,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for game_id in game_ids:
        last_error: Exception | None = None
        for _ in range(retries + 1):
            try:
                frame = load_traditional_boxscore(game_id, timeout=timeout)
            except RequestException as exc:
                last_error = exc
                frame = pd.DataFrame()
            if not frame.empty:
                frame = frame.copy()
                frame["gameId"] = frame.get("gameId", game_id)
                frame["game_date"] = target_date.date()
                frames.append(frame)
                break
        else:
            message = str(last_error) if last_error else "box score payload not available"
            print(f"Skipping {game_id}: {message}")

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def _compute_zscore_row(row_vals: List[float], fga: float, fta: float) -> float:
    diff = np.subtract(row_vals, DEFAULT_MEAN)
    stdev_array = np.array(DEFAULT_STDEV, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        z_each = np.divide(diff, stdev_array, out=np.zeros_like(diff), where=stdev_array != 0)

    weights = np.array(
        [
            1,
            1,
            1,
            1,
            1,
            1,
            (fga / FGA_NORM) if FGA_NORM else 1,
            (fta / FTA_NORM) if FTA_NORM else 1,
            -1,
        ],
        dtype=float,
    )
    total = float(np.sum(z_each * weights))
    return round(total, 2)


def compute_zscores(box_df: pd.DataFrame) -> pd.DataFrame:
    if box_df.empty:
        return box_df

    df = box_df.copy()

    first = df.get("firstName")
    last = df.get("familyName")
    if first is not None and last is not None:
        df["PLAYER_NAME"] = (
            first.fillna("").astype(str).str.strip()
            + " "
            + last.fillna("").astype(str).str.strip()
        ).str.strip()
    else:
        df["PLAYER_NAME"] = df.get("PLAYER_NAME", pd.Series(["" for _ in range(len(df))]))

    if "PLAYER_NAME" in df:
        empty_mask = df["PLAYER_NAME"].fillna("").eq("")
        if empty_mask.any():
            if "playerName" in df:
                df.loc[empty_mask, "PLAYER_NAME"] = (
                    df.loc[empty_mask, "playerName"].fillna("").astype(str)
                )

    for target, source in ZSCORE_SOURCE_MAP.items():
        df[target] = pd.to_numeric(df.get(source), errors="coerce").fillna(0.0)

    df["FGA"] = pd.to_numeric(df.get("fieldGoalsAttempted"), errors="coerce").fillna(0.0)
    df["FTA"] = pd.to_numeric(df.get("freeThrowsAttempted"), errors="coerce").fillna(0.0)

    z_scores: list[float] = []
    for _, row in df.iterrows():
        row_vals = [float(row.get(cat, 0.0)) for cat in NINE_CAT_ORDER]
        fga = float(row.get("FGA", 0.0))
        fta = float(row.get("FTA", 0.0))
        z_scores.append(_compute_zscore_row(row_vals, fga=fga, fta=fta))

    df["z_score"] = z_scores
    return df


def _clean_string(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    value_str = str(value).strip()
    return value_str or None


def build_bq_payload(box_df: pd.DataFrame, season: str) -> pd.DataFrame:
    if box_df.empty:
        return pd.DataFrame(columns=BQ_COLUMNS)

    df = box_df.copy()

    player_series = df.get("personId")
    if player_series is None:
        return pd.DataFrame(columns=BQ_COLUMNS)
    df["player_id"] = pd.to_numeric(player_series, errors="coerce").astype("Int64")
    df = df[df["player_id"].notna() & df["player_id"].ne(0)].copy()

    game_series = df.get("gameId")
    if game_series is None:
        return pd.DataFrame(columns=BQ_COLUMNS)
    df["game_id"] = pd.to_numeric(game_series, errors="coerce").astype("Int64")
    df = df[df["game_id"].notna()].copy()

    team_series = df.get("teamId")
    if team_series is None:
        return pd.DataFrame(columns=BQ_COLUMNS)
    df["team_id"] = pd.to_numeric(team_series, errors="coerce").astype("Int64")

    minutes_series = df.get("minutes")
    if minutes_series is None:
        df["minutes"] = 0.0
    else:
        df["minutes"] = minutes_series.apply(_minutes_to_float)
    df = df[df["minutes"] > 0].copy()

    for target, source in INT_SOURCE_MAP.items():
        source_series = df.get(source)
        if source_series is None:
            df[target] = pd.Series([0] * len(df), dtype="Int64", index=df.index)
        else:
            df[target] = pd.to_numeric(source_series, errors="coerce").fillna(0).astype("Int64")

    for target, source in FLOAT_SOURCE_MAP.items():
        source_series = df.get(source)
        if source_series is None:
            df[target] = pd.Series([np.nan] * len(df), index=df.index, dtype=float)
        else:
            df[target] = pd.to_numeric(source_series, errors="coerce")

    name_series = df.get("PLAYER_NAME")
    if name_series is None:
        name_series = pd.Series(["" for _ in range(len(df))], index=df.index)
    df["player_name"] = name_series.apply(_clean_string)

    for target, source in STRING_SOURCE_MAP.items():
        source_series = df.get(source)
        if source_series is None:
            df[target] = pd.Series([None] * len(df), index=df.index)
        else:
            df[target] = source_series.apply(_clean_string)

    df["game_date"] = pd.to_datetime(df.get("game_date")).dt.date
    df["season"] = season
    df["z_score"] = pd.to_numeric(df.get("z_score"), errors="coerce").fillna(0.0)

    ordered = df[BQ_COLUMNS].copy()
    return ordered.reset_index(drop=True)


def load_into_bigquery_tables(
    payload: pd.DataFrame,
    *,
    client: bigquery.Client,
    partitioned_table: str = PARTITIONED_TABLE,
    mirror_table: str | None = MIRROR_TABLE,
) -> None:
    if payload.empty:
        print("No rows to load into BigQuery.")
        return

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_dataframe(payload, partitioned_table, job_config=job_config)
    job.result()
    print(f"Loaded {len(payload)} rows into {partitioned_table}.")

    if mirror_table:
        mirror_job = client.load_table_from_dataframe(payload, mirror_table, job_config=job_config)
        mirror_job.result()
        print(f"Loaded {len(payload)} rows into {mirror_table}.")


def run_ingestion(
    target_date: datetime,
    *,
    project_id: str = DEFAULT_PROJECT,
    partitioned_table: str = PARTITIONED_TABLE,
    mirror_table: str | None = MIRROR_TABLE,
    skip_mirror: bool = False,
    dry_run: bool = False,
    game_ids: Iterable[str] | None = None,
    season_override: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
) -> pd.DataFrame:
    season = season_override or _season_from_date(target_date.date())

    if game_ids is None:
        game_ids = discover_game_ids(target_date, timeout=timeout, retries=retries)

    game_ids = [str(gid) for gid in game_ids]
    if not game_ids:
        print(f"No games found for {target_date.date()}.")
        return pd.DataFrame(columns=BQ_COLUMNS)

    combined = collect_boxscores(game_ids, target_date, timeout=timeout, retries=retries)
    if combined.empty:
        print("No box scores returned; nothing to ingest.")
        return pd.DataFrame(columns=BQ_COLUMNS)

    combined = compute_zscores(combined)
    payload = build_bq_payload(combined, season)
    if payload.empty:
        print("No rows after filtering; nothing to ingest.")
        return payload

    if dry_run:
        return payload

    client = bigquery.Client(project=project_id)
    load_into_bigquery_tables(
        payload,
        client=client,
        partitioned_table=partitioned_table,
        mirror_table=None if skip_mirror else mirror_table,
    )
    return payload


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest NBA player box scores for a specific date using BoxScoreTraditionalV3 "
            "and load them into BigQuery."
        )
    )
    parser.add_argument(
        "--date",
        type=lambda value: datetime.strptime(value, "%Y-%m-%d"),
        default=datetime.utcnow() - timedelta(days=1),
        help="Target date in YYYY-MM-DD format (defaults to yesterday).",
    )
    parser.add_argument(
        "--project",
        default=DEFAULT_PROJECT,
        help="Google Cloud project for BigQuery operations.",
    )
    parser.add_argument(
        "--table",
        default=PARTITIONED_TABLE,
        help="Fully-qualified BigQuery partitioned table to load (player_daily_game_stats_p).",
    )
    parser.add_argument(
        "--mirror-table",
        default=MIRROR_TABLE,
        help="Optional non-partitioned table to mirror results (player_daily_game_stats).",
    )
    parser.add_argument(
        "--skip-mirror",
        action="store_true",
        help="Skip loading into the non-partitioned mirror table.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and shape data without loading into BigQuery.",
    )
    parser.add_argument(
        "--game-ids",
        nargs="*",
        help="Optional explicit list of GAME_ID values to ingest (skips discovery).",
    )
    parser.add_argument(
        "--season",
        default=None,
        help="Optional season override (e.g. 2024-25).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Timeout in seconds for NBA Stats API requests.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help="Number of additional retries for Scoreboard and box score requests.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    payload = run_ingestion(
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

    if payload.empty:
        print("Ingestion completed with no rows.")
    else:
        print(f"Ingestion completed with {len(payload)} rows prepared.")
        if args.dry_run:
            with pd.option_context("display.max_rows", None, "display.max_columns", None):
                print(payload)


if __name__ == "__main__":
    main()
