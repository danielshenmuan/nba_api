"""Ingest NBA box scores into player_daily_game_stats_p."""
from __future__ import annotations

import importlib
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery
from requests.exceptions import RequestException
from google.api_core.exceptions import NotFound

CURRENT_DIR = Path(__file__).resolve().parent


def _import_boxscore_utils():  # pragma: no cover - import helper for Cloud Run images
    candidates = ("jobs.boxscore_v3_utils", "boxscore_v3_utils")
    for name in candidates:
        try:
            if name == "boxscore_v3_utils" and str(CURRENT_DIR) not in sys.path:
                sys.path.insert(0, str(CURRENT_DIR))
            return importlib.import_module(name)
        except ModuleNotFoundError:
            continue
    raise ModuleNotFoundError(
        "Unable to import boxscore_v3_utils. Ensure it is packaged with the job image."
    )


_boxscore_utils = _import_boxscore_utils()

DEFAULT_RETRIES = _boxscore_utils.DEFAULT_RETRIES
DEFAULT_TIMEOUT = _boxscore_utils.DEFAULT_TIMEOUT
discover_game_ids = _boxscore_utils.discover_game_ids
load_traditional_boxscore = _boxscore_utils.load_traditional_boxscore
map_traditional_boxscore = _boxscore_utils.map_traditional_boxscore
minutes_to_float = _boxscore_utils.minutes_to_float

# ----------------------------
# Baseline stats (update each season if needed)
# ----------------------------
WEIGHTED_MEAN = [11.69, 4.32, 2.76, 0.75, 0.50, 1.28, 0.47, 0.75, 1.33]
WEIGHTED_STD = [7.23, 2.51, 2.09, 0.38, 0.45, 0.95, 0.082, 0.124, 0.85]

DEFAULT_PROJECT_ID = "fantasy-survivor-app"
PARTITIONED_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"
MIRROR_TABLE = "fantasy-survivor-app.nba_data.player_daily_game_stats"
LEAGUE_STATS_TABLE = "fantasy-survivor-app.nba_data.league_pg_stats_by_season"
LEAGUE_SQL_FILENAME = "create_league_pg_stats_by_season.sql"
BQ_LOCATION = "northamerica-northeast1"


def _season_from_date(d: date) -> str:
    year = d.year
    if d.month >= 10:
        return f"{year}-{(year + 1) % 100:02d}"
    return f"{year - 1}-{year % 100:02d}"


def compute_zscores(box: pd.DataFrame) -> pd.DataFrame:
    box = box.copy()

    def _min_to_int(value):
        if pd.isna(value):
            return None
        if isinstance(value, str) and ":" in value:
            try:
                return int(float(value.split(":")[0]))
            except ValueError:
                return None
        if isinstance(value, (int, float)):
            return int(value)
        return None

    box["MINUTES_INT"] = box["MINUTES"].apply(_min_to_int)

    stat_columns = [
        "PLAYER_NAME",
        "PTS",
        "REB",
        "AST",
        "STL",
        "BLK",
        "FG3M",
        "FG_PCT",
        "FT_PCT",
        "TO",
    ]
    nine = box[stat_columns].fillna(0)

    z_list: list[float] = []
    for i in range(len(nine)):
        vals = nine.iloc[i].tolist()[1:]
        diff = np.subtract(vals, WEIGHTED_MEAN)
        z = np.divide(diff, WEIGHTED_STD)
        fga = box["FGA"].iloc[i] if pd.notnull(box["FGA"].iloc[i]) else 0
        fta = box["FTA"].iloc[i] if pd.notnull(box["FTA"].iloc[i]) else 0
        adj = np.multiply(z, [1, 1, 1, 1, 1, 1, (fga / 20.0), (fta / 8.0), -1])
        z_list.append(round(float(np.sum(adj)), 3))

    box["Z_SCORE"] = z_list
    return box


def _locate_sql_file(filename: str) -> Path:
    """Locate a SQL file bundled with the job or in the repo tree."""

    search_paths: list[Path] = [CURRENT_DIR / "sql" / filename, CURRENT_DIR / filename]

    for parent in CURRENT_DIR.parents:
        search_paths.append(parent / "infra" / "bq" / "sql" / filename)

    for candidate in search_paths:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        f"Unable to locate {filename}. Ensure it is packaged with the deployment image."
    )


def collect_boxscores(
    game_ids: list[str],
    target_date: datetime,
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Fetch and normalize box scores for ``game_ids`` sequentially."""

    if not game_ids:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    seen: set[str] = set()
    request_timeout = timeout or DEFAULT_TIMEOUT
    per_game_retries = max(1, DEFAULT_RETRIES)

    for raw_game_id in game_ids:
        game_id = str(raw_game_id).strip()
        if not game_id or game_id in seen:
            continue
        seen.add(game_id)

        raw: pd.DataFrame | None = None
        last_error: Exception | None = None
        for attempt in range(per_game_retries):
            try:
                raw = load_traditional_boxscore(
                    game_id,
                    retries=1,
                    timeout=request_timeout,
                )
            except RequestException as exc:
                last_error = exc
                continue

            if raw is None or raw.empty:
                last_error = None
                raw = None
                break

            last_error = None
            break

        if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
            message = (
                f"Skipping {game_id}: box score not available yet."
                if last_error is None
                else f"Skipping {game_id}: {last_error}"
            )
            print(message)
            continue

        try:
            mapped = map_traditional_boxscore(raw, game_id, target_date.date())
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"Skipping {game_id}: failed to map box score ({exc}).")
            continue

        if mapped.empty:
            print(f"Skipping {game_id}: box score missing required player data.")
            continue

        frames.append(mapped)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def build_bq_payload(frame: pd.DataFrame, season_value: str | None = None) -> pd.DataFrame:
    """Shape a mapped + z-scored frame into the BigQuery schema."""

    if frame.empty:
        return pd.DataFrame()

    df = frame.copy()
    df["PLAYER_ID"] = pd.to_numeric(df["PLAYER_ID"], errors="coerce")
    df["GAME_ID_INT"] = pd.to_numeric(df["GAME_ID"], errors="coerce")
    df["TEAM_ID_INT"] = pd.to_numeric(df["TEAM_ID"], errors="coerce")
    df["MINUTES_FLOAT"] = df["MINUTES"].apply(minutes_to_float)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"]).dt.date

    if season_value is not None:
        df["season"] = season_value
    else:
        df["season"] = df["GAME_DATE"].apply(_season_from_date)

    df = df[df["PLAYER_ID"].notna()]
    df = df[df["GAME_ID_INT"].notna()]
    df = df[df["MINUTES_FLOAT"].notna() & (df["MINUTES_FLOAT"] > 0)]

    if df.empty:
        return pd.DataFrame()

    def _string_series(series: pd.Series) -> pd.Series:
        return series.astype("string").replace({pd.NA: None}).astype(object)

    int_map = {
        "fgm": "FGM",
        "fga": "FGA",
        "fg3m": "FG3M",
        "fg3a": "FG3A",
        "ftm": "FTM",
        "fta": "FTA",
        "pts": "PTS",
        "reb": "REB",
        "ast": "AST",
        "stl": "STL",
        "blk": "BLK",
        "turnovers": "TO",
        "pf": "PF",
        "dreb": "DREB",
        "oreb": "OREB",
        "plus_minus": "PLUS_MINUS",
    }

    float_map = {
        "fg_pct": "FG_PCT",
        "fg3_pct": "FG3_PCT",
        "ft_pct": "FT_PCT",
    }

    shaped: dict[str, pd.Series] = {
        "game_date": df["GAME_DATE"],
        "game_id": df["GAME_ID_INT"].astype("Int64"),
        "player_id": df["PLAYER_ID"].astype("Int64"),
        "player_name": df["PLAYER_NAME"].astype(str),
        "team_id": df["TEAM_ID_INT"].astype("Int64"),
        "team_abbr": _string_series(df["TEAM_ABBREVIATION"]),
        "team_city": _string_series(df["TEAM_CITY"]),
        "team_name": _string_series(df["TEAM_NAME"]),
        "team_slug": _string_series(df["TEAM_SLUG"]),
        "position": _string_series(df["POSITION"]),
        "comment": _string_series(df["COMMENT"]),
        "jersey_num": _string_series(df["JERSEY_NUM"]),
        "minutes": df["MINUTES_FLOAT"].astype(float),
    }

    for dest, src in int_map.items():
        values = pd.to_numeric(df[src], errors="coerce")
        shaped[dest] = values.round().astype("Int64")

    for dest, src in float_map.items():
        shaped[dest] = pd.to_numeric(df[src], errors="coerce").astype(float)

    shaped["z_score"] = pd.to_numeric(df["Z_SCORE"], errors="coerce").astype(float)
    shaped["season"] = df["season"].astype(str)

    ordered_columns = [
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

    out = pd.DataFrame(shaped, columns=ordered_columns)
    return out.reset_index(drop=True)


def _rows_exist_for_date(
    table_ref: str, day: date, *, client: bigquery.Client
) -> bool:
    query = f"SELECT COUNT(1) AS row_count FROM `{table_ref}` WHERE game_date = @target"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("target", "DATE", day)]
    )
    job = client.query(query, job_config=job_config, location=BQ_LOCATION)
    result = list(job.result())
    if not result:
        return False
    return result[0].get("row_count", 0) > 0


def _league_stats_refreshed_today(client: bigquery.Client) -> bool:
    try:
        table = client.get_table(LEAGUE_STATS_TABLE)
    except NotFound:
        return False

    modified = getattr(table, "modified", None)
    if modified is None:
        return False

    if modified.tzinfo is None:
        modified = modified.replace(tzinfo=timezone.utc)
    else:
        modified = modified.astimezone(timezone.utc)

    return modified.date() >= datetime.now(timezone.utc).date()


def run_ingestion(
    target_date: datetime | None = None,
    season: str | None = None,
    *,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    if target_date is None:
        target_date = datetime.today() - timedelta(days=1)

    season_value = season or _season_from_date(target_date.date())

    try:
        game_ids = discover_game_ids(
            target_date,
            retries=retries,
            timeout=timeout,
        )
    except RequestException as exc:
        print(f"Failed to load ScoreboardV2 for {target_date.date()}: {exc}")
        return pd.DataFrame()

    if not game_ids:
        print(f"No games found on {target_date.date()}.")
        return pd.DataFrame()

    combined = collect_boxscores(game_ids, target_date, timeout=timeout)
    if combined.empty:
        print("No player stats returned; nothing to load.")
        return pd.DataFrame()

    combined = compute_zscores(combined)
    payload = build_bq_payload(combined, season_value)
    return payload


def refresh_league_pg_stats(
    *, client: bigquery.Client | None = None, project_id: str = DEFAULT_PROJECT_ID
) -> None:
    bq_client = client or bigquery.Client(project=project_id)
    sql_path = _locate_sql_file(LEAGUE_SQL_FILENAME)
    job = bq_client.query(sql_path.read_text(), location=BQ_LOCATION)
    job.result()
    print("Refreshed league_pg_stats_by_season ✅")


def load_into_bigquery_tables(
    df: pd.DataFrame,
    *,
    client: bigquery.Client | None = None,
    project_id: str = DEFAULT_PROJECT_ID,
    partitioned_table: str = PARTITIONED_TABLE,
    mirror_table: str | None = MIRROR_TABLE,
    delete_mirror_dates: bool = True,
) -> None:
    """Append ``df`` into the partitioned table and optional mirror table."""

    if df.empty:
        print("No rows provided for BigQuery load.")
        return

    bq_client = client or bigquery.Client(project=project_id)
    load_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    def _align_to_schema(
        frame: pd.DataFrame, table_ref: str
    ) -> pd.DataFrame:
        table_obj = bq_client.get_table(table_ref)
        schema_columns = [field.name for field in table_obj.schema]
        schema_set = set(schema_columns)

        if "min" in schema_set:
            raise ValueError(
                "BigQuery table"
                f" {table_obj.full_table_id} still contains a 'min' column."
                " Rename it to 'minutes' so the ingestion payload aligns with the"
                " updated schema."
            )

        if "minutes" not in schema_set:
            raise ValueError(
                "BigQuery table"
                f" {table_obj.full_table_id} is missing the required 'minutes' column."
                " Update the warehouse schema to use 'minutes' instead of 'min'."
            )

        aligned = frame.copy()
        extra_cols = [col for col in aligned.columns if col not in schema_columns]
        if extra_cols:
            print(
                "Dropping columns not present in"
                f" {table_obj.full_table_id}: {sorted(extra_cols)}"
            )
            aligned = aligned.drop(columns=extra_cols)

        for column in schema_columns:
            if column not in aligned.columns:
                aligned[column] = None

        return aligned[schema_columns]

    partition_frame = _align_to_schema(df, partitioned_table)
    job = bq_client.load_table_from_dataframe(
        partition_frame, partitioned_table, job_config=load_config
    )
    job.result()
    print(f"Loaded {len(df)} rows into {partitioned_table}.")

    if not mirror_table:
        return

    if delete_mirror_dates:
        non_null_dates = [
            pd.to_datetime(value).date()
            for value in df["game_date"].dropna().unique().tolist()
        ]
        if non_null_dates:
            delete_sql = f"DELETE FROM `{mirror_table}` WHERE game_date IN UNNEST(@dates)"
            delete_job = bq_client.query(
                delete_sql,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter("dates", "DATE", non_null_dates)
                    ]
                ),
            )
            delete_job.result()

    mirror_frame = _align_to_schema(df, mirror_table)
    mirror_job = bq_client.load_table_from_dataframe(
        mirror_frame, mirror_table, job_config=load_config
    )
    mirror_job.result()
    print(f"Loaded {len(df)} rows into {mirror_table}.")


if __name__ == "__main__":
    target_date = datetime.today() - timedelta(days=1)
    client = bigquery.Client(project=DEFAULT_PROJECT_ID)
    partition_date = target_date.date()

    try:
        rows_exist = _rows_exist_for_date(
            PARTITIONED_TABLE, partition_date, client=client
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        print(
            "Unable to determine if rows already exist for",
            f"{partition_date}: {exc}. Proceeding with ingestion.",
        )
        rows_exist = False

    df = pd.DataFrame()

    if rows_exist:
        print(
            f"{PARTITIONED_TABLE} already has rows for {partition_date}; "
            "skipping ingestion."
        )
    else:
        df = run_ingestion(target_date)

        if df.empty:
            print("No rows to load.")
        else:
            load_into_bigquery_tables(
                df,
                client=client,
                project_id=DEFAULT_PROJECT_ID,
                partitioned_table=PARTITIONED_TABLE,
                mirror_table=MIRROR_TABLE,
            )

    try:
        already_refreshed = _league_stats_refreshed_today(client)
    except Exception as exc:  # pragma: no cover - defensive logging
        print(
            "Unable to determine if league_pg_stats_by_season was refreshed today:",
            f"{exc}. Running refresh.",
        )
        already_refreshed = False

    if already_refreshed:
        print("league_pg_stats_by_season already refreshed today; skipping refresh.")
    else:
        refresh_league_pg_stats(client=client)
