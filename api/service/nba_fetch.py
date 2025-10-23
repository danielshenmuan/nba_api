from __future__ import annotations

from google.cloud import bigquery
import os
import pandas as pd
import numpy as np

PROJECT_ID = os.getenv("PROJECT_ID", "fantasy-survivor-app")
DATASET = "nba_data"
TABLE = "player_daily_game_stats_p"
TABLE_FQN = f"{PROJECT_ID}.{DATASET}.{TABLE}"

_TABLE_COLUMN_CACHE: set[str] | None = None


def _available_columns(client: bigquery.Client) -> set[str]:
    """Return and cache the set of columns available on the stats table."""

    global _TABLE_COLUMN_CACHE
    if _TABLE_COLUMN_CACHE is None:
        table = client.get_table(TABLE_FQN)
        _TABLE_COLUMN_CACHE = {field.name for field in table.schema}
    return _TABLE_COLUMN_CACHE


def _minutes_source(columns: set[str]) -> str | None:
    if "minutes" in columns:
        return "minutes"
    if "min" in columns:
        return "min"
    return None


def _build_select_clause(
    client: bigquery.Client,
    requested: list[tuple[str, bool]],
) -> tuple[str, str | None]:
    """Return a SELECT clause for the requested columns.

    Args:
        client: BigQuery client used to introspect the table schema.
        requested: Iterable of ``(column_name, required)`` pairs representing
            the desired output order. ``minutes`` is treated specially so it can
            alias either ``minutes`` or legacy ``min``.

    Returns:
        Tuple containing the ``SELECT`` clause and the underlying minutes
        column name (if available).
    """

    columns = _available_columns(client)
    minutes_column = _minutes_source(columns)

    select_parts: list[str] = []
    missing_required: list[str] = []

    for name, required in requested:
        if name == "minutes":
            if minutes_column:
                if minutes_column == "minutes":
                    select_parts.append("minutes")
                else:
                    select_parts.append(f"{minutes_column} AS minutes")
            elif required:
                missing_required.append("minutes")
            continue

        if name in columns:
            select_parts.append(name)
        elif required:
            missing_required.append(name)

    if missing_required:
        raise RuntimeError(
            "BigQuery table "
            f"{TABLE_FQN} is missing required columns: {', '.join(sorted(missing_required))}."
        )

    if not select_parts:
        raise RuntimeError(
            f"No columns available to select from {TABLE_FQN}. Check the table schema."
        )

    select_clause = ",\n      ".join(select_parts)
    return select_clause, minutes_column

def get_client():
    return bigquery.Client(project=PROJECT_ID)

def safe_records(df: pd.DataFrame):
    """
    Convert NaN/Inf values into None so JSON serialization won't fail.
    """
    df = df.replace([np.inf, -np.inf], np.nan)       # replace +/- inf with NaN
    return df.where(df.notnull(), None).to_dict("records")  # NaN -> None

def get_daily_leaders(date, limit=10, mode="best", min_minutes: float = 0):
    client = get_client()

    requested_columns: list[tuple[str, bool]] = [
        ("player_id", True),
        ("player_name", True),
        ("game_id", True),
        ("game_date", True),
        ("team_id", False),
        ("team_abbr", False),
        ("team_city", False),
        ("team_name", False),
        ("team_slug", False),
        ("position", False),
        ("comment", False),
        ("jersey_num", False),
        ("minutes", True),
        ("fgm", False),
        ("fga", False),
        ("fg_pct", False),
        ("fg3m", False),
        ("fg3a", False),
        ("fg3_pct", False),
        ("ftm", False),
        ("fta", False),
        ("ft_pct", False),
        ("pts", True),
        ("reb", True),
        ("dreb", False),
        ("oreb", False),
        ("ast", True),
        ("stl", True),
        ("blk", True),
        ("turnovers", True),
        ("pf", False),
        ("plus_minus", False),
        ("z_score", True),
    ]

    select_clause, minutes_column = _build_select_clause(client, requested_columns)

    if not minutes_column:
        raise RuntimeError(
            f"BigQuery table {TABLE_FQN} does not expose a minutes column."
        )

    order_direction = "DESC" if mode == "best" else "ASC"

    query = f"""
    SELECT
      {select_clause}
    FROM `{TABLE_FQN}`
    WHERE game_date = @date
      AND {minutes_column} >= @min_minutes
    ORDER BY z_score {order_direction}
    LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date", "DATE", date),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("min_minutes", "FLOAT64", float(min_minutes)),
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()

    for column, _ in requested_columns:
        if column not in df.columns:
            df[column] = None

    return safe_records(df[[name for name, _ in requested_columns]])

def get_player_time_series(player_id, start_date=None, end_date=None):
    client = get_client()
    conditions = ["player_id = @pid"]
    params = [bigquery.ScalarQueryParameter("pid", "INT64", player_id)]

    if start_date:
        conditions.append("game_date >= @start")
        params.append(bigquery.ScalarQueryParameter("start", "DATE", start_date))
    if end_date:
        conditions.append("game_date <= @end")
        params.append(bigquery.ScalarQueryParameter("end", "DATE", end_date))

    where_clause = " AND ".join(conditions)

    requested_columns: list[tuple[str, bool]] = [
        ("game_date", True),
        ("game_id", True),
        ("team_id", False),
        ("team_abbr", False),
        ("team_name", False),
        ("team_slug", False),
        ("position", False),
        ("comment", False),
        ("jersey_num", False),
        ("minutes", True),
        ("fgm", False),
        ("fga", False),
        ("fg_pct", False),
        ("fg3m", False),
        ("fg3a", False),
        ("fg3_pct", False),
        ("ftm", False),
        ("fta", False),
        ("ft_pct", False),
        ("pts", True),
        ("reb", True),
        ("dreb", False),
        ("oreb", False),
        ("ast", True),
        ("stl", True),
        ("blk", True),
        ("turnovers", True),
        ("pf", False),
        ("plus_minus", False),
        ("z_score", True),
    ]

    select_clause, _ = _build_select_clause(client, requested_columns)

    query = f"""
    SELECT
      {select_clause}
    FROM `{TABLE_FQN}`
    WHERE {where_clause}
    ORDER BY game_date ASC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = client.query(query, job_config=job_config).to_dataframe()

    for column, _ in requested_columns:
        if column not in df.columns:
            df[column] = None

    return safe_records(df[[name for name, _ in requested_columns]])
