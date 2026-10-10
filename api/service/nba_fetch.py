from __future__ import annotations

from datetime import date as _date, timedelta as _timedelta
from google.cloud import bigquery
import os
import pandas as pd
import numpy as np

PROJECT_ID = os.getenv("PROJECT_ID", "fantasy-survivor-app")
DATASET = "nba_data"
# Remapped view: exposes NBA player_id (INT64) instead of the SportsBlaze UUID.
# See nba_data.player_id_crosswalk. Switched 2026-09-26 to fix the provider
# ID-namespace break introduced in Feb 2026.
TABLE = "v_player_daily_game_stats"
TABLE_FQN = f"{PROJECT_ID}.{DATASET}.{TABLE}"

OWNERSHIP_CURRENT_FQN = (
    "fantasy-survivor-app.nba_data.v_player_ownership_latest"
)
OWNERSHIP_HISTORY_FQN = "fantasy-survivor-app.nba_data.player_ownership"

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
    table_alias: str | None = None,
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

    def _with_alias(expr: str) -> str:
        if not table_alias:
            return expr
        if " AS " in expr:
            original, alias = expr.split(" AS ", 1)
            return f"{table_alias}.{original.strip()} AS {alias.strip()}"
        return f"{table_alias}.{expr}"

    for name, required in requested:
        if name == "minutes":
            if minutes_column:
                if minutes_column == "minutes":
                    select_parts.append(_with_alias("minutes"))
                else:
                    select_parts.append(_with_alias(f"{minutes_column} AS minutes"))
            elif required:
                missing_required.append("minutes")
            continue

        if name in columns:
            select_parts.append(_with_alias(name))
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

    select_clause, minutes_column = _build_select_clause(
        client, requested_columns, table_alias="stats"
    )

    if not minutes_column:
        raise RuntimeError(
            f"BigQuery table {TABLE_FQN} does not expose a minutes column."
        )

    order_direction = "DESC" if mode == "best" else "ASC"
    min_roster_pct = 0.0 if mode == "best" else 20.0
    minutes_expr = f"stats.{minutes_column}"

    query = f"""
    SELECT
      {select_clause}
      , own.roster_pct
    FROM `{TABLE_FQN}` AS stats
    LEFT JOIN `fantasy-survivor-app.nba_data.v_player_ownership_latest` AS own
      ON stats.player_id = own.player_id  -- by ID: provider name spellings differ (accents, suffixes)
    WHERE stats.game_date = @date
      AND {minutes_expr} >= @min_minutes
      AND COALESCE(own.roster_pct, 0) >= @min_roster_pct
    ORDER BY stats.z_score {order_direction}
    LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date", "DATE", date),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("min_minutes", "FLOAT64", float(min_minutes)),
            bigquery.ScalarQueryParameter("min_roster_pct", "FLOAT64", min_roster_pct),
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()

    for column, _ in requested_columns:
        if column not in df.columns:
            df[column] = None

    if "roster_pct" not in df.columns:
        df["roster_pct"] = None

    output_columns = [name for name, _ in requested_columns] + ["roster_pct"]
    return safe_records(df[output_columns])

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


def get_player_roster_pct(
    mode: str,
    player_id: int | None = None,
):
    if mode not in {"current", "history"}:
        raise ValueError("mode must be either 'current' or 'history'")
    if player_id is None:
        raise ValueError("player_id is required")

    client = get_client()

    base_sql = """
    SELECT
      player_id,
      player_name,
      -- player_key does not exist on player_ownership / v_player_ownership_latest.
      -- Selected as NULL to preserve the existing API response shape.
      CAST(NULL AS STRING) AS player_key,
      roster_pct,
      snapshot_date
    FROM `{table}`
    WHERE player_id = @player_id
    {order_clause}
    """

    table = OWNERSHIP_CURRENT_FQN if mode == "current" else OWNERSHIP_HISTORY_FQN
    order_clause = "ORDER BY snapshot_date" if mode == "history" else ""

    query = base_sql.format(table=table, order_clause=order_clause)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()
    if df.empty:
        return []

    output_columns = [
        "player_id",
        "player_name",
        "player_key",
        "roster_pct",
        "snapshot_date",
    ]
    for column in output_columns:
        if column not in df.columns:
            df[column] = None

    return safe_records(df[output_columns])


YAHOO_OWNERSHIP_FQN = "fantasy-survivor-app.nba_data.player_ownership"
ESPN_OWNERSHIP_FQN = "fantasy-survivor-app.nba_data.player_ownership_espn"


def _pick_ownership_source(client: bigquery.Client, source: str, as_of) -> str:
    """Yahoo is primary, ESPN the fallback (Daniel, 2026-10-09): use Yahoo when its latest snapshot is <= 3 days old."""
    if source in ("yahoo", "espn"):
        return source
    row = list(client.query(f"SELECT MAX(snapshot_date) AS d FROM `{YAHOO_OWNERSHIP_FQN}`").result())[0]
    ref = as_of or _date.today()
    return "yahoo" if row.d is not None and row.d >= ref - _timedelta(days=3) else "espn"


def get_waiver_risers(
    days: int = 3,
    min_change: float = 5.0,
    max_roster_pct: float = 50.0,
    min_minutes: float = 24.0,
    limit: int = 10,
    source: str = "auto",
    as_of=None,
):
    """Players whose ownership is rising AND who are playing real minutes.

    Backtest (Yahoo ownership Oct 2025 - Feb 2026, players owned 5-50%): the rise itself predicts little. Among
    players averaging 24+ minutes, rising ownership gave a next-7-day Fantasy Value of 1.16 vs 1.02 for flat
    ownership, while "spike" risers on low or falling minutes fell to -0.58 (all-players baseline 0.42). The
    minutes filter is what carries the signal, so min_minutes defaults to 24 and the caller is told the source.
    """
    client = get_client()
    src = _pick_ownership_source(client, source, as_of)
    table = ESPN_OWNERSHIP_FQN if src == "espn" else YAHOO_OWNERSHIP_FQN
    own_cols = (
        "player_id, player_name, roster_pct, snapshot_date, pct_change, injury_status"
        if src == "espn"
        else "player_id, player_name, roster_pct, snapshot_date, CAST(NULL AS FLOAT64) AS pct_change, CAST(NULL AS STRING) AS injury_status"
    )
    query = f"""
    WITH own AS (SELECT {own_cols} FROM `{table}`),
    d0 AS (SELECT MAX(snapshot_date) AS d FROM own WHERE snapshot_date <= COALESCE(@as_of, CURRENT_DATE('America/New_York'))),
    prev_d AS (SELECT MAX(snapshot_date) AS d FROM own WHERE snapshot_date <= DATE_SUB((SELECT d FROM d0), INTERVAL @days DAY)),
    cur AS (SELECT * FROM own WHERE snapshot_date = (SELECT d FROM d0)),
    prv AS (
      SELECT player_id, roster_pct AS prev_pct FROM own
      WHERE snapshot_date = (SELECT d FROM prev_d) AND (SELECT d FROM prev_d) < (SELECT d FROM d0)
    ),
    games AS (
      SELECT player_id, game_date, minutes, z_score, pts, reb, ast, stl, blk, fg3m, fg_pct, team_abbr, position,
             ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
      FROM `{TABLE_FQN}`
      WHERE game_date <= (SELECT d FROM d0) AND game_date > DATE_SUB((SELECT d FROM d0), INTERVAL 30 DAY)
    ),
    form AS (
      SELECT player_id, COUNT(*) AS games_last5, AVG(minutes) AS min_last5, AVG(z_score) AS fantasy_value_last5,
             AVG(pts) AS pts, AVG(reb) AS reb, AVG(ast) AS ast, AVG(stl) AS stl, AVG(blk) AS blk,
             AVG(fg3m) AS fg3m, AVG(fg_pct) AS fg_pct, ANY_VALUE(team_abbr) AS team_abbr,
             ANY_VALUE(position) AS position, MAX(game_date) AS last_game
      FROM games WHERE rn <= 5 GROUP BY player_id
    ),
    prior AS (SELECT player_id, AVG(minutes) AS min_prior FROM games WHERE rn BETWEEN 6 AND 15 GROUP BY player_id)
    SELECT
      c.player_id, c.player_name, f.team_abbr, f.position,
      c.roster_pct, p.prev_pct AS roster_pct_prev,
      COALESCE(c.roster_pct - p.prev_pct, IF(@days = 1, c.pct_change, NULL)) AS change,
      IF(p.prev_pct IS NOT NULL, 'snapshots', IF(@days = 1 AND c.pct_change IS NOT NULL, 'espn_pct_change', NULL)) AS change_basis,
      c.snapshot_date, (SELECT d FROM prev_d) AS previous_snapshot_date,
      f.games_last5, f.min_last5, pr.min_prior, f.min_last5 - pr.min_prior AS minutes_trend,
      f.fantasy_value_last5, f.pts, f.reb, f.ast, f.stl, f.blk, f.fg3m, f.fg_pct, c.injury_status
    FROM cur c
    LEFT JOIN prv p USING (player_id)
    JOIN form f USING (player_id)
    LEFT JOIN prior pr USING (player_id)
    WHERE c.roster_pct <= @max_pct
      AND f.games_last5 >= 2
      AND f.min_last5 >= @min_minutes
      AND DATE_DIFF((SELECT d FROM d0), f.last_game, DAY) <= 10
      AND COALESCE(c.roster_pct - p.prev_pct, IF(@days = 1, c.pct_change, NULL)) >= @min_change
    ORDER BY change DESC, f.fantasy_value_last5 DESC
    LIMIT @limit
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("as_of", "DATE", as_of),
        bigquery.ScalarQueryParameter("days", "INT64", int(days)),
        bigquery.ScalarQueryParameter("min_change", "FLOAT64", float(min_change)),
        bigquery.ScalarQueryParameter("max_pct", "FLOAT64", float(max_roster_pct)),
        bigquery.ScalarQueryParameter("min_minutes", "FLOAT64", float(min_minutes)),
        bigquery.ScalarQueryParameter("limit", "INT64", int(limit)),
    ])
    df = client.query(query, job_config=cfg).to_dataframe()
    return src, safe_records(df)
