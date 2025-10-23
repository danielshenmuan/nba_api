"""Ingest NBA box scores into player_daily_game_stats_p."""
from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery
from requests.exceptions import RequestException

from jobs.boxscore_v3_utils import (
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
    discover_game_ids,
    load_traditional_boxscore,
    map_traditional_boxscore,
    minutes_to_float,
)

# ----------------------------
# Baseline stats (update each season if needed)
# ----------------------------
WEIGHTED_MEAN = [11.69, 4.32, 2.76, 0.75, 0.50, 1.28, 0.47, 0.75, 1.33]
WEIGHTED_STD = [7.23, 2.51, 2.09, 0.38, 0.45, 0.95, 0.082, 0.124, 0.85]


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


def _collect_boxscores(
    game_ids: list[str],
    target_date: datetime,
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
            print(f"Skipping {game_id}: box score payload not available yet.")
            continue

        mapped = map_traditional_boxscore(raw, game_id, target_date.date())
        if mapped.empty:
            print(f"Skipping {game_id}: box score missing required player data.")
            continue

        frames.append(mapped)
        time.sleep(0.3)

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
        "z_score",
        "season",
    ]

    out = pd.DataFrame(shaped, columns=ordered_columns)
    return out.reset_index(drop=True)


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
        game_ids = discover_game_ids(target_date, retries=retries, timeout=timeout)
    except RequestException as exc:
        print(f"Failed to load ScoreboardV2 for {target_date.date()}: {exc}")
        return pd.DataFrame()

    if not game_ids:
        print(f"No games found on {target_date.date()}.")
        return pd.DataFrame()

    combined = _collect_boxscores(game_ids, target_date, retries=retries, timeout=timeout)
    if combined.empty:
        print("No player stats returned; nothing to load.")
        return pd.DataFrame()

    combined = compute_zscores(combined)
    payload = build_bq_payload(combined, season_value)
    return payload


def refresh_league_pg_stats() -> None:
    client = bigquery.Client(project="fantasy-survivor-app")
    sql_path = Path(__file__).resolve().parents[1] / "infra" / "bq" / "sql" / "create_league_pg_stats_by_season.sql"
    job = client.query(sql_path.read_text(), location="northamerica-northeast1")
    job.result()
    print("Refreshed league_pg_stats_by_season ✅")


if __name__ == "__main__":
    target_date = datetime.today() - timedelta(days=1)
    df = run_ingestion(target_date)

    if df.empty:
        print("No rows to load.")
    else:
        client = bigquery.Client(project="fantasy-survivor-app")
        table = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"

        job = client.load_table_from_dataframe(
            df,
            table,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        )
        job.result()
        print(f"Loaded {len(df)} rows into {table} for {target_date.date()}")

        refresh_league_pg_stats()
