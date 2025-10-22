"""Helpers for normalizing BoxScoreTraditionalV3 payloads."""
from __future__ import annotations

import re
from datetime import date
from typing import Iterable

import pandas as pd

# Pattern that captures ISO-8601 style minute strings such as ``PT33M12.00S``.
_MINUTES_ISO_PATTERN = re.compile(
    r"PT(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?"
)


def _normalize_minutes(value) -> str | None:
    """Return a ``MM:SS`` string for the provided minute representation."""
    if value is None or pd.isna(value):
        return None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.startswith("PT"):
            match = _MINUTES_ISO_PATTERN.fullmatch(s)
            if match:
                minutes = int(match.group("minutes") or 0)
                raw_seconds = match.group("seconds") or "0"
                seconds = int(float(raw_seconds))
                return f"{minutes:d}:{seconds:02d}"
            return None
        if ":" in s:
            mins, secs = s.split(":", 1)
            try:
                minutes = int(float(mins))
                seconds_part = secs.split(".")[0]
                seconds = int(float(seconds_part))
                return f"{minutes:d}:{seconds:02d}"
            except ValueError:
                return None
        try:
            minutes = int(float(s))
        except ValueError:
            return None
        return f"{minutes:d}:00"

    try:
        minutes = int(float(value))
    except (TypeError, ValueError):
        return None
    return f"{minutes:d}:00"


_COLUMN_SOURCE_MAP: dict[str, str] = {
    "fieldGoalsMade": "FGM",
    "fieldGoalsAttempted": "FGA",
    "fieldGoalsPercentage": "FG_PCT",
    "threePointersMade": "FG3M",
    "threePointersAttempted": "FG3A",
    "threePointersPercentage": "FG3_PCT",
    "freeThrowsMade": "FTM",
    "freeThrowsAttempted": "FTA",
    "freeThrowsPercentage": "FT_PCT",
    "reboundsTotal": "REB",
    "assists": "AST",
    "steals": "STL",
    "blocks": "BLK",
    "turnovers": "TO",
    "points": "PTS",
}


_EXPECTED_COLUMNS: Iterable[str] = (
    "GAME_ID",
    "PLAYER_ID",
    "PLAYER_NAME",
    "TEAM_ABBREVIATION",
    "MIN",
    "FGM",
    "FGA",
    "FG_PCT",
    "FG3M",
    "FG3A",
    "FG3_PCT",
    "FTM",
    "FTA",
    "FT_PCT",
    "REB",
    "AST",
    "STL",
    "BLK",
    "TO",
    "PTS",
    "GAME_DATE",
)


def map_traditional_boxscore(
    raw_df: pd.DataFrame, game_id: str, game_date: date
) -> pd.DataFrame:
    """Return a DataFrame aligned with ``compute_zscores`` expectations."""
    if raw_df.empty:
        return pd.DataFrame(columns=_EXPECTED_COLUMNS)

    df = raw_df.copy()

    # Filter out team-total rows that aggregate across franchises.
    if "teamTricode" in df.columns:
        df = df[df["teamTricode"].astype(str).str.upper() != "TOT"]

    person_ids = df.get("personId")
    if person_ids is None:
        return pd.DataFrame(columns=_EXPECTED_COLUMNS)

    df["PLAYER_ID"] = pd.to_numeric(person_ids, errors="coerce")
    df = df[df["PLAYER_ID"].notna()]
    if df.empty:
        return pd.DataFrame(columns=_EXPECTED_COLUMNS)

    first = df.get("firstName", "").fillna("").astype(str).str.strip()
    last = df.get("familyName", "").fillna("").astype(str).str.strip()
    names = (first + " " + last).str.strip()

    fallback_name = df.get("name")
    if fallback_name is not None:
        fallback = fallback_name.fillna("").astype(str).str.strip()
        names = names.mask(names == "", fallback)

    fallback_player_name = df.get("playerName")
    if fallback_player_name is not None:
        fallback = fallback_player_name.fillna("").astype(str).str.strip()
        names = names.mask(names == "", fallback)

    df["PLAYER_NAME"] = names
    df = df[df["PLAYER_NAME"].astype(str).str.strip() != ""]

    team_series = df.get("teamTricode")
    if team_series is not None:
        df["TEAM_ABBREVIATION"] = team_series.fillna("").astype(str).str.upper()
    else:
        df["TEAM_ABBREVIATION"] = ""

    minutes_series = df.get("minutes")
    if minutes_series is not None:
        df["MIN"] = minutes_series.apply(_normalize_minutes)
    else:
        df["MIN"] = None

    for source, target in _COLUMN_SOURCE_MAP.items():
        series = df.get(source)
        if series is None:
            df[target] = pd.NA
        else:
            df[target] = pd.to_numeric(series, errors="coerce")

    df["GAME_ID"] = str(game_id)
    df["GAME_DATE"] = game_date

    for column in _EXPECTED_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    return df.loc[:, list(_EXPECTED_COLUMNS)].reset_index(drop=True)
