"""Helpers for normalizing BoxScoreTraditionalV3 payloads."""
from __future__ import annotations

import re
import time
from datetime import date, datetime
from typing import Iterable

import pandas as pd
from nba_api.live.nba.endpoints import scoreboard as live_scoreboard
from nba_api.stats.endpoints import BoxScoreTraditionalV3
from nba_api.stats.library.http import NBAStatsHTTP
from requests.exceptions import RequestException

# Pattern that captures ISO-8601 style minute strings such as ``PT33M12.00S``.
_MINUTES_ISO_PATTERN = re.compile(
    r"PT(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?"
)

DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3
_FINAL_STATUS_ID = "3"


def discover_game_ids(
    target_date: datetime,
    *,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
    include_non_final: bool = False,
) -> list[str]:
    """Return game IDs for ``target_date`` using the ScoreboardV2 endpoint."""

    params = {
        "GameDate": target_date.strftime("%m/%d/%Y"),
        "DayOffset": 0,
        "LeagueID": "00",
    }

    def _normalize_game_id(raw_value) -> str | None:
        if not raw_value:
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

    for attempt in range(retries):
        try:
            response = NBAStatsHTTP().send_api_request(
                endpoint="scoreboardv2",
                parameters=params,
                timeout=timeout,
            )
            data = response.get_normalized_dict()
            headers = data.get("GameHeader", []) if data else []
            game_ids: set[str] = set()
            for row in headers:
                gid_str = _normalize_game_id(row.get("GAME_ID"))
                if not gid_str:
                    continue
                if not include_non_final:
                    status_raw = row.get("GAME_STATUS_ID")
                    if status_raw is None:
                        continue
                    status_str = str(status_raw).strip()
                    if status_str != _FINAL_STATUS_ID:
                        try:
                            status_normalized = str(int(float(status_str)))
                        except (TypeError, ValueError):
                            status_normalized = ""
                        if status_normalized != _FINAL_STATUS_ID:
                            continue
                game_ids.add(gid_str)
            if game_ids:
                return sorted(game_ids)
        except Exception as exc:  # noqa: BLE001 - stats SDK raises generic exceptions
            if isinstance(exc, KeyboardInterrupt):
                raise
            if attempt == retries - 1:
                raise RequestException(
                    f"Failed to load ScoreboardV2 data: {exc}"
                ) from exc
            time.sleep(1)

    # Fallback to the live scoreboard feed, which often exposes schedules earlier.
    try:
        board = live_scoreboard.ScoreBoard(
            game_date=target_date.strftime("%Y-%m-%d"), timeout=timeout
        )
        board.get_request()
        board_data = board.get_dict()
    except Exception as exc:  # noqa: BLE001 - defensive fallback
        raise RequestException(
            f"Failed to load live scoreboard data: {exc}"
        ) from exc

    games = (board_data or {}).get("scoreboard", {}).get("games", [])
    collected: list[str] = []
    for game in games:
        gid_str = _normalize_game_id(game.get("gameId"))
        if not gid_str:
            continue
        if not include_non_final:
            status = str(game.get("gameStatus", "")).strip()
            if status and status != _FINAL_STATUS_ID:
                continue
        collected.append(gid_str)

    return sorted(dict.fromkeys(collected))


def load_traditional_boxscore(
    game_id: str,
    *,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Fetch ``BoxScoreTraditionalV3`` data for ``game_id`` as a DataFrame."""

    for attempt in range(retries):
        try:
            box = BoxScoreTraditionalV3(game_id=game_id, timeout=timeout)
            try:
                frames = box.get_data_frames()
            except AttributeError as attr_err:
                # When the stats API returns an empty body the SDK calls ``.keys()``
                # on ``None``. Treat that scenario the same as "no data yet".
                if "'NoneType' object has no attribute 'keys'" in str(attr_err):
                    return pd.DataFrame()
                raise
            if not frames:
                return pd.DataFrame()
            return frames[0].copy()
        except Exception as exc:  # noqa: BLE001 - stats SDK raises generic exceptions
            if isinstance(exc, KeyboardInterrupt):
                raise
            if attempt == retries - 1:
                raise RequestException(
                    f"Failed to load BoxScoreTraditionalV3 for {game_id}: {exc}"
                ) from exc
            time.sleep(1)

    return pd.DataFrame()


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


def minutes_to_float(value) -> float | None:
    """Convert an ``MM:SS`` style value into decimal minutes."""

    if value is None or pd.isna(value):
        return None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if ":" not in s:
            s = _normalize_minutes(s) or ""
        if ":" in s:
            mins, secs = s.split(":", 1)
            try:
                minutes = int(float(mins))
                seconds = float(secs)
            except ValueError:
                return None
            return minutes + seconds / 60.0
        try:
            return float(s)
        except ValueError:
            return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
    "reboundsOffensive": "OREB",
    "reboundsDefensive": "DREB",
    "foulsPersonal": "PF",
}


_STRING_COLUMN_MAP: dict[str, str] = {
    "teamCity": "TEAM_CITY",
    "teamName": "TEAM_NAME",
    "teamSlug": "TEAM_SLUG",
    "position": "POSITION",
    "comment": "COMMENT",
    "jerseyNum": "JERSEY_NUM",
}


_EXPECTED_COLUMNS: Iterable[str] = (
    "GAME_ID",
    "PLAYER_ID",
    "PLAYER_NAME",
    "TEAM_ID",
    "TEAM_ABBREVIATION",
    "TEAM_CITY",
    "TEAM_NAME",
    "TEAM_SLUG",
    "POSITION",
    "COMMENT",
    "JERSEY_NUM",
    "MINUTES",
    "FGM",
    "FGA",
    "FG_PCT",
    "FG3M",
    "FG3A",
    "FG3_PCT",
    "FTM",
    "FTA",
    "FT_PCT",
    "OREB",
    "DREB",
    "REB",
    "AST",
    "STL",
    "BLK",
    "TO",
    "PF",
    "PTS",
    "PLUS_MINUS",
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
        df["TEAM_ABBREVIATION"] = team_series.astype("string").str.upper()
    else:
        df["TEAM_ABBREVIATION"] = pd.NA

    team_id_series = df.get("teamId")
    if team_id_series is not None:
        df["TEAM_ID"] = pd.to_numeric(team_id_series, errors="coerce")
    else:
        df["TEAM_ID"] = pd.NA

    for source, target in _STRING_COLUMN_MAP.items():
        series = df.get(source)
        if series is None:
            df[target] = pd.NA
        else:
            df[target] = series.astype("string")

    minutes_series = df.get("minutes")
    if minutes_series is not None:
        df["MINUTES"] = minutes_series.apply(_normalize_minutes)
    else:
        df["MINUTES"] = None

    for source, target in _COLUMN_SOURCE_MAP.items():
        series = df.get(source)
        if series is None:
            df[target] = pd.NA
        else:
            df[target] = pd.to_numeric(series, errors="coerce")

    plus_minus_series = df.get("plusMinusPoints")
    if plus_minus_series is not None:
        df["PLUS_MINUS"] = pd.to_numeric(plus_minus_series, errors="coerce")
    else:
        df["PLUS_MINUS"] = pd.NA

    df["GAME_ID"] = str(game_id)
    df["GAME_DATE"] = game_date

    for column in _EXPECTED_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    return df.loc[:, list(_EXPECTED_COLUMNS)].reset_index(drop=True)


__all__ = [
    "DEFAULT_RETRIES",
    "DEFAULT_TIMEOUT",
    "discover_game_ids",
    "load_traditional_boxscore",
    "map_traditional_boxscore",
    "minutes_to_float",
]
