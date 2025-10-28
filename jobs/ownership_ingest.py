"""Yahoo roster ownership ingestion job."""
from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from google.cloud import bigquery
from yfpy.models import Player
from yfpy.query import YahooFantasySportsQuery

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


@dataclass
class OwnershipConfig:
    project_id: str
    dataset: str
    partition_table: str
    mirror_table: str
    view_name: str
    refresh_view: bool
    snapshot_date: date
    league_id: str
    game_code: str
    game_id: Optional[int]
    dry_run: bool
    delete_existing: bool
    yahoo_consumer_key: str
    yahoo_consumer_secret: str
    yahoo_access_token: Dict[str, Any]

    @property
    def partition_table_id(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.partition_table}"

    @property
    def mirror_table_id(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.mirror_table}"

    @property
    def view_id(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.view_name}"


class OwnershipIngestError(RuntimeError):
    """Raised when mandatory configuration or data is missing."""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Yahoo roster ownership percentages into BigQuery")
    parser.add_argument("--date", type=_parse_date, help="Snapshot date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--project", help="GCP project ID. Defaults to PROJECT_ID/GOOGLE_CLOUD_PROJECT env vars.")
    parser.add_argument("--dataset", default=os.environ.get("OWNERSHIP_DATASET", "nba_data"), help="BigQuery dataset name")
    parser.add_argument(
        "--partition-table",
        default=os.environ.get("OWNERSHIP_PARTITION_TABLE", "player_ownership_p"),
        help="Partitioned BigQuery table name",
    )
    parser.add_argument(
        "--mirror-table",
        default=os.environ.get("OWNERSHIP_MIRROR_TABLE", "player_ownership"),
        help="Non-partitioned BigQuery table name",
    )
    parser.add_argument(
        "--view-name",
        default=os.environ.get("OWNERSHIP_VIEW_NAME", "player_ownership_latest"),
        help="View name to refresh after load",
    )
    parser.add_argument("--no-view-refresh", action="store_true", help="Skip recreating the latest ownership view")
    parser.add_argument("--league-id", help="Yahoo Fantasy league ID (defaults to YAHOO_LEAGUE_ID env var)")
    parser.add_argument("--game-code", default=os.environ.get("YAHOO_GAME_CODE", "nba"), help="Yahoo game code")
    parser.add_argument("--game-id", type=int, help="Yahoo game id (optional override)")
    parser.add_argument("--dry-run", action="store_true", help="Print the dataframe instead of loading BigQuery")
    parser.add_argument(
        "--skip-delete",
        action="store_true",
        help="Skip removing existing rows for the snapshot date before loading",
    )
    return parser.parse_args()


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(name, default)


def _load_token_payload() -> Dict[str, Any]:
    """Load Yahoo OAuth token details from environment variables or JSON."""

    json_blob = _env("YAHOO_ACCESS_TOKEN_JSON")
    token_file = _env("YAHOO_ACCESS_TOKEN_FILE")

    if json_blob:
        try:
            payload = json.loads(json_blob)
        except json.JSONDecodeError as exc:
            raise OwnershipIngestError("Unable to parse YAHOO_ACCESS_TOKEN_JSON") from exc
    elif token_file:
        path = Path(token_file)
        try:
            payload = json.loads(path.read_text())
        except FileNotFoundError as exc:
            raise OwnershipIngestError(f"Yahoo token file not found: {path}") from exc
        except json.JSONDecodeError as exc:
            raise OwnershipIngestError(f"Unable to parse Yahoo token file: {path}") from exc
    else:
        payload = {
            "consumer_key": _env("YAHOO_CONSUMER_KEY"),
            "consumer_secret": _env("YAHOO_CONSUMER_SECRET"),
            "access_token": _env("YAHOO_ACCESS_TOKEN"),
            "refresh_token": _env("YAHOO_REFRESH_TOKEN"),
            "token_type": _env("YAHOO_TOKEN_TYPE"),
            "token_time": float(_env("YAHOO_TOKEN_TIME", "0") or 0.0),
            "guid": _env("YAHOO_GUID"),
        }

    missing = [k for k in ("consumer_key", "consumer_secret", "refresh_token") if not payload.get(k)]
    if missing:
        raise OwnershipIngestError(
            "Missing Yahoo OAuth credentials: " + ", ".join(missing) + ". Set YAHOO_ACCESS_TOKEN_JSON or individual vars."
        )

    # Normalise token_time to float if present
    if "token_time" in payload:
        try:
            payload["token_time"] = float(payload["token_time"] or 0.0)
        except (TypeError, ValueError):
            payload["token_time"] = 0.0

    return payload


def _build_config(args: argparse.Namespace) -> OwnershipConfig:
    snapshot_date = args.date or date.today()
    league_id = args.league_id or _env("YAHOO_LEAGUE_ID")
    if not league_id:
        raise OwnershipIngestError("League ID is required via --league-id or YAHOO_LEAGUE_ID")

    project_id = (
        args.project
        or _env("PROJECT_ID")
        or _env("GOOGLE_CLOUD_PROJECT")
        or _env("GCP_PROJECT")
    )
    if not project_id:
        raise OwnershipIngestError("Project ID is required via --project or PROJECT_ID/GOOGLE_CLOUD_PROJECT env vars")

    token_payload = _load_token_payload()
    consumer_key = token_payload.get("consumer_key")
    consumer_secret = token_payload.get("consumer_secret")

    if not consumer_key or not consumer_secret:
        raise OwnershipIngestError("Yahoo consumer key/secret missing from token payload")

    return OwnershipConfig(
        project_id=project_id,
        dataset=args.dataset,
        partition_table=args.partition_table,
        mirror_table=args.mirror_table,
        view_name=args.view_name,
        refresh_view=not args.no_view_refresh,
        snapshot_date=snapshot_date,
        league_id=league_id,
        game_code=args.game_code,
        game_id=args.game_id,
        dry_run=args.dry_run,
        delete_existing=not args.skip_delete,
        yahoo_consumer_key=consumer_key,
        yahoo_consumer_secret=consumer_secret,
        yahoo_access_token=token_payload,
    )


def _create_query(cfg: OwnershipConfig) -> YahooFantasySportsQuery:
    LOGGER.info("Authenticating with Yahoo Fantasy Sports API for league %s", cfg.league_id)
    return YahooFantasySportsQuery(
        league_id=cfg.league_id,
        game_code=cfg.game_code,
        game_id=cfg.game_id,
        yahoo_consumer_key=cfg.yahoo_consumer_key,
        yahoo_consumer_secret=cfg.yahoo_consumer_secret,
        yahoo_access_token_json=cfg.yahoo_access_token,
        browser_callback=False,
        env_var_fallback=False,
        retries=3,
        backoff=2,
    )


def _fetch_league_players(query: YahooFantasySportsQuery) -> List[Player]:
    LOGGER.info("Retrieving league player metadata")
    players = query.get_league_players()
    LOGGER.info("Retrieved %d players", len(players))
    return players


def _player_rows(
    players: Iterable[Player],
    cfg: OwnershipConfig,
    league_key: str,
) -> pd.DataFrame:
    snapshot_date = cfg.snapshot_date
    ingested_at = datetime.utcnow()

    rows: List[Dict[str, Any]] = []
    for player in players:
        percent_owned = None
        percent_delta = None
        coverage_type = None
        coverage_week = None
        if player.percent_owned:
            percent_owned = float(player.percent_owned.value) if player.percent_owned.value is not None else None
            percent_delta = (
                float(player.percent_owned.delta)
                if player.percent_owned.delta is not None
                else None
            )
            coverage_type = player.percent_owned.coverage_type or None
            coverage_week = player.percent_owned.week
        eligibility = ",".join(sorted(filter(None, player.eligible_positions)))

        rows.append(
            {
                "snapshot_date": snapshot_date,
                "ingested_at": ingested_at,
                "league_id": cfg.league_id,
                "league_key": league_key,
                "game_code": cfg.game_code,
                "game_id": cfg.game_id,
                "player_id": _safe_int(player.player_id),
                "player_key": player.player_key or None,
                "editorial_player_key": player.editorial_player_key or None,
                "player_first_name": player.first_name or None,
                "player_last_name": player.last_name or None,
                "player_full_name": player.full_name or None,
                "editorial_team_abbr": player.editorial_team_abbr or None,
                "editorial_team_full_name": player.editorial_team_full_name or None,
                "primary_position": player.primary_position or None,
                "eligible_positions": eligibility or None,
                "position_type": player.position_type or None,
                "status": player.status or None,
                "ownership_type": getattr(player.ownership, "ownership_type", None) or None,
                "owner_team_key": getattr(player.ownership, "owner_team_key", None) or None,
                "owner_team_name": getattr(player.ownership, "owner_team_name", None) or None,
                "ownership_display_date": getattr(player.ownership, "display_date", None),
                "ownership_waiver_date": getattr(player.ownership, "waiver_date", None) or None,
                "percent_owned": percent_owned,
                "percent_owned_delta": percent_delta,
                "percent_owned_coverage_type": coverage_type,
                "percent_owned_week": _safe_int(coverage_week),
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    frame["player_id"] = pd.to_numeric(frame["player_id"], errors="coerce").astype("Int64")
    frame["percent_owned"] = pd.to_numeric(frame["percent_owned"], errors="coerce")
    frame["percent_owned_delta"] = pd.to_numeric(frame["percent_owned_delta"], errors="coerce")
    frame["percent_owned_week"] = pd.to_numeric(frame["percent_owned_week"], errors="coerce").astype("Int64")

    return frame


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _delete_existing_rows(client: bigquery.Client, table_id: str, snapshot: date) -> None:
    sql = f"DELETE FROM `{table_id}` WHERE snapshot_date = @snapshot"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("snapshot", "DATE", snapshot)]
    )
    client.query(sql, job_config=job_config).result()
    LOGGER.info("Deleted existing rows from %s for %s", table_id, snapshot)


def _load_dataframe(client: bigquery.Client, table_id: str, frame: pd.DataFrame) -> None:
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(frame, table_id, job_config=job_config)
    job.result()
    LOGGER.info("Loaded %d rows into %s", len(frame), table_id)


def _refresh_view(client: bigquery.Client, cfg: OwnershipConfig) -> None:
    sql_path = Path(__file__).resolve().parent / "sql" / "create_player_ownership_latest.sql"
    if not sql_path.exists():
        raise OwnershipIngestError(f"View SQL not found at {sql_path}")

    sql = sql_path.read_text()
    sql = sql.replace("{{PROJECT_ID}}", cfg.project_id)
    sql = sql.replace("{{DATASET}}", cfg.dataset)
    sql = sql.replace("{{PARTITION_TABLE}}", cfg.partition_table)
    sql = sql.replace("{{VIEW_NAME}}", cfg.view_name)

    client.query(sql).result()
    LOGGER.info("Refreshed view %s", cfg.view_id)


def run() -> pd.DataFrame:
    args = _parse_args()
    cfg = _build_config(args)

    query = _create_query(cfg)
    league_key = query.get_league_key()
    players = _fetch_league_players(query)
    frame = _player_rows(players, cfg, league_key)

    if frame.empty:
        LOGGER.warning("No ownership rows retrieved for %s", cfg.snapshot_date)
        return frame

    if cfg.dry_run:
        LOGGER.info("Dry run requested; returning dataframe without loading to BigQuery")
        print(frame.head())
        return frame

    client = bigquery.Client(project=cfg.project_id)

    if cfg.delete_existing:
        _delete_existing_rows(client, cfg.partition_table_id, cfg.snapshot_date)
        _delete_existing_rows(client, cfg.mirror_table_id, cfg.snapshot_date)

    _load_dataframe(client, cfg.partition_table_id, frame)
    _load_dataframe(client, cfg.mirror_table_id, frame)

    if cfg.refresh_view:
        _refresh_view(client, cfg)

    return frame


if __name__ == "__main__":
    run()
