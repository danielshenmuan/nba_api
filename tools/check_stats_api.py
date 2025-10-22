"""Quick sanity check that the NBA *live* stats API is returning box score data.

Usage:
    python tools/check_stats_api.py --date 2024-04-16

When --date is omitted the script uses today's games. It fetches the live
scoreboard JSON to discover game IDs and then pulls the corresponding live
boxscores, printing the full statistics payload for every player returned.
This mirrors what you should see in production when the CDN-backed live feed is
healthy.
"""
from __future__ import annotations

import json
import time
from argparse import ArgumentParser
from datetime import datetime
from typing import Iterable

from nba_api.live.nba.endpoints import boxscore as live_boxscore
from nba_api.live.nba.endpoints import scoreboard as live_scoreboard
from requests.exceptions import RequestException


LIVE_TODAY_ENDPOINT = "scoreboard/todaysScoreboard_00.json"


def scoreboard_endpoint_for_date(target_date: datetime) -> str:
    today = datetime.today().date()
    if target_date.date() == today:
        return LIVE_TODAY_ENDPOINT
    return f"scoreboard/scoreboard_{target_date.strftime('%Y%m%d')}.json"


def load_games(target_date: datetime, timeout: int = 15, retries: int = 3) -> list[dict]:
    board = live_scoreboard.ScoreBoard(get_request=False, timeout=timeout)
    board.endpoint_url = scoreboard_endpoint_for_date(target_date)

    for attempt in range(retries):
        try:
            board.get_request()
            data = board.get_dict()
            games = data.get("scoreboard", {}).get("games", [])
            return games or []
        except RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(1)

    return []


def fetch_boxscore(game_id: str, timeout: int = 15, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            box = live_boxscore.BoxScore(game_id=game_id, timeout=timeout)
            return box.get_dict().get("game", {})
        except RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(1)

    return {}


def iter_team_players(game_payload: dict) -> Iterable[tuple[str, dict, dict]]:
    for side in ("homeTeam", "awayTeam"):
        team = game_payload.get(side) or {}
        players = team.get("players") or []
        for player in players:
            yield side, team, player


def main() -> None:
    parser = ArgumentParser(description="Check NBA live stats API availability for a game date.")
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        default=datetime.today(),
        help="Target game date in YYYY-MM-DD format (defaults to today).",
    )
    args = parser.parse_args()

    target_date = args.date
    print(f"Checking live scoreboard for {target_date.date()}...")

    try:
        games = load_games(target_date)
    except RequestException as exc:
        print(f"Failed to load live scoreboard: {exc}")
        return

    if not games:
        print("No games found on the live scoreboard.")
        return

    print(f"Discovered {len(games)} game(s): {', '.join(game['gameId'] for game in games)}")

    for game in games:
        game_id = game["gameId"]
        print(f"\nFetching live box score for GAME_ID={game_id}")
        try:
            payload = fetch_boxscore(game_id)
        except RequestException as exc:
            print(f"  Failed to load live box score: {exc}")
            continue

        if not payload:
            print("  No live data returned.")
            continue

        for side, team, player in iter_team_players(payload):
            team_name = f"{team.get('teamCity', '')} {team.get('teamName', '')}".strip()
            print(
                f"  {side.replace('Team', '').capitalize()} team {team_name} "
                f"({team.get('teamTricode', '???')}), player #{player.get('jerseyNum', '')} "
                f"{player.get('name', 'Unknown')}"
            )
            stats = player.get("statistics", {})
            if stats:
                stats_json = json.dumps(stats, indent=4, sort_keys=True)
                for line in stats_json.splitlines():
                    print(f"      {line}")
            else:
                print("      (no statistics in payload)")


if __name__ == "__main__":
    main()