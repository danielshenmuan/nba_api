# tools/get_yahoo_game_id.py
import os
from pathlib import Path
from dotenv import dotenv_values
from yfpy.query import YahooFantasySportsQuery

# --- 1) Load ROOT .env explicitly (not the jobs/.env) ---
ROOT_ENV = Path(__file__).resolve().parents[1] / ".env"   # .../nba_api/.env
vals = dotenv_values(ROOT_ENV)
for k in ["YAHOO_CONSUMER_KEY", "YAHOO_CONSUMER_SECRET", "YAHOO_LEAGUE_ID"]:
    if not vals.get(k):
        raise RuntimeError(f"Missing {k} in {ROOT_ENV}")
os.environ.update({k: v for k, v in vals.items() if v is not None})

# --- 2) Init YFPY with explicit keys + .env for stored tokens ---
q = YahooFantasySportsQuery(
    league_id='13411',          # e.g., "13411"
    game_code="nba",
    yahoo_consumer_key=os.environ["YAHOO_CONSUMER_KEY"],
    yahoo_consumer_secret=os.environ["YAHOO_CONSUMER_SECRET"],
    env_file_location=ROOT_ENV,                        # lets YFPY read saved access tokens
)

def get_nba_game_id_for_season(season_str: str) -> tuple[int, str]:
    """
    season_str like '2025-26' -> looks for Yahoo game with code='nba' and season='2025'
    Returns (game_id, game_key)
    """
    start_year = season_str.split("-")[0].strip()
    games = q.get_all_yahoo_fantasy_game_keys()  # YFPY call
    target = next(
        g for g in games
        if str(getattr(g, "code", "")) == "nba" and str(getattr(g, "season", "")) == start_year
    )
    return int(target.game_id), str(target.game_key)

if __name__ == "__main__":
    # Example: 2025-26 season
    game_id, game_key = get_nba_game_id_for_season("2025-26")
    print(f"NBA 2025-26 → game_id={game_id}, game_key={game_key}")
