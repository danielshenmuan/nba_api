# tools/smoke_owned_by_week.py
import os
from pathlib import Path
from dotenv import dotenv_values
from yfpy.query import YahooFantasySportsQuery

PLAYER_NAME = "Keyonte George"   # <-- change for testing

# 1) Load root .env
ENV = Path(__file__).resolve().parents[1] / ".env"
os.environ.update({k: v for k, v in dotenv_values(ENV).items() if v})

# 2) Init YFPY (no prompts)
kwargs = dict(
    league_id=os.environ["YAHOO_LEAGUE_ID"],
    game_code="nba",
    yahoo_consumer_key=os.environ["YAHOO_CONSUMER_KEY"],
    yahoo_consumer_secret=os.environ["YAHOO_CONSUMER_SECRET"],
    env_file_location=ENV,
)
gid = os.environ.get("YAHOO_GAME_ID")
if gid: kwargs["game_id"] = int(gid)
q = YahooFantasySportsQuery(**kwargs)

# 3) Find player_key from league players
players = q.get_league_players()
target = None
needle = PLAYER_NAME.strip().lower()
for p in players:
    name = (getattr(getattr(p, "name", None), "full", None) or getattr(p, "full_name", None) or "").strip()
    if name.lower() == needle:
        target = p
        break
if not target:
    raise SystemExit(f"Player '{PLAYER_NAME}' not found in league roster.")

player_key = getattr(target, "player_key", None)
if not player_key:
    raise SystemExit("player_key missing for matched player.")

# 4) Universal percent-owned via weekly endpoint (current week)
ply = q.get_player_percent_owned_by_week(player_key, "current")  # returns Player model w/ .percent_owned
po = getattr(ply, "percent_owned", None)
val = getattr(po, "value", None) if po is not None else None

print(f"{PLAYER_NAME} percent-owned: {val if val is not None else 'N/A'}")
