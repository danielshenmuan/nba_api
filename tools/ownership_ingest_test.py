import os
from datetime import date, datetime
from dotenv import load_dotenv, find_dotenv
from yfpy.query import YahooFantasySportsQuery
from yfpy.exceptions import YahooFantasySportsDataNotFound

# --- .env bootstrap (kept from your script style) ---
env_path = find_dotenv(usecwd=True)
loaded = load_dotenv(dotenv_path=env_path, override=False)
print(f"[dotenv] loaded={loaded} path={env_path or 'NOT FOUND'}")
print(f"[cwd] {os.getcwd()}")

REQUIRED = ["YAHOO_LEAGUE_ID", "YAHOO_CONSUMER_KEY", "YAHOO_CONSUMER_SECRET"]
for k in REQUIRED:
    print(f"{k}: {'SET' if os.getenv(k) else 'MISSING'}")
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

def _yahoo_query():
    kwargs = dict(
        league_id=os.environ["YAHOO_LEAGUE_ID"],
        game_code="nba",
        yahoo_consumer_key=os.environ["YAHOO_CONSUMER_KEY"],
        yahoo_consumer_secret=os.environ["YAHOO_CONSUMER_SECRET"],
    )
    gid = os.getenv("YAHOO_GAME_ID")
    if gid:
        kwargs["game_id"] = int(gid)
    return YahooFantasySportsQuery(**kwargs)

q = _yahoo_query()
SNAPSHOT_DATE = date.today().isoformat()
def _as_mapping(obj):
    if hasattr(obj, "as_dict"):
        try:
            return obj.as_dict()
        except Exception:
            pass
    return obj

def _player_full_name(p):
    n = p.get("name") if isinstance(p, dict) else getattr(p, "name", {}) or {}
    if isinstance(n, dict):
        return n.get("full") or " ".join(filter(None, [n.get("first"), n.get("last")])) or ""
    full = getattr(n, "full", None)
    if full:
        return full
    first = getattr(n, "first", "") or ""
    last = getattr(n, "last", "") or ""
    return (first + " " + last).strip()

def _player_key(p):
    if isinstance(p, dict):
        return p.get("player_key") or p.get("editorial_player_key")
    return getattr(p, "player_key", None) or getattr(p, "editorial_player_key", None)

def _player_id(p):
    return p.get("player_id") if isinstance(p, dict) else getattr(p, "player_id", None)

def _iter_all_players():
    # Try no-arg first
    try:
        res = q.get_league_players()
        if res:
            for pl in res:
                yield _as_mapping(pl)
            return
    except TypeError:
        pass
    # Paginated fallback (start, count)
    start, page = 0, 50
    while True:
        chunk = q.get_league_players(start, page)
        if not chunk:
            break
        yielded = False
        items = list(chunk)
        for pl in items:
            yielded = True
            yield _as_mapping(pl)
        if not yielded:
            break
        start += len(items)

def _extract_percent_value(po_obj):
    """
    Accepts either a dict {'value': ...} or an object with .value or .get('value').
    Returns float or None.
    """
    if po_obj is None:
        return None
    # dict-like
    if isinstance(po_obj, dict):
        val = po_obj.get("value")
    else:
        # object-like: try attribute, then mapping-style get
        val = getattr(po_obj, "value", None)
        if val is None and hasattr(po_obj, "get"):
            try:
                val = po_obj.get("value")
            except Exception:
                pass
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None

def _percent_owned_current(player_key: str):
    """
    Uses Yahoo's 'current' week shortcut.
    Avoids dict(res); pulls from attributes if needed.
    """
    try:
        res = q.get_player_percent_owned_by_week(player_key, "current")
    except YahooFantasySportsDataNotFound:
        return None
    if not res:
        return None

    # Try to get percent_owned from dict or attributes
    if hasattr(res, "as_dict"):
        try:
            d = res.as_dict()
            return _extract_percent_value(d.get("percent_owned"))
        except Exception:
            pass

    # attribute fallbacks
    po = getattr(res, "percent_owned", None)
    return _extract_percent_value(po)

# ---- Print: player_id | player_name | snapshot_date | roster_pct | player_key ----
count = 0
print("\nplayer_id | player_name | snapshot_date | roster_pct | player_key")
print("-" * 90)

for p in _iter_all_players():
    pid = _player_id(p)
    pkey = _player_key(p)
    name = _player_full_name(p)
    if not (pid and pkey and name):
        continue
    pct = _percent_owned_current(pkey)
    pct_str = "NA" if pct is None else f"{pct:.1f}"
    print(f"{pid} | {name} | {SNAPSHOT_DATE} | {pct_str} | {pkey}")
    count += 1

print(f"\n[done] total players printed: {count}")