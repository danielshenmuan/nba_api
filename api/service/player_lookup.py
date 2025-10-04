from functools import lru_cache
from nba_api.stats.static import players
from rapidfuzz import process, fuzz
import unicodedata, re

# High-confidence nicknames
ALIAS = {
    "dame": 203081, "steph": 201939, "kd": 201142, "bron": 2544, "lbj": 2544,
    "joker": 203999, "ad": 203076, "pg13": 202331, "book": 1626164, "jt": 1628369,
    "giannis": 203507, "harden": 201935, "kyrie": 202681, "zion": 1629627,
}

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9 ]+", "", s.lower()).strip()

@lru_cache(maxsize=1)
def _player_index():
    plist = players.get_players()  # [{'id':201939, 'full_name':'Stephen Curry', 'is_active':True}, ...]
    by_id = {p["id"]: p for p in plist}
    full_norm_to_id = {_norm(p["full_name"]): p["id"] for p in plist}
    last_to_ids = {}
    for p in plist:
        last = _norm(p["full_name"].split()[-1])
        last_to_ids.setdefault(last, set()).add(p["id"])
    names = [p["full_name"] for p in plist]
    return by_id, full_norm_to_id, last_to_ids, names

def search_players(q: str, limit: int = 5):
    by_id, full_norm_to_id, last_to_ids, names = _player_index()
    qn = _norm(q)

    # 1) Hard alias hits (e.g., "dame", "steph", "kd")
    if qn in ALIAS:
        pid = ALIAS[qn]; p = by_id[pid]
        return [{"player_id": pid, "full_name": p["full_name"], "confidence": 1.0, "reason": "alias"}]

    # 2) Exact full-name match
    if qn in full_norm_to_id:
        pid = full_norm_to_id[qn]; p = by_id[pid]
        return [{"player_id": pid, "full_name": p["full_name"], "confidence": 1.0, "reason": "exact"}]

    # 3) Unique last-name shortcut
    if qn in last_to_ids and len(last_to_ids[qn]) == 1:
        pid = list(last_to_ids[qn])[0]; p = by_id[pid]
        return [{"player_id": pid, "full_name": p["full_name"], "confidence": 0.9, "reason": "unique_last"}]

    # 4) Fuzzy on full names
    top = process.extract(q, names, scorer=fuzz.WRatio, limit=limit)
    out = []
    for name, score, idx in top:
        # 'names[idx]' equals 'name'
        pid = next(p["id"] for p in by_id.values() if p["full_name"] == name)
        out.append({"player_id": pid, "full_name": name, "confidence": score / 100.0, "reason": "fuzzy"})
    return out
