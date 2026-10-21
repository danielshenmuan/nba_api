"""Match free-text player names (accents, suffixes, punctuation, partial names) to box-score rows. Pure and unit-tested."""
from __future__ import annotations

import re
import unicodedata

_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def norm_tokens(name: str) -> list[str]:
    """'Luka Dončić' -> ['luka', 'doncic']; 'Jaime Jaquez Jr.' -> ['jaime', 'jaquez']; "De'Aaron Fox" -> ['de', 'aaron', 'fox']."""
    s = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode("ascii").lower()
    toks = [t for t in re.split(r"[^a-z0-9]+", s) if t]
    return [t for t in toks if t not in _SUFFIXES]


def match_players(rows: list[dict], queries: list[str]) -> dict:
    """For each query pick the single row it names: exact normalised name first, else a row containing every word of the
    query, but only if that is unambiguous. Returns {'lines': [...], 'not_found': [...], 'ambiguous': [...]}."""
    index = [(r, norm_tokens(r.get("player_name", ""))) for r in rows]
    lines, not_found, ambiguous, seen = [], [], [], set()
    for q in queries:
        qt = norm_tokens(q)
        if not qt:
            continue
        exact = [r for r, t in index if t == qt]
        hits = exact or [r for r, t in index if set(qt) <= set(t)]
        if len(hits) == 1:
            if id(hits[0]) not in seen:
                seen.add(id(hits[0])); lines.append(hits[0])
        elif len(hits) == 0:
            not_found.append(q)
        else:
            ambiguous.append(q)
    return {"lines": lines, "not_found": not_found, "ambiguous": ambiguous}
