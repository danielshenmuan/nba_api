import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from service.player_names import match_players, norm_tokens  # noqa: E402

ROWS = [{"player_name": n, "pts": i} for i, n in enumerate(
    ["Victor Wembanyama", "Luka Doncic", "Jaime Jaquez Jr.", "De'Aaron Fox", "Jalen Brunson", "Jalen Williams", "Karl-Anthony Towns", "Shai Gilgeous-Alexander"])]


def test_normalisation():
    assert norm_tokens("Luka Dončić") == ["luka", "doncic"]
    assert norm_tokens("Jaime Jaquez Jr.") == ["jaime", "jaquez"]
    assert norm_tokens("De'Aaron Fox") == ["de", "aaron", "fox"]
    assert norm_tokens("  ") == [] and norm_tokens(None) == []


def test_exact_and_accent_insensitive():
    r = match_players(ROWS, ["Victor Wembanyama", "luka dončić", "Jaime Jaquez"])
    assert [x["player_name"] for x in r["lines"]] == ["Victor Wembanyama", "Luka Doncic", "Jaime Jaquez Jr."]
    assert r["not_found"] == [] and r["ambiguous"] == []


def test_a_unique_partial_name_works():
    assert [x["player_name"] for x in match_players(ROWS, ["Wembanyama"])["lines"]] == ["Victor Wembanyama"]
    assert [x["player_name"] for x in match_players(ROWS, ["Gilgeous-Alexander"])["lines"]] == ["Shai Gilgeous-Alexander"]


def test_an_ambiguous_name_is_never_guessed():
    r = match_players(ROWS, ["Jalen"])
    assert r["lines"] == [] and r["ambiguous"] == ["Jalen"]


def test_unknown_players_are_reported_not_invented():
    r = match_players(ROWS, ["Nobody Real", "Victor Wembanyama"])
    assert r["not_found"] == ["Nobody Real"] and len(r["lines"]) == 1


def test_a_nickname_does_not_match_and_is_reported():
    assert match_players(ROWS, ["Wemby"])["not_found"] == ["Wemby"]


def test_duplicates_and_blanks():
    r = match_players(ROWS, ["Victor Wembanyama", "wembanyama", "", "  "])
    assert len(r["lines"]) == 1
