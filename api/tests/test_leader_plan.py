import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from service.nba_fetch import SOCIAL_TABLE_FQN, TABLE_FQN, classify_missing, leader_query_plan, split_low_owned  # noqa: E402


def test_defaults_are_unchanged_for_existing_callers():
    best, worst = leader_query_plan("best"), leader_query_plan("worst")
    assert (best["table"], best["score"], best["direction"], best["min_roster_pct"], best["extra_select"]) == (TABLE_FQN, "z_score", "DESC", 0.0, "")
    assert (worst["table"], worst["score"], worst["direction"], worst["min_roster_pct"]) == (TABLE_FQN, "z_score", "ASC", 20.0)


def test_social_ranks_by_the_social_score_from_the_social_view():
    p = leader_query_plan("worst", "social", 40)
    assert p["table"] == SOCIAL_TABLE_FQN and p["score"] == "social_score" and p["direction"] == "ASC" and p["min_roster_pct"] == 40.0
    assert "social_score" in p["extra_select"]
    assert leader_query_plan("best", "social")["direction"] == "DESC"


def test_the_floor_can_be_set_on_either_list_and_zero_is_respected():
    assert leader_query_plan("best", "fv", 30)["min_roster_pct"] == 30.0
    assert leader_query_plan("worst", "fv", 0)["min_roster_pct"] == 0.0          # an explicit 0 is not "use the default"
    assert leader_query_plan("worst", "social", "40")["min_roster_pct"] == 40.0


@pytest.mark.parametrize("mode,rank_by", [("median", "fv"), ("best", "vibes"), ("", "fv"), ("best", "")])
def test_bad_input_is_rejected_not_guessed(mode, rank_by):
    with pytest.raises(ValueError):
        leader_query_plan(mode, rank_by)


def test_cold_ranks_by_the_cold_score_from_the_social_view():
    p = leader_query_plan("worst", "cold", 40)
    assert p["table"] == SOCIAL_TABLE_FQN and p["score"] == "cold_score" and p["direction"] == "ASC" and p["min_roster_pct"] == 40.0
    assert "cold_score" in p["extra_select"] and "social_score" not in p["extra_select"]


def _pool(n=20):
    # best-first; ownership falls as rank falls, scores fall 1.0 per rank from 20
    return [{"player_name": f"P{i}", "roster_pct": 99 - i * 5, "social_score": 20 - i} for i in range(n)]


def test_low_owned_zone_takes_the_best_low_owned_nights_below_the_top():
    top, low = split_low_owned(_pool(), 8, 3, max_pct=50, min_score=3.0)
    assert [r["player_name"] for r in top] == [f"P{i}" for i in range(8)]
    assert [r["player_name"] for r in low] == ["P10", "P11", "P12"]            # first ranks past 8 with ownership < 50 (99-5i < 50 -> i >= 10)
    assert all(r["social_score"] >= 3.0 and r["roster_pct"] < 50 for r in low)


def test_low_owned_players_already_in_the_top_stay_in_the_main_list():
    pool = _pool(); pool[2]["roster_pct"] = 3
    top, low = split_low_owned(pool, 8, 3)
    assert pool[2] in top and pool[2] not in low


def test_not_enough_low_owned_extends_the_main_list_instead():
    pool = _pool(); 
    for r in pool: r["roster_pct"] = 90
    pool[10]["roster_pct"] = 5
    top, low = split_low_owned(pool, 8, 3)
    assert [r["player_name"] for r in low] == ["P10"]
    assert [r["player_name"] for r in top] == [f"P{i}" for i in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)]   # 8 + the next two best that are not in the zone
    assert len(top) + len(low) == 11


def test_score_floor_and_unknown_ownership():
    pool = _pool(); pool[9]["social_score"] = 2.9; pool[9]["roster_pct"] = 5
    pool[10]["roster_pct"] = None                                              # unknown ownership counts as 0, like the ownership floor
    _, low = split_low_owned(pool, 8, 3)
    assert "P9" not in [r["player_name"] for r in low] and "P10" in [r["player_name"] for r in low]


KNOWN = [{"player_name": "Klay Thompson", "team_abbr": "MIA", "last_game_date": "2026-11-02"},
         {"player_name": "Bam Adebayo", "team_abbr": "MIA", "last_game_date": "2026-11-02"},
         {"player_name": "Jaylen Brown", "team_abbr": "BOS", "last_game_date": "2026-11-04"},
         {"player_name": "Jaylen Williams", "team_abbr": "OKC", "last_game_date": "2026-11-04"}]


def test_a_real_player_with_no_box_score_is_no_game_not_not_found():
    out = classify_missing({"lines": [], "not_found": ["Klay Thompson", "Nobody Atall", "Jaylen"], "ambiguous": []}, KNOWN)
    assert [g["player_name"] for g in out["no_game"]] == ["Klay Thompson"] and out["no_game"][0]["last_game_date"] == "2026-11-02"
    assert out["not_found"] == ["Nobody Atall"] and out["ambiguous"] == ["Jaylen"]


def test_standout_clause_drops_anyone_with_one_good_stat():
    from service.nba_fetch import standout_clause
    sql, params = standout_clause(15, 8, 6)
    assert sql == "AND COALESCE(stats.pts, 0) < @standout_pts AND COALESCE(stats.reb, 0) < @standout_reb AND COALESCE(stats.ast, 0) < @standout_ast"
    assert params == [("standout_pts", 15.0), ("standout_reb", 8.0), ("standout_ast", 6.0)]


def test_standout_clause_is_empty_when_unset_and_partial_when_some_set():
    from service.nba_fetch import standout_clause
    assert standout_clause() == ("", [])
    sql, params = standout_clause(reb=8)
    assert sql == "AND COALESCE(stats.reb, 0) < @standout_reb" and params == [("standout_reb", 8.0)]
