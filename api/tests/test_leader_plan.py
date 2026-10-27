import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from service.nba_fetch import SOCIAL_TABLE_FQN, TABLE_FQN, leader_query_plan  # noqa: E402


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
