import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from service.waiver_wire import (Rules, candidate_pool, player_levels, render_en, render_zh, top_group, waiver_wire)  # noqa: E402

AS_OF = date(2026, 11, 10)


def games(spec):
    """spec: {pid: dict(name, team, games=[(days_ago, minutes, z), ...])} -> games frame."""
    rows = []
    for pid, s in spec.items():
        for days_ago, mins, z in s["games"]:
            rows.append(dict(player_id=pid, player_name=s.get("name", f"P{pid}"), team_abbr=s.get("team", "AAA"), position="G",
                             game_date=AS_OF - timedelta(days=days_ago), minutes=mins, z_score=z, pts=20.0, reb=5.0, ast=4.0, stl=1.0, blk=0.5, fg3m=2.0))
    return pd.DataFrame(rows)


def own(rows):
    return pd.DataFrame([dict(player_id=p, roster_pct=r, injury_status=i) for p, r, *rest in rows for i in [rest[0] if rest else None]])


def steady(z, n=20, mins=30.0, start=1):
    return [(start + i, mins, z) for i in range(n)]


class TestPlayerLevels:
    def test_blend_of_last5_and_prior_once_10_games(self):
        g = games({1: dict(games=[(i, 30.0, 6.0) for i in range(1, 6)] + [(i, 30.0, 2.0) for i in range(6, 21)])})
        lv = player_levels(g, AS_OF)
        assert lv.loc[1, "z5"] == 6.0 and lv.loc[1, "zprior"] == 2.0 and lv.loc[1, "level"] == 4.0

    def test_below_10_games_level_is_the_plain_mean(self):
        g = games({1: dict(games=[(1, 30.0, 8.0), (2, 30.0, 8.0), (3, 30.0, 2.0), (4, 30.0, 2.0)])})
        assert player_levels(g, AS_OF).loc[1, "level"] == 5.0

    def test_games_on_or_after_as_of_are_ignored(self):
        g = games({1: dict(games=[(0, 30.0, 99.0), (-1, 30.0, 99.0)] + steady(3.0, 12))})
        assert player_levels(g, AS_OF).loc[1, "z5"] == 3.0

    def test_prior_needs_5_games_else_falls_back_to_mean(self):
        g = games({1: dict(games=steady(4.0, 10))})   # 5 recent + only 5 prior -> blend allowed
        assert player_levels(g, AS_OF).loc[1, "level"] == 4.0
        g2 = games({1: dict(games=[(i, 30.0, 6.0) for i in range(1, 6)] + [(i, 30.0, 0.0) for i in range(6, 10)] + [(10, 30.0, 0.0)])})
        assert player_levels(g2, AS_OF).loc[1, "n_games"] == 10



class TestPool:
    def base(self):
        g = games({1: dict(games=steady(3.0)), 2: dict(games=steady(3.0)), 3: dict(games=steady(3.0, 3)), 4: dict(games=steady(3.0, mins=23.9)),
                   5: dict(games=steady(3.0, start=12)), 6: dict(games=steady(3.0)), 7: dict(games=steady(3.0)), 8: dict(games=steady(3.0))})
        return player_levels(g, AS_OF)

    def test_gates(self):
        lv = self.base()
        o = own([(1, 4.9), (2, 5.0), (3, 20.0), (4, 20.0), (5, 20.0), (6, 50.0), (7, 49.9), (8, 20.0, "OUT")])
        got = set(candidate_pool(lv, o, AS_OF).index)
        assert got == {2, 7}            # 1: <5%, 3: 3 games, 4: 23.9 min, 5: last game 12 days ago, 6: 50%, 8: out

    def test_injury_status_missing_is_fine(self):
        lv = self.base()
        assert 1 in candidate_pool(lv, pd.DataFrame([dict(player_id=1, roster_pct=20.0)]), AS_OF).index

    def test_top_group_sorted_and_capped(self):
        lv = self.base(); lv["level"] = [1.0, 5.0, 3.0, 4.0, 2.0, 0.0, 6.0, 7.0]
        assert list(top_group(lv, 3).index) == [8, 7, 2]


R = Rules(top_k=3, min_risers=1, min_days_into_season=0, trend_level_quantile=0.5)


def scenario():
    """A, B, C are the stable top three. X was bad until 3 days ago and has jumped into the top group. T has a big ownership rise."""
    return games({
        1: dict(name="Alpha", games=steady(6.0)), 2: dict(name="Bravo", games=steady(5.5)), 3: dict(name="Charlie", games=steady(5.0)),
        4: dict(name="Xray", games=steady(13.0, 5) + steady(0.0, 15, start=6)),
        5: dict(name="Tango", games=steady(5.2)),     # just outside the top 3 (Charlie is 5.0), above the pool median
        6: dict(name="Low", games=steady(0.5)), 7: dict(name="Low2", games=steady(0.2)), 8: dict(name="Low3", games=steady(0.1)),
    })


NOW = own([(p, 20.0) for p in range(1, 9)])
THEN = own([(p, 20.0) for p in range(1, 9)])


class TestWaiverWire:
    def test_new_entrant_is_a_riser_and_holdovers_are_listed_separately(self):
        res = waiver_wire(scenario(), NOW, THEN, AS_OF, None, R)
        assert [r["name"] for r in res.risers] == ["Xray"] and res.risers[0]["reasons"] == ["new_in_top_group"]
        assert {h["name"] for h in res.holdovers} == {"Alpha", "Bravo"}
        assert res.post_worthy

    def test_ownership_rise_with_level_qualifies_on_its_own(self):
        now = own([(p, 20.0) for p in range(1, 9)]); now.loc[now.player_id == 5, "roster_pct"] = 28.0
        res = waiver_wire(scenario(), now, THEN, AS_OF, None, R)
        t = [r for r in res.risers if r["name"] == "Tango"][0]
        assert t["reasons"] == ["ownership_up"] and t["own_change"] == 8.0

    def test_big_ownership_rise_without_the_level_is_ignored(self):
        now = own([(p, 20.0) for p in range(1, 9)]); now.loc[now.player_id == 6, "roster_pct"] = 40.0
        assert "Low" not in [r["name"] for r in waiver_wire(scenario(), now, THEN, AS_OF, None, R).risers]

    def test_small_rise_does_not_trigger(self):
        now = own([(p, 20.0) for p in range(1, 9)]); now.loc[now.player_id == 5, "roster_pct"] = 24.9
        assert "Tango" not in [r["name"] for r in waiver_wire(scenario(), now, THEN, AS_OF, None, R).risers]

    def test_a_player_in_both_reasons_lists_both(self):
        now = own([(p, 20.0) for p in range(1, 9)]); now.loc[now.player_id == 4, "roster_pct"] = 30.0
        assert waiver_wire(scenario(), now, THEN, AS_OF, None, R).risers[0]["reasons"] == ["new_in_top_group", "ownership_up"]

    def test_not_enough_new_players_means_no_post(self):
        res = waiver_wire(scenario(), NOW, THEN, AS_OF, None, Rules(top_k=3, min_risers=3, min_days_into_season=0))
        assert not res.post_worthy and res.skip_reason == "too_few_new_players"

    def test_first_week_of_the_season_is_skipped(self):
        res = waiver_wire(scenario(), NOW, THEN, AS_OF, AS_OF - timedelta(days=6), Rules(top_k=3, min_risers=1))
        assert res.skip_reason == "season_too_young" and not res.post_worthy
        assert waiver_wire(scenario(), NOW, THEN, AS_OF, AS_OF - timedelta(days=7), Rules(top_k=3, min_risers=1)).skip_reason is None

    def test_no_earlier_ownership_falls_back_and_says_so(self):
        res = waiver_wire(scenario(), NOW, None, AS_OF, None, R)
        assert res.freshness_basis == "current_ownership" and [r["name"] for r in res.risers] == ["Xray"]
        assert res.risers[0]["own_change"] is None

    def test_post_is_capped_at_max_in_post(self):
        spec = {i: dict(name=f"N{i}", games=steady(16.0, 5) + steady(0.0, 15, start=6)) for i in range(1, 9)}   # level 8.0 now, 3.2 three days ago
        spec.update({i: dict(name=f"S{i}", games=steady(7.0)) for i in range(20, 30)})                      # ten steady players at 7.0
        o = own([(p, 20.0) for p in spec])
        res = waiver_wire(games(spec), o, o, AS_OF, None, Rules(top_k=10, min_risers=1, min_days_into_season=0, max_in_post=5))
        assert len(res.risers) == 5

    def test_players_who_stopped_playing_or_are_out_never_appear(self):
        g = scenario(); o = NOW.copy(); o.loc[o.player_id == 4, "injury_status"] = "OUT"
        assert waiver_wire(g, o, THEN, AS_OF, None, R).risers == []

    def test_no_games_is_a_clean_skip(self):
        g = games({1: dict(games=[(0, 30.0, 5.0)])})
        assert waiver_wire(g, NOW, THEN, AS_OF, None, R).skip_reason == "no_games"


class TestText:
    def res(self, now=None):
        return waiver_wire(scenario(), NOW if now is None else now, THEN, AS_OF, None, R)

    def test_zh_has_the_data_and_the_exact_shape(self):
        t = render_zh(self.res(), "yahoo")
        lines = t.split("\n")
        assert lines[0] == "🔥 自由市場推薦名單｜11/10" and lines[1] == "" and lines[2] == "1. Xray（AAA）"
        assert lines[3].startswith("   近5場平均 ") and "分鐘" in lines[3]
        assert "不是預測" not in t and "（只是" not in t                                  # no disclaimer or footnote
        assert lines[-1].startswith("   近5場平均 ") or lines[-1].startswith("📌 持續在榜：")   # the post ends with content, not a note
        assert "持有率低於50%" not in t and "近況和上場時間都撐得住" not in t           # the old subtitle line is gone

    def test_ownership_never_appears_in_any_text(self):
        """ESPN's roster % differs a lot from Yahoo's, so no percentage and no +N change is shown, whatever the data says."""
        for pct in (6.0, 20.0, 31.0, 49.0):
            now = own([(p, 20.0) for p in range(1, 9)]); now.loc[now.player_id == 4, "roster_pct"] = pct
            for src in ("yahoo", "espn"):
                for t in (render_zh(self.res(now), src), render_en(self.res(now), src)):
                    assert "%" not in t and "持有率" not in t and "擁有率" not in t and "rostered" not in t.lower()
                    assert "ESPN" not in t and "Yahoo" not in t and "（+" not in t and " (+" not in t
        assert "own_change" in self.res().risers[0] and "roster_pct" in self.res().risers[0]       # still in the data, just not in the post

    def test_internal_metrics_never_appear(self):
        for t in (render_zh(self.res(), "yahoo"), render_en(self.res(), "yahoo")):
            assert "FV" not in t and "Fantasy Value" not in t and "level" not in t.lower() and "social" not in t.lower()

    def test_still_on_the_wire_lists_names_only(self):
        assert "📌 持續在榜：Alpha、Bravo" in render_zh(self.res(), "yahoo")
        assert "Still on the wire: Alpha, Bravo" in render_en(self.res(), "espn")
        assert "Alpha（" not in render_zh(self.res(), "yahoo") and "Alpha (" not in render_en(self.res(), "yahoo")

    def test_en_text(self):
        t = render_en(self.res(), "espn")
        lines = t.split("\n")
        assert lines[0] == "🔥 Waiver Wire: new names | 11/10" and lines[1] == "" and lines[2] == "1. Xray (AAA)"
        assert "20 pts, 5 reb, 4 ast" in t and "forecast" not in t.lower() and "(Recent" not in t and lines[-1].startswith("Still on the wire:")
        assert "Under 50% rostered" not in t

    def test_nothing_to_say_means_empty_text(self):
        r = waiver_wire(scenario(), NOW, THEN, AS_OF, AS_OF, Rules(top_k=3, min_risers=1))
        assert render_zh(r, "yahoo") == "" and render_en(r, "yahoo") == ""

    def test_standout_steals_blocks_threes_are_shown_at_most_two(self):
        r = {"name": "N", "team": "T", "roster_pct": 20.0, "own_change": None, "pts": 10.0, "reb": 5.0, "ast": 2.0, "stl": 2.0, "blk": 2.5, "fg3m": 3.0, "minutes_last5": 30.0}
        from service.waiver_wire import _extras
        assert _extras(r) == ["2.5阻攻", "2.0抄截"] and _extras(r, False) == ["2.5 blk", "2.0 stl"]
        assert _extras({**r, "stl": 0.5, "blk": 0.5, "fg3m": 1.0}) == []


from service.waiver_wire import latest_snapshot, waiver_wire_for_day  # noqa: E402


def snapshots(days=14, pct=20.0, players=range(1, 9)):
    return pd.DataFrame([dict(player_id=p, roster_pct=pct, injury_status=None, snapshot_date=AS_OF - timedelta(days=i))
                         for i in range(days) for p in players])


class TestLatestSnapshot:
    def test_takes_the_latest_on_or_before_and_ignores_stale(self):
        o = pd.DataFrame([dict(player_id=1, roster_pct=10.0, snapshot_date=AS_OF - timedelta(days=1)),
                          dict(player_id=1, roster_pct=12.0, snapshot_date=AS_OF),
                          dict(player_id=1, roster_pct=99.0, snapshot_date=AS_OF + timedelta(days=1)),   # the future: never used
                          dict(player_id=2, roster_pct=5.0, snapshot_date=AS_OF - timedelta(days=5))])      # too old
        got = latest_snapshot(o, AS_OF, 2).set_index("player_id")["roster_pct"].to_dict()
        assert got == {1: 12.0}


class TestNoRepeats:
    """Xray is new on AS_OF. The ledger (exclude_ids) is what keeps him from being featured twice."""
    def test_not_excluded_he_is_a_riser(self):
        r = waiver_wire_for_day(scenario(), snapshots(), AS_OF, None, R)
        assert [x["name"] for x in r.risers] == ["Xray"]

    def test_excluded_he_is_left_out_and_so_is_the_post(self):
        r = waiver_wire_for_day(scenario(), snapshots(), AS_OF, None, R, exclude_ids={4})
        assert r.risers == [] and not r.post_worthy and r.skip_reason == "too_few_new_players"

    def test_excluding_someone_else_changes_nothing(self):
        r = waiver_wire_for_day(scenario(), snapshots(), AS_OF, None, R, exclude_ids={1, 2, 3})
        assert [x["name"] for x in r.risers] == ["Xray"]

    def test_excluded_players_can_still_be_holdovers_context(self):
        r = waiver_wire_for_day(scenario(), snapshots(), AS_OF, None, R, exclude_ids={1})
        assert "Alpha" in [h["name"] for h in r.holdovers]
