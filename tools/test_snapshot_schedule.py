"""Schedule part of jobs/snapshot_export.py: `weeks` unchanged, `days` added. No network.

Run: python tools/test_snapshot_schedule.py
"""
import sys
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
sys.modules.setdefault("google.cloud.bigquery", mock.MagicMock())
sys.modules.setdefault("google.cloud", mock.MagicMock())
import snapshot_export as se  # noqa: E402

START = date(2026, 11, 23)  # a Monday


def sample():
    g = {}
    g["2026-11-23"] = (["BOS", "NYK"], 0)
    g["2026-11-24"] = ([], 0)
    g["2026-11-25"] = (["BOS", "LAL"], 1)
    g["2026-11-26"] = ([], 2)  # only TBD teams listed
    g["2026-11-30"] = (["BOS", "LAL"], 0)
    return g


class ScheduleTests(unittest.TestCase):
    def test_weeks_derived_from_days_match(self):
        sc, _ = se.schedule_from_days(sample(), set(), START)
        self.assertEqual(sc["weeks"][0], {"start": "2026-11-23", "games": {"BOS": 2, "LAL": 1, "NYK": 1}, "tbd_slots": 3})
        self.assertEqual(sc["weeks"][1], {"start": "2026-11-30", "games": {"BOS": 1, "LAL": 1}})
        self.assertEqual(sc["days"], {"2026-11-23": ["BOS", "NYK"], "2026-11-25": ["BOS", "LAL"], "2026-11-26": [], "2026-11-30": ["BOS", "LAL"]})
        self.assertEqual(sc["tbd_days"], {"2026-11-25": 1, "2026-11-26": 2})

    def test_failed_day_drops_week_and_truncates_days(self):
        sc, w = se.schedule_from_days(sample(), {"2026-11-27"}, START)
        self.assertEqual([x["start"] for x in sc["weeks"]], ["2026-11-30"])
        self.assertNotIn("2026-11-30", sc["days"])  # days stop before the first failure: no silent holes
        self.assertTrue(any("2026-11-27" in m for m in w))

    def test_weeks_limited_to_28_days_days_go_further(self):
        g = sample()
        g["2026-12-30"] = (["BOS"], 0)  # day 38: beyond the weekly window
        sc, _ = se.schedule_from_days(g, set(), START)
        self.assertEqual([x["start"] for x in sc["weeks"]], ["2026-11-23", "2026-11-30"])
        self.assertIn("2026-12-30", sc["days"])

    def test_nothing_fetched(self):
        sc, _ = se.schedule_from_days({}, {"2026-11-23"}, START)
        self.assertIsNone(sc)

    def test_horizon(self):
        self.assertEqual(se.horizon_days(date(2026, 11, 25)), (date(2027, 4, 15) - START).days + 1)
        self.assertLessEqual(se.horizon_days(date(2026, 7, 1)), se.MAX_SCHEDULE_DAYS)
        self.assertEqual(se.horizon_days(date(2027, 4, 14)), 28)  # last week of the season: at least the weekly window

if __name__ == "__main__":
    unittest.main()
