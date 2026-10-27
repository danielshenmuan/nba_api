-- "Social score": the per-game score used to RANK every social post (爆氣榜, 爆胎榜, the banner's best performer, 自由市場).
-- Internal only: it never appears in a post or image. Decided with Daniel on 2026-10-26.
--
-- Same math as z_score (the Fantasy Value) except steals, blocks and 3-pointers count at HALF weight, so a 4-steal / 9-point night
-- no longer reads as 爆氣: points, rebounds and assists carry the ranking. FG% and FT% (scaled by attempts) and turnovers (x -0.2)
-- are unchanged.
--   per category: (value - mean) / std      means/stds = the constants in jobs/espn_ingest.py (WEIGHTED_MEAN / WEIGHTED_STD)
--   weights:      PTS 1, REB 1, AST 1, STL 0.5, BLK 0.5, 3PM 0.5, FG% 1 x (FGA/20), FT% 1 x (FTA/8), TO -0.2
CREATE OR REPLACE VIEW `fantasy-survivor-app.nba_data.v_player_daily_game_stats_social` AS
SELECT
  s.*,
  ROUND(
      (s.pts - 11.69) / 7.23
    + (s.reb - 4.32)  / 2.51
    + (s.ast - 2.76)  / 2.09
    + 0.5 * (s.stl  - 0.75) / 0.38
    + 0.5 * (s.blk  - 0.50) / 0.45
    + 0.5 * (s.fg3m - 1.28) / 0.95
    + (IFNULL(s.fg_pct, 0) - 0.47) / 0.082 * (s.fga / 20.0)
    + (IFNULL(s.ft_pct, 0) - 0.75) / 0.124 * (s.fta / 8.0)
    - 0.2 * (s.turnovers - 1.33) / 0.85
  , 3) AS social_score
FROM `fantasy-survivor-app.nba_data.v_player_daily_game_stats` AS s;
