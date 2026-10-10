-- ROLLBACK / FALLBACK: the pre-2026-10-09 definition (SportsBlaze UUIDs remapped via the crosswalk).
-- z_score is RECOMPUTED here at turnover weight 0.2 so the scale matches the live ESPN data (the SportsBlaze
-- ingest stored it at weight 1). Run this to switch the API back to SportsBlaze data. The live definition is
-- nba_data.v_player_daily_game_stats.sql (ESPN).

-- Daily stats with player_id remapped from SportsBlaze UUID to NBA player_id.
-- Created 2026-09-26.
--
-- *** READ THIS BEFORE QUERYING DAILY STATS ***
-- Always read through this view, never `player_daily_game_stats_p` directly.
-- The raw table's player_id is a SportsBlaze UUID (STRING) and will not join to
-- player_historical_game_stats, player_ownership, or the public API's
-- ?player_id= contract, all of which use NBA integer ids.
--
-- Rows whose player is not yet in player_id_crosswalk are EXCLUDED
-- (5,868 of 5,976 exposed as of 2026-09-26 = 98.19%).
CREATE OR REPLACE VIEW `fantasy-survivor-app.nba_data.v_player_daily_game_stats` AS
SELECT
  x.nba_player_id AS player_id,
  d.player_id     AS source_player_id,
  d.* EXCEPT(player_id)
  REPLACE(ROUND((d.pts-11.69)/7.23 + (d.reb-4.32)/2.51 + (d.ast-2.76)/2.09 + (d.stl-0.75)/0.38 + (d.blk-0.50)/0.45 + (d.fg3m-1.28)/0.95
    + ((d.fg_pct-0.47)/0.082)*(d.fga/20.0) + ((d.ft_pct-0.75)/0.124)*(d.fta/8.0) - 0.2*((d.turnovers-1.33)/0.85), 3) AS z_score)
FROM `fantasy-survivor-app.nba_data.player_daily_game_stats_p` d
JOIN `fantasy-survivor-app.nba_data.player_id_crosswalk` x
  ON x.sb_uuid = d.player_id
WHERE x.nba_player_id IS NOT NULL;
