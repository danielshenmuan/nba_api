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
FROM `fantasy-survivor-app.nba_data.player_daily_game_stats_p` d
JOIN `fantasy-survivor-app.nba_data.player_id_crosswalk` x
  ON x.sb_uuid = d.player_id
WHERE x.nba_player_id IS NOT NULL;
