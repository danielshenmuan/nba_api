-- SportsBlaze UUID <-> NBA player_id crosswalk.
-- Created 2026-09-26 to repair the Feb-2026 provider switch, which moved daily
-- stats onto SportsBlaze UUIDs while the rest of the stack (historical stats,
-- Yahoo ownership, the public API contract) stayed on NBA integer ids.
--
-- Seeded by exact player_name match: 494 matched, 38 unresolved, 0 ambiguous.
-- The 38 are late-season call-ups (1.81% of rows, ~0% fantasy-relevant).
CREATE TABLE IF NOT EXISTS `fantasy-survivor-app.nba_data.player_id_crosswalk` (
  sb_uuid        STRING NOT NULL OPTIONS(description='SportsBlaze player UUID'),
  nba_player_id  INT64           OPTIONS(description='NBA.com player id. NULL = unresolved'),
  player_name    STRING          OPTIONS(description='Name as it appears in SportsBlaze'),
  match_method   STRING          OPTIONS(description='exact_name | manual | draft_match | unresolved'),
  confidence     FLOAT64         OPTIONS(description='1.0 exact, <1 fuzzy/manual'),
  created_at     TIMESTAMP,
  updated_at     TIMESTAMP
)
CLUSTER BY sb_uuid;

-- Reseed (idempotent for the exact-name portion):
-- INSERT INTO `...player_id_crosswalk`
-- WITH nba AS (
--   SELECT DISTINCT player_id AS nba_id, player_name FROM `...player_daily_game_stats` WHERE player_name IS NOT NULL
--   UNION DISTINCT SELECT DISTINCT player_id, player_name FROM `...player_ownership` WHERE player_name IS NOT NULL),
-- sb AS (SELECT DISTINCT player_id AS sb_uuid, player_name FROM `...player_daily_game_stats_p`)
-- SELECT sb.sb_uuid, MIN(nba.nba_id), sb.player_name,
--        IF(COUNT(nba.nba_id)>0,'exact_name','unresolved'),
--        IF(COUNT(nba.nba_id)>0,1.0,0.0), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
-- FROM sb LEFT JOIN nba ON nba.player_name = sb.player_name GROUP BY 1,3;
