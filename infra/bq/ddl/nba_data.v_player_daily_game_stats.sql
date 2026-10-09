-- LIVE definition (cut over from SportsBlaze to ESPN on 2026-10-09).
--
-- Same 35 columns, same order and same TYPES as the SportsBlaze-era view, so the FastAPI
-- service and n8n see no contract change: game_id and team_id are STRING again.
-- Unresolved players are INCLUDED with a negative surrogate player_id (-espn_id) so a
-- breakout call-up still shows up in daily leaders.
--
-- Fallback to SportsBlaze: run nba_data.v_player_daily_game_stats.sportsblaze_fallback.sql
CREATE OR REPLACE VIEW `fantasy-survivor-app.nba_data.v_player_daily_game_stats` AS
SELECT
  player_id,
  CAST(NULL AS STRING)       AS source_player_id,
  game_date,
  CAST(game_id AS STRING)    AS game_id,
  player_name,
  CAST(team_id AS STRING)    AS team_id,
  team_abbr, team_city, team_name, team_slug,
  position, comment, jersey_num,
  minutes,
  fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct,
  pts, reb, ast, stl, blk, turnovers, pf, dreb, oreb, plus_minus,
  z_score, season
FROM `fantasy-survivor-app.nba_data.player_daily_game_stats_espn`;
