-- *** player_id here is a SportsBlaze UUID (STRING), NOT an NBA player_id. ***
-- It will not join to player_historical_game_stats, player_ownership, or the
-- public API's ?player_id= contract. Query nba_data.v_player_daily_game_stats
-- instead, which remaps it via nba_data.player_id_crosswalk. See docs/DATA-SOURCES.md

CREATE TABLE IF NOT EXISTS `fantasy-survivor-app.nba_data.player_daily_game_stats_p` (
  game_date DATE,
  game_id STRING,
  player_id STRING,
  player_name STRING,
  team_id STRING,
  team_abbr STRING,
  team_city STRING,
  team_name STRING,
  team_slug STRING,
  position STRING,
  comment STRING,
  jersey_num STRING,
  minutes FLOAT64,
  fgm INT64,
  fga INT64,
  fg_pct FLOAT64,
  fg3m INT64,
  fg3a INT64,
  fg3_pct FLOAT64,
  ftm INT64,
  fta INT64,
  ft_pct FLOAT64,
  pts INT64,
  reb INT64,
  ast INT64,
  stl INT64,
  blk INT64,
  turnovers INT64,
  pf INT64,
  dreb INT64,
  oreb INT64,
  plus_minus INT64,
  z_score FLOAT64,
  season STRING
)
PARTITION BY DATE(game_date)
CLUSTER BY player_id, season;
