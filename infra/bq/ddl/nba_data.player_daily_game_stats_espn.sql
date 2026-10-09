-- Daily player box scores sourced from ESPN's unofficial site API.
-- Same 34 columns as player_daily_game_stats_p, but with the NBA-era TYPES:
-- player_id is the NBA integer person id (or -espn_id when unresolved; see
-- espn_player_map), and game_id is the ESPN event id (numeric).
-- Regular season only (ESPN season type 2), matching the NBA-era table.
CREATE TABLE IF NOT EXISTS `fantasy-survivor-app.nba_data.player_daily_game_stats_espn` (
  game_date   DATE,
  game_id     INT64   OPTIONS(description='ESPN event id (not the NBA game id)'),
  player_id   INT64   OPTIONS(description='NBA person id; negative = -espn_id when unresolved'),
  player_name STRING,
  team_id     INT64   OPTIONS(description='NBA team id'),
  team_abbr   STRING  OPTIONS(description='NBA tricode'),
  team_city   STRING,
  team_name   STRING,
  team_slug   STRING,
  position    STRING,
  comment     STRING,
  jersey_num  STRING,
  minutes     FLOAT64 OPTIONS(description='Whole minutes (ESPN does not publish seconds)'),
  fgm INT64, fga INT64, fg_pct FLOAT64,
  fg3m INT64, fg3a INT64, fg3_pct FLOAT64,
  ftm INT64, fta INT64, ft_pct FLOAT64,
  pts INT64, reb INT64, ast INT64, stl INT64, blk INT64, turnovers INT64, pf INT64,
  dreb INT64, oreb INT64, plus_minus INT64,
  z_score FLOAT64,
  season STRING
)
PARTITION BY game_date
CLUSTER BY player_id, season;
