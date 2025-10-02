CREATE TABLE `fantasy-survivor-app.nba_data.player_daily_game_stats_p`
(
  game_id INT64,
  player_id INT64,
  player_name STRING,
  min FLOAT64,
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
  z_score FLOAT64,
  game_date DATE
)
PARTITION BY game_date
CLUSTER BY player_id;
