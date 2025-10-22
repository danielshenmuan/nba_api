CREATE TABLE IF NOT EXISTS `fantasy-survivor-app.nba_data.player_daily_game_stats_p` (
  game_date DATE,
  game_id INT64,
  player_id INT64,
  player_name STRING,
  minutes FLOAT64,
  pts FLOAT64,
  reb FLOAT64,
  ast FLOAT64,
  stl FLOAT64,
  blk FLOAT64,
  fg3m FLOAT64,
  fg_pct FLOAT64,
  ft_pct FLOAT64,
  turnovers FLOAT64,
  z_score FLOAT64,
  season STRING
)
PARTITION BY DATE(game_date)
CLUSTER BY player_id, season;
