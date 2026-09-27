CREATE TABLE IF NOT EXISTS `fantasy-survivor-app.nba_data.nba_schedule` (
  game_id STRING NOT NULL,
  game_date DATE NOT NULL,
  home_team_id STRING,
  away_team_id STRING,
  home_team_name STRING,
  away_team_name STRING,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
CLUSTER BY game_date;
