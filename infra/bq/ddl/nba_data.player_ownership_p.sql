CREATE TABLE `fantasy-survivor-app.nba_data.player_ownership_p`
(
  snapshot_date DATE NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  league_id STRING NOT NULL,
  league_key STRING NOT NULL,
  game_code STRING NOT NULL,
  game_id INT64,
  player_id INT64,
  player_key STRING,
  editorial_player_key STRING,
  player_first_name STRING,
  player_last_name STRING,
  player_full_name STRING,
  editorial_team_abbr STRING,
  editorial_team_full_name STRING,
  primary_position STRING,
  eligible_positions STRING,
  position_type STRING,
  status STRING,
  ownership_type STRING,
  owner_team_key STRING,
  owner_team_name STRING,
  ownership_display_date STRING,
  ownership_waiver_date STRING,
  percent_owned FLOAT64,
  percent_owned_delta FLOAT64,
  percent_owned_coverage_type STRING,
  percent_owned_week INT64
)
PARTITION BY snapshot_date
CLUSTER BY player_id, player_key;
