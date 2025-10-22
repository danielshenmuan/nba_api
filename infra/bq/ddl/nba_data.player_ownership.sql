-- infra/bq/ddl/nba_data.player_ownership.sql
CREATE TABLE IF NOT EXISTS `fantasy-survivor-app.nba_data.player_ownership` (
  player_id INT64,
  player_name STRING,
  snapshot_date DATE,   -- date of the pull
  roster_pct FLOAT64    -- 0–100
)
PARTITION BY snapshot_date
CLUSTER BY player_id;
