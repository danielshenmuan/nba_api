-- Daily ESPN Fantasy ownership snapshot. First four columns match
-- player_ownership (Yahoo). NOTE: ESPN's population differs from Yahoo's, so
-- the numbers are correlated with, but not equal to, Yahoo roster %.
-- ESPN exposes only the CURRENT value, so history accrues from the first run.
CREATE TABLE IF NOT EXISTS `fantasy-survivor-app.nba_data.player_ownership_espn` (
  player_id      INT64,
  player_name    STRING,
  roster_pct     FLOAT64 OPTIONS(description='ESPN percentOwned'),
  snapshot_date  DATE,
  pct_change     FLOAT64 OPTIONS(description='ESPN percentChange (day over day)'),
  pct_started    FLOAT64 OPTIONS(description='ESPN percentStarted'),
  injury_status  STRING  OPTIONS(description='ESPN injuryStatus, e.g. ACTIVE / DAY_TO_DAY / OUT'),
  espn_player_id INT64
)
PARTITION BY snapshot_date
CLUSTER BY player_id;
