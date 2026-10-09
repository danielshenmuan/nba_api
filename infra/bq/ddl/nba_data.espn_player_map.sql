-- ESPN athlete id -> NBA person id. Fix a wrong or missing mapping with a plain UPDATE.
-- nba_player_id NULL = unresolved (rows then carry player_id = -espn_id).
CREATE TABLE IF NOT EXISTS `fantasy-survivor-app.nba_data.espn_player_map` (
  espn_player_id INT64 NOT NULL,
  nba_player_id  INT64,
  espn_name      STRING,
  method         STRING OPTIONS(description='exact_norm_name | manual | surrogate'),
  updated_at     TIMESTAMP
)
CLUSTER BY espn_player_id;
