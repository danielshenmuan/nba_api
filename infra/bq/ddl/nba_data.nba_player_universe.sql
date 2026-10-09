-- NBA person ids and names used to resolve ESPN athletes. Seeded by
-- `espn_ingest.py seed-universe` from nba_api's offline static list plus every
-- (player_id, player_name) already present in the NBA-era BigQuery tables.
CREATE TABLE IF NOT EXISTS `fantasy-survivor-app.nba_data.nba_player_universe` (
  nba_id          INT64 NOT NULL,
  full_name       STRING,
  norm_name       STRING OPTIONS(description='lowercased, accents/punctuation/suffix removed'),
  is_active       BOOL,
  in_bq_history   BOOL   OPTIONS(description='appears in the NBA-era stats/ownership tables')
);
