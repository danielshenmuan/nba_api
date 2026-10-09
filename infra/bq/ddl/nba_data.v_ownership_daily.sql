-- Every ownership source, side by side, with a PERCENTILE RANK inside each source and day.
-- Percentile rank (0 = least owned, 1 = most owned) is comparable across platforms in a way the raw
-- percentage is not, so the analytics layer should prefer pct_rank and show the source by name.
-- injury_status exists for ESPN only today (Yahoo's ingest does not capture it yet).
CREATE OR REPLACE VIEW `fantasy-survivor-app.nba_data.v_ownership_daily` AS
SELECT 'yahoo' AS source, player_id, player_name, snapshot_date, roster_pct,
       CAST(NULL AS FLOAT64) AS pct_change, CAST(NULL AS STRING) AS injury_status,
       PERCENT_RANK() OVER (PARTITION BY snapshot_date ORDER BY roster_pct) AS pct_rank
FROM `fantasy-survivor-app.nba_data.player_ownership`
UNION ALL
SELECT 'espn', player_id, player_name, snapshot_date, roster_pct, pct_change, injury_status,
       PERCENT_RANK() OVER (PARTITION BY snapshot_date ORDER BY roster_pct)
FROM `fantasy-survivor-app.nba_data.player_ownership_espn`;
