-- ROLLBACK: the definition before 2026-10-09 (Yahoo only). Run this to restore it.
CREATE OR REPLACE VIEW `fantasy-survivor-app.nba_data.v_player_ownership_latest` AS
WITH latest AS (
  SELECT MAX(snapshot_date) AS snapshot_date
  FROM `fantasy-survivor-app.nba_data.player_ownership`
)
SELECT po.player_id, po.player_name, po.roster_pct, po.snapshot_date
FROM `fantasy-survivor-app.nba_data.player_ownership` po
JOIN latest USING (snapshot_date);
