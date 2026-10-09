-- Latest ownership snapshot. Policy (Daniel, 2026-10-09): YAHOO is primary, ESPN is the fallback.
--   * A player gets Yahoo's value if Yahoo's latest snapshot is no more than 3 days old.
--   * Otherwise (Yahoo stale or the player is absent from it) the value comes from ESPN's latest snapshot.
-- While the Yahoo app is dead (frozen at 2026-02-02) this therefore serves ESPN for everyone.
-- Same 4 columns, order and types as before, so the API needs no change.
-- Rollback: nba_data.v_player_ownership_latest.yahoo_only.sql
CREATE OR REPLACE VIEW `fantasy-survivor-app.nba_data.v_player_ownership_latest` AS
WITH yahoo AS (
  SELECT player_id, player_name, roster_pct, snapshot_date
  FROM `fantasy-survivor-app.nba_data.player_ownership`
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `fantasy-survivor-app.nba_data.player_ownership`)
    AND snapshot_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 3 DAY)
),
espn AS (
  SELECT player_id, player_name, roster_pct, snapshot_date
  FROM `fantasy-survivor-app.nba_data.player_ownership_espn`
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `fantasy-survivor-app.nba_data.player_ownership_espn`)
)
SELECT
  COALESCE(y.player_id, e.player_id)       AS player_id,
  COALESCE(y.player_name, e.player_name)   AS player_name,
  COALESCE(y.roster_pct, e.roster_pct)     AS roster_pct,
  COALESCE(y.snapshot_date, e.snapshot_date) AS snapshot_date
FROM yahoo y
FULL OUTER JOIN espn e ON y.player_id = e.player_id;
