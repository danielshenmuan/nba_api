-- Same data as v_player_ownership_latest, in this view's original column order.
CREATE OR REPLACE VIEW `fantasy-survivor-app.nba_data.v_player_roster_pct_latest` AS
SELECT player_id, player_name, snapshot_date, roster_pct
FROM `fantasy-survivor-app.nba_data.v_player_ownership_latest`;
