CREATE OR REPLACE VIEW `fantasy-survivor-app.nba_data.player_ownership_latest` AS
WITH ranked AS (
  SELECT
    snapshot_date,
    ingested_at,
    league_id,
    league_key,
    game_code,
    game_id,
    player_id,
    player_key,
    editorial_player_key,
    player_first_name,
    player_last_name,
    player_full_name,
    editorial_team_abbr,
    editorial_team_full_name,
    primary_position,
    eligible_positions,
    position_type,
    status,
    ownership_type,
    owner_team_key,
    owner_team_name,
    ownership_display_date,
    ownership_waiver_date,
    percent_owned,
    percent_owned_delta,
    percent_owned_coverage_type,
    percent_owned_week,
    ROW_NUMBER() OVER (
      PARTITION BY player_id, player_key
      ORDER BY snapshot_date DESC, ingested_at DESC
    ) AS row_num
  FROM `fantasy-survivor-app.nba_data.player_ownership_p`
)
SELECT
  snapshot_date,
  ingested_at,
  league_id,
  league_key,
  game_code,
  game_id,
  player_id,
  player_key,
  editorial_player_key,
  player_first_name,
  player_last_name,
  player_full_name,
  editorial_team_abbr,
  editorial_team_full_name,
  primary_position,
  eligible_positions,
  position_type,
  status,
  ownership_type,
  owner_team_key,
  owner_team_name,
  ownership_display_date,
  ownership_waiver_date,
  percent_owned,
  percent_owned_delta,
  percent_owned_coverage_type,
  percent_owned_week
FROM ranked
WHERE row_num = 1;
