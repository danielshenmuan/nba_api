CREATE OR REPLACE VIEW `fantasy-survivor-app.nba_data.player_games_next_7_days` AS
WITH player_teams AS (
  -- Extract latest team mapping for each player from the daily stats table
  -- This handles trades naturally as it uses the most recent game data
  SELECT DISTINCT
    player_id,
    player_name,
    team_id
  FROM `fantasy-survivor-app.nba_data.player_daily_game_stats_p`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY game_date DESC) = 1
),
schedule_7_days AS (
  -- Get all games in the next 7 days (including today)
  SELECT
    game_id,
    game_date,
    home_team_id,
    away_team_id
  FROM `fantasy-survivor-app.nba_data.nba_schedule`
  WHERE game_date BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 6 DAY)
),
team_games AS (
  -- Count games per team in the 7-day window
  SELECT team_id, COUNT(*) as games_next_7_days
  FROM (
    SELECT home_team_id as team_id FROM schedule_7_days
    UNION ALL
    SELECT away_team_id as team_id FROM schedule_7_days
  )
  GROUP BY team_id
)
SELECT
  p.player_id,
  p.player_name,
  p.team_id,
  COALESCE(t.games_next_7_days, 0) as games_next_7_days
FROM player_teams p
LEFT JOIN team_games t ON p.team_id = t.team_id;
