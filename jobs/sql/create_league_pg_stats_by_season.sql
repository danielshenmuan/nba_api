-- NOTE: duplicates infra/bq/sql/create_league_pg_stats_by_season.sql so a deploy of only jobs/ still bundles it.
-- Rebuilds ONE season of league_pg_stats_by_season from player_daily_game_stats_espn and
-- leaves every other season untouched (the old version replaced the whole table, so the
-- first run of a new season erased the previous one).
--
-- Parameter: @season_start_year INT64   e.g. 2025 = the 2025-26 season.
--            NULL = the season in progress today (US Eastern).
-- Methodology is unchanged from the SportsBlaze-era query so z-scores stay on the same scale.
-- The fix: fga/fta now come straight from the ESPN table. The old LEFT JOIN to
-- player_historical_game_stats_p never matched, so s_fg_imp / s_ft_imp were 0 and every
-- FG% / FT% z-score in /v1/player_baselines was NULL.
DECLARE today DATE DEFAULT CURRENT_DATE('America/New_York');
DECLARE y1 INT64 DEFAULT IFNULL(@season_start_year,
  IF(EXTRACT(MONTH FROM today) >= 10, EXTRACT(YEAR FROM today), EXTRACT(YEAR FROM today) - 1));
DECLARE season_label STRING DEFAULT FORMAT('%d-%02d', y1, MOD(y1 + 1, 100));

BEGIN TRANSACTION;

DELETE FROM `fantasy-survivor-app.nba_data.league_pg_stats_by_season` WHERE season = season_label;

INSERT INTO `fantasy-survivor-app.nba_data.league_pg_stats_by_season` (season, means, stds, usage_q101)
WITH season_games AS (
  SELECT
    player_id, game_date, minutes,
    pts, reb, ast, stl, blk, fg3m, fg_pct, ft_pct, turnovers, fga, fta
  FROM `fantasy-survivor-app.nba_data.player_daily_game_stats_espn`
  WHERE game_date BETWEEN DATE(y1, 10, 1) AND DATE(y1 + 1, 6, 30)
    AND minutes > 0
),
per_player_pg AS (
  SELECT
    player_id,
    AVG(pts)  AS pts,  AVG(reb) AS reb, AVG(ast) AS ast, AVG(stl) AS stl, AVG(blk) AS blk,
    AVG(fg3m) AS fg3m, AVG(fg_pct) AS fg_pct, AVG(ft_pct) AS ft_pct, AVG(turnovers) AS turnovers,
    AVG(COALESCE(fga,0)) AS fga, AVG(COALESCE(fta,0)) AS fta,
    AVG(minutes) AS minutes,
    SAFE_DIVIDE(AVG(COALESCE(fga,0)) + AVG(COALESCE(fta,0)), NULLIF(AVG(minutes),0)) AS usage_per_min
  FROM season_games
  GROUP BY player_id
),
league_means AS (
  SELECT
    AVG(pts) AS m_pts,   AVG(reb) AS m_reb,   AVG(ast) AS m_ast,
    AVG(stl) AS m_stl,   AVG(blk) AS m_blk,   AVG(fg3m) AS m_fg3m,
    AVG(fg_pct) AS m_fg_pct, AVG(ft_pct) AS m_ft_pct, AVG(turnovers) AS m_tov
  FROM per_player_pg
),
league_std AS (
  SELECT
    STDDEV_POP(pts)  AS s_pts,   STDDEV_POP(reb) AS s_reb, STDDEV_POP(ast) AS s_ast,
    STDDEV_POP(stl)  AS s_stl,   STDDEV_POP(blk) AS s_blk, STDDEV_POP(fg3m) AS s_fg3m,
    STDDEV_POP(fg_pct) AS s_fg_pct, STDDEV_POP(ft_pct) AS s_ft_pct, STDDEV_POP(turnovers) AS s_tov,
    STDDEV_POP((fg_pct - (SELECT m_fg_pct FROM league_means)) * fga) AS s_fg_imp,
    STDDEV_POP((ft_pct - (SELECT m_ft_pct FROM league_means)) * fta) AS s_ft_imp
  FROM per_player_pg
),
usage_quantiles AS (
  SELECT APPROX_QUANTILES(usage_per_min, 101) AS usage_q101 FROM per_player_pg
)
SELECT
  season_label AS season,
  (SELECT AS STRUCT * FROM league_means) AS means,
  (SELECT AS STRUCT * FROM league_std)   AS stds,
  (SELECT usage_q101 FROM usage_quantiles) AS usage_q101
-- no rows in the season yet (e.g. preseason) -> insert nothing rather than a row of NULLs
FROM (SELECT 1) WHERE EXISTS (SELECT 1 FROM season_games);

COMMIT TRANSACTION;
