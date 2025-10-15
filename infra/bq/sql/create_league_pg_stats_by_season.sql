-- Creates/refreshes season-level league means/stds and usage quantiles
CREATE OR REPLACE TABLE `fantasy-survivor-app.nba_data.league_pg_stats_by_season` AS
WITH seasons AS (
  -- add rows for each season you want to support
  SELECT "2024-25" AS season, DATE "2024-10-01" AS start_date, DATE "2025-06-30" AS end_date
),
season_games AS (
  SELECT
    d.player_id, d.game_date, d.minutes,
    d.pts, d.reb, d.ast, d.stl, d.blk, d.fg3m, d.fg_pct, d.ft_pct, d.turnovers,
    h.fg_attempts AS fga, h.ft_attempts AS fta
  FROM `fantasy-survivor-app.nba_data.player_daily_game_stats_p` d
  LEFT JOIN `fantasy-survivor-app.nba_data.player_historical_game_stats_p` h
    USING (player_id, game_date)
  JOIN seasons s
    ON d.game_date BETWEEN s.start_date AND s.end_date
  WHERE d.minutes > 0
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
impacts AS (
  SELECT
    (fg_pct - (SELECT m_fg_pct FROM league_means)) * fga AS fg_impact,
    (ft_pct - (SELECT m_ft_pct FROM league_means)) * fta AS ft_impact
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
  (SELECT season FROM seasons) AS season,
  (SELECT AS STRUCT * FROM league_means) AS means,
  (SELECT AS STRUCT * FROM league_std)   AS stds,
  (SELECT usage_q101 FROM usage_quantiles) AS usage_q101;
