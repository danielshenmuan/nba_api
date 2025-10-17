from google.cloud import bigquery

PROJECT = "fantasy-survivor-app"
LOC = "northamerica-northeast1"
TABLE_DAILY = "fantasy-survivor-app.nba_data.player_daily_game_stats_p"
TABLE_HIST  = "fantasy-survivor-app.nba_data.player_historical_game_stats_p"
TABLE_PRE   = "fantasy-survivor-app.nba_data.league_pg_stats_by_season"

def get_player_baselines_v1(player_id: int, season: str, window: int = 5):
    client = bigquery.Client(project=PROJECT)

    sql = f"""
    DECLARE season STRING DEFAULT @season;
    DECLARE y1 INT64 DEFAULT CAST(SPLIT(season, '-')[OFFSET(0)] AS INT64);
    DECLARE s_start DATE DEFAULT DATE(y1, 10, 1);
    DECLARE s_end   DATE DEFAULT DATE(y1 + 1, 6, 30);
    DECLARE w INT64 DEFAULT @window;

    WITH pre AS (
      SELECT * FROM `{TABLE_PRE}` WHERE season = @season
    ),
    season_games AS (
      SELECT
        d.player_id, d.player_name, d.team_abbr, d.game_date, d.minutes,
        d.pts, d.reb, d.ast, d.stl, d.blk, d.fg3m, d.fg_pct, d.ft_pct, d.turnovers,
        h.fg_attempts AS fga, h.ft_attempts AS fta
      FROM `{TABLE_DAILY}` d
      LEFT JOIN `{TABLE_HIST}` h USING (player_id, game_date)
      WHERE d.player_id = @pid
        AND d.season = @season          -- fast equality on cluster + prunes by date below
        AND d.game_date BETWEEN s_start AND s_end
        AND d.minutes > 0
    ),
    per_player_pg AS (
      SELECT
        ANY_VALUE(player_id) AS player_id,
        ANY_VALUE(player_name) AS player_name,
        ANY_VALUE(team_abbr)   AS team,
        AVG(pts)  AS pts,  AVG(reb) AS reb, AVG(ast) AS ast, AVG(stl) AS stl, AVG(blk) AS blk,
        AVG(fg3m) AS fg3m, AVG(fg_pct) AS fg_pct, AVG(ft_pct) AS ft_pct, AVG(turnovers) AS turnovers,
        AVG(COALESCE(fga,0)) AS fga, AVG(COALESCE(fta,0)) AS fta,
        AVG(minutes) AS minutes,
        SAFE_DIVIDE(AVG(COALESCE(fga,0)) + AVG(COALESCE(fta,0)), NULLIF(AVG(minutes),0)) AS usage_per_min,
        COUNT(*) AS gp
      FROM season_games
    ),
    lastN AS (
      SELECT * EXCEPT(rn) FROM (
        SELECT sg.*, ROW_NUMBER() OVER (ORDER BY game_date DESC) rn
        FROM season_games sg
      )
      WHERE rn <= w
    ),
    per_player_l5 AS (
      SELECT
        AVG(pts)  AS pts,  AVG(reb) AS reb, AVG(ast) AS ast, AVG(stl) AS stl, AVG(blk) AS blk,
        AVG(fg3m) AS fg3m, AVG(fg_pct) AS fg_pct, AVG(ft_pct) AS ft_pct, AVG(turnovers) AS turnovers,
        AVG(COALESCE(fga,0)) AS fga, AVG(COALESCE(fta,0)) AS fta,
        AVG(minutes) AS minutes,
        SAFE_DIVIDE(AVG(COALESCE(fga,0)) + AVG(COALESCE(fta,0)), NULLIF(AVG(minutes),0)) AS usage_per_min
      FROM lastN
    ),
    usage_percentiles AS (
      SELECT
        CAST(ARRAY_LENGTH( (SELECT ARRAY(SELECT v FROM UNNEST(pre.usage_q101) v WHERE v IS NOT NULL AND v <= pg.usage_per_min)) ) - 1 AS INT64) AS usage_proxy,
        CAST(ARRAY_LENGTH( (SELECT ARRAY(SELECT v FROM UNNEST(pre.usage_q101) v WHERE v IS NOT NULL AND v <= l5.usage_per_min)) ) - 1 AS INT64) AS usage_proxy_l5
      FROM per_player_pg pg, per_player_l5 l5, pre
    )
    SELECT
      pg.player_id, pg.player_name AS name, pg.team,
      pg.minutes AS minutes_season, l5.minutes AS minutes_l5,
      NULLIF(up.usage_proxy,   -1) AS usage_proxy,
      NULLIF(up.usage_proxy_l5,-1) AS usage_proxy_l5,

      STRUCT(pg.pts AS avg_season,
             SAFE_DIVIDE(pg.pts - pre.means.m_pts, NULLIF(pre.stds.s_pts,0)) AS z_season,
             l5.pts AS avg_l5,
             SAFE_DIVIDE(l5.pts - pre.means.m_pts, NULLIF(pre.stds.s_pts,0)) AS z_l5,
             (SAFE_DIVIDE(l5.pts - pre.means.m_pts, NULLIF(pre.stds.s_pts,0)) - SAFE_DIVIDE(pg.pts - pre.means.m_pts, NULLIF(pre.stds.s_pts,0))) AS z_delta) AS PTS,

      STRUCT(pg.reb AS avg_season,
             SAFE_DIVIDE(pg.reb - pre.means.m_reb, NULLIF(pre.stds.s_reb,0)) AS z_season,
             l5.reb AS avg_l5,
             SAFE_DIVIDE(l5.reb - pre.means.m_reb, NULLIF(pre.stds.s_reb,0)) AS z_l5,
             (SAFE_DIVIDE(l5.reb - pre.means.m_reb, NULLIF(pre.stds.s_reb,0)) - SAFE_DIVIDE(pg.reb - pre.means.m_reb, NULLIF(pre.stds.s_reb,0))) AS z_delta) AS REB,

      STRUCT(pg.ast AS avg_season,
             SAFE_DIVIDE(pg.ast - pre.means.m_ast, NULLIF(pre.stds.s_ast,0)) AS z_season,
             l5.ast AS avg_l5,
             SAFE_DIVIDE(l5.ast - pre.means.m_ast, NULLIF(pre.stds.s_ast,0)) AS z_l5,
             (SAFE_DIVIDE(l5.ast - pre.means.m_ast, NULLIF(pre.stds.s_ast,0)) - SAFE_DIVIDE(pg.ast - pre.means.m_ast, NULLIF(pre.stds.s_ast,0))) AS z_delta) AS AST,

      STRUCT(pg.stl AS avg_season,
             SAFE_DIVIDE(pg.stl - pre.means.m_stl, NULLIF(pre.stds.s_stl,0)) AS z_season,
             l5.stl AS avg_l5,
             SAFE_DIVIDE(l5.stl - pre.means.m_stl, NULLIF(pre.stds.s_stl,0)) AS z_l5,
             (SAFE_DIVIDE(l5.stl - pre.means.m_stl, NULLIF(pre.stds.s_stl,0)) - SAFE_DIVIDE(pg.stl - pre.means.m_stl, NULLIF(pre.stds.s_stl,0))) AS z_delta) AS STL,

      STRUCT(pg.blk AS avg_season,
             SAFE_DIVIDE(pg.blk - pre.means.m_blk, NULLIF(pre.stds.s_blk,0)) AS z_season,
             l5.blk AS avg_l5,
             SAFE_DIVIDE(l5.blk - pre.means.m_blk, NULLIF(pre.stds.s_blk,0)) AS z_l5,
             (SAFE_DIVIDE(l5.blk - pre.means.m_blk, NULLIF(pre.stds.s_blk,0)) - SAFE_DIVIDE(pg.blk - pre.means.m_blk, NULLIF(pre.stds.s_blk,0))) AS z_delta) AS BLK,

      STRUCT(pg.fg3m AS avg_season,
             SAFE_DIVIDE(pg.fg3m - pre.means.m_fg3m, NULLIF(pre.stds.s_fg3m,0)) AS z_season,
             l5.fg3m AS avg_l5,
             SAFE_DIVIDE(l5.fg3m - pre.means.m_fg3m, NULLIF(pre.stds.s_fg3m,0)) AS z_l5,
             (SAFE_DIVIDE(l5.fg3m - pre.means.m_fg3m, NULLIF(pre.stds.s_fg3m,0)) - SAFE_DIVIDE(pg.fg3m - pre.means.m_fg3m, NULLIF(pre.stds.s_fg3m,0))) AS z_delta) AS `3PM`,

      STRUCT(pg.fg_pct AS avg_season,
             SAFE_DIVIDE((pg.fg_pct - pre.means.m_fg_pct) * pg.fga, NULLIF(pre.stds.s_fg_imp,0)) AS z_season,
             l5.fg_pct AS avg_l5,
             SAFE_DIVIDE((l5.fg_pct - pre.means.m_fg_pct) * l5.fga, NULLIF(pre.stds.s_fg_imp,0)) AS z_l5,
             (SAFE_DIVIDE((l5.fg_pct - pre.means.m_fg_pct) * l5.fga, NULLIF(pre.stds.s_fg_imp,0))
            -  SAFE_DIVIDE((pg.fg_pct - pre.means.m_fg_pct) * pg.fga, NULLIF(pre.stds.s_fg_imp,0))) AS z_delta) AS `FG%`,

      STRUCT(pg.ft_pct AS avg_season,
             SAFE_DIVIDE((pg.ft_pct - pre.means.m_ft_pct) * pg.fta, NULLIF(pre.stds.s_ft_imp,0)) AS z_season,
             l5.ft_pct AS avg_l5,
             SAFE_DIVIDE((l5.ft_pct - pre.means.m_ft_pct) * l5.fta, NULLIF(pre.stds.s_ft_imp,0)) AS z_l5,
             (SAFE_DIVIDE((l5.ft_pct - pre.means.m_ft_pct) * l5.fta, NULLIF(pre.stds.s_ft_imp,0))
            -  SAFE_DIVIDE((pg.ft_pct - pre.means.m_ft_pct) * pg.fta, NULLIF(pre.stds.s_ft_imp,0))) AS z_delta) AS `FT%`,

      STRUCT(pg.turnovers AS avg_season,
             SAFE_DIVIDE(pg.turnovers - pre.means.m_tov, NULLIF(pre.stds.s_tov,0)) AS z_season,
             l5.turnovers AS avg_l5,
             SAFE_DIVIDE(l5.turnovers - pre.means.m_tov, NULLIF(pre.stds.s_tov,0)) AS z_l5,
             (SAFE_DIVIDE(l5.turnovers - pre.means.m_tov, NULLIF(pre.stds.s_tov,0)) - SAFE_DIVIDE(pg.turnovers - pre.means.m_tov, NULLIF(pre.stds.s_tov,0))) AS z_delta) AS turnovers;
    """

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("pid", "INT64", player_id),
                bigquery.ScalarQueryParameter("season", "STRING", season),
                bigquery.ScalarQueryParameter("window", "INT64", window),
            ]
        ),
        location=LOC,
    )
    rows = list(job.result())
    if not rows:
        return None
    r = rows[0]

    # totals
    def z(obj, k): return float(obj[k]) if obj[k] is not None else 0.0
    z_total_season = (
        z(r["PTS"], "z_season") + z(r["REB"], "z_season") + z(r["AST"], "z_season") +
        z(r["STL"], "z_season") + z(r["BLK"], "z_season") + z(r["3PM"], "z_season") +
        z(r["FG%"], "z_season") + z(r["FT%"], "z_season") - z(r["TO"], "z_season")
    )
    z_total_l5 = (
        z(r["PTS"], "z_l5") + z(r["REB"], "z_l5") + z(r["AST"], "z_l5") +
        z(r["STL"], "z_l5") + z(r["BLK"], "z_l5") + z(r["3PM"], "z_l5") +
        z(r["FG%"], "z_l5") + z(r["FT%"], "z_l5") - z(r["TO"], "z_l5")
    )

    return {
        "player_id": r["player_id"],
        "name": r["name"],
        "team": r["team"],
        "position": None,
        "minutes_season": r["minutes_season"],
        "minutes_l5": r["minutes_l5"],
        "usage_proxy": r["usage_proxy"],
        "usage_proxy_l5": r["usage_proxy_l5"],
        "PTS": dict(r["PTS"]),
        "REB": dict(r["REB"]),
        "AST": dict(r["AST"]),
        "STL": dict(r["STL"]),
        "BLK": dict(r["BLK"]),
        "3PM": dict(r["3PM"]),
        "FG%": dict(r["FG%"]),
        "FT%": dict(r["FT%"]),
        "turnovers":  dict(r["turnovers"]),
        "z_total_season": round(z_total_season, 3),
        "z_total_l5": round(z_total_l5, 3),
        "z_total_delta": round(z_total_l5 - z_total_season, 3),
    }
