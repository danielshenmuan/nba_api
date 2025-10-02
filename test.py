from nba_api.stats.endpoints import LeagueGameLog, BoxScoreTraditionalV2
import pandas as pd


def get_game_ids(date_str: str):
    """
    Get game IDs for a specific date (YYYY-MM-DD) using LeagueGameLog.
    """
    # Derive season string from date
    year = int(date_str[:4])
    month = int(date_str[5:7])
    if month >= 10:  # season starts in October
        season = f"{year}-{str(year + 1)[2:]}"
    else:
        season = f"{year - 1}-{str(year)[2:]}"

    game_log = LeagueGameLog(
        season=season,
        season_type_all_star="Regular Season",
        player_or_team_abbreviation="T",
        timeout=30
    )
    df = game_log.get_data_frames()[0]
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    target_date = pd.to_datetime(date_str)
    games_df = df[df["GAME_DATE"] == target_date]
    return games_df["GAME_ID"].unique().tolist()


def get_boxscore(game_id: str) -> pd.DataFrame:
    """
    Get player stats for a specific game.
    """
    box = BoxScoreTraditionalV2(game_id=game_id, timeout=30)
    return box.get_data_frames()[0]


def get_day_boxscores(date_str: str) -> pd.DataFrame:
    """
    Get player stats for all games on a given date.
    """
    game_ids = get_game_ids(date_str)
    if not game_ids:
        return pd.DataFrame()

    frames = []
    for gid in game_ids:
        try:
            df = get_boxscore(gid)
            frames.append(df)
        except Exception as e:
            print(f"Skipping {gid}: {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


if __name__ == "__main__":
    # Example run
    date = "2025-01-02"
    game_ids = get_game_ids(date)
    print("Game IDs:", game_ids)

    all_stats = get_day_boxscores(date)
    print(all_stats.head())

