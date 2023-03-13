from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
from nba_api.stats.static import teams
import pandas as pd
from requests.exceptions import Timeout


def get_clips_id():
    clips = teams.find_team_by_abbreviation('lac')
    clips_id = clips["id"]

    return clips_id


def get_current_season_game_ids(clips_id):
    # default param for LeagueGameFinder is current season
    try:
        gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=clips_id, timeout=60)
    except Timeout:
        print('Timeout caught')

    games = gamefinder.get_data_frames()[0]
    current_season_id = games['SEASON_ID'].iloc[0]
    current_season_games = games[games.SEASON_ID == current_season_id]
    current_season_games = current_season_games.loc[current_season_games['GAME_DATE'] > '2022-10-01']

    current_season_game_ids = list(current_season_games['GAME_ID'])
    # this contains summer league and preseason games
    return current_season_game_ids


def get_game_player_stats(clips_id, game_id):
    try:
        game_sets = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id, timeout=60)
    except Timeout:
        print('Timeout caught')

    game_df = game_sets.get_data_frames()[0]
    game_df = game_df.loc[game_df['TEAM_ID'] == clips_id]
    game_df = game_df.drop(['TEAM_ABBREVIATION', 'TEAM_CITY', 'NICKNAME', 'COMMENT'], axis=1)

    return game_df


def season_to_date_team(clips_id, current_season_game_ids):
    # get the dataframes of all the games then combine them into one
    game_df = pd.DataFrame()
    tracker = 0
    for game_id in current_season_game_ids:
        try:
            game_df = pd.concat([game_df, get_game_player_stats(clips_id, game_id)], axis=0)
        except Timeout:
            print('Timeout caught')

        tracker += 1
        print(tracker)

    return game_df


def format_for_db(season_to_date_team_df):

    for row in season_to_date_team_df:
        minutes_string = str(season_to_date_team_df.iloc[row, 'MIN'])
        minutes_list = minutes_string.split(':')

        hours = minutes_list[0] * 60

        int_minutes = hours + minutes_list[1]

    return int_minutes


def main():
    clips_id = get_clips_id()
    current_season_game_ids = get_current_season_game_ids(clips_id)
    season_to_date_team_df = season_to_date_team(clips_id, current_season_game_ids)
    season_to_date_team_df = format_for_db(season_to_date_team_df)

    return


if __name__ == '__main__':
    main()
