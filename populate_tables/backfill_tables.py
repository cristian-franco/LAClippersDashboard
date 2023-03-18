from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
from nba_api.stats.static import teams
import pandas as pd
from requests.exceptions import Timeout


# EXTRACT
def get_clips_id():
    clips_info = teams.find_team_by_abbreviation('lac')
    clips_id = clips_info["id"]

    return clips_id


def game_finder(team_id, int_timeout):
    # default param for LeagueGameFinder is current season
    try:
        games_found = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id, timeout=int_timeout)
    except Timeout:
        print('Timeout caught, retying...')
        games_found = game_finder(team_id, int_timeout)

    return games_found


def get_current_season_game_ids(clips_id):
    # contains summer league and preseason games
    games = game_finder(clips_id, 10).get_data_frames()[0]
    current_season_id = games['SEASON_ID'].iloc[0]

    current_season_games = games[games.SEASON_ID == current_season_id]
    current_season_games = current_season_games.loc[current_season_games['GAME_DATE'] > '2022-10-01']

    current_season_game_ids = list(current_season_games['GAME_ID'])

    return current_season_game_ids


def get_box_score(game_id, int_timeout):
    try:
        game_sets = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id, timeout=int_timeout)
    except Timeout:
        print('Timeout caught, retrying...')
        game_sets = get_box_score(game_id, int_timeout)

    print('Box score pulled!')

    return game_sets


def get_game_df(clips_id, game_id):
    game_sets = get_box_score(game_id, 10)
    game_df = game_sets.get_data_frames()[0]
    game_df = game_df.loc[game_df['TEAM_ID'] == clips_id]
    game_df = game_df.drop(['TEAM_ABBREVIATION', 'TEAM_CITY', 'NICKNAME', 'COMMENT'], axis=1)

    return game_df


def get_season_to_date_box_scores(clips_id, current_season_game_ids):
    game_df = pd.DataFrame()
    tracker = 0
    for game_id in current_season_game_ids:
        try:
            game_df = pd.concat([game_df, get_game_df(clips_id, game_id)], axis=0)
        except Timeout:
            print('Timeout caught')

        tracker += 1
        print(tracker)

    return game_df


# TRANSFORM
def convert_minutes(minutes):
    if minutes is None:
        return '0:00'

    list_mm_ss = str(minutes).split(':')
    int_minutes = int(float(list_mm_ss[0]))
    int_seconds = int(list_mm_ss[1])

    if int_seconds >= 60:
        int_minutes += 1
        int_seconds -= 60

    str_minutes = str(int_minutes)
    str_seconds = str(int_seconds).zfill(2)
    
    return str_minutes + ':' + str_seconds


def format_float(pct):
    # FG_PCT, FG3_PCT, FT_PCT
    pct = round(100 * pct, 1)
    str_pct = str(pct) + '%'

    return str_pct


def format_floats_to_ints(flt):
    return int(flt)


def format_for_db(season_to_date_team_df):
    season_to_date_team_df['MIN'] = season_to_date_team_df['MIN'].apply(convert_minutes)

    floats_columns = ['FG_PCT', 'FG3_PCT', 'FT_PCT']
    season_to_date_team_df[str(floats_columns)] = season_to_date_team_df[str(floats_columns)].apply(format_float)

    f_to_i_columns = ['FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO',
    'PF', 'PTS', 'PLUS_MINUS']
    season_to_date_team_df[str(f_to_i_columns)] = season_to_date_team_df[str(f_to_i_columns)].apply(format_floats_to_ints)

    return season_to_date_team_df


def main():
    clips_id = get_clips_id()
    current_season_game_ids = get_current_season_game_ids(clips_id)

    season_to_date_team_df = get_season_to_date_box_scores(clips_id, current_season_game_ids)
    
    season_to_date_team_df = format_for_db(season_to_date_team_df)

    return


if __name__ == '__main__':
    main()
