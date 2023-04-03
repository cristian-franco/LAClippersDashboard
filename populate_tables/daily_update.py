from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
from nba_api.stats.static import teams
import pandas as pd
import time
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from requests.exceptions import Timeout
import config
import pymysql

pymysql.install_as_MySQLdb()


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
        time.sleep(5)
        games_found = game_finder(team_id, int_timeout)

    return games_found


def get_latest_game_id(clips_id):
    # contains summer league and preseason games
    games = game_finder(clips_id, 10).get_data_frames()[0]

    latest_game_id = games['GAME_ID'].iloc[0]

    return latest_game_id


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
    if pd.isna(pct):
        return pct

    pct = round(100 * pct, 1)
    str_pct = str(pct) + '%'

    return str_pct


def transform_pcts(season_to_date_team_df):
    season_to_date_team_df['FG_PCT'] = season_to_date_team_df['FG_PCT'].apply(format_float)
    season_to_date_team_df['FG3_PCT'] = season_to_date_team_df['FG3_PCT'].apply(format_float)
    season_to_date_team_df['FT_PCT'] = season_to_date_team_df['FT_PCT'].apply(format_float)

    return season_to_date_team_df


def format_columns(season_to_date_team_df):
    season_to_date_team_df = season_to_date_team_df.drop('PLAYER_NAME', axis=1)
    ptsCol = season_to_date_team_df.pop('PTS')
    season_to_date_team_df.insert(5, 'Pts', ptsCol)
    season_to_date_team_df.rename(columns={'GAME_ID': 'GameID', 'TEAM_ID': 'TeamID', 'PLAYER_ID': 'PlayerID',
                                           'START_POSITION': 'StartPosition', 'MIN': 'Min', 'FG_PCT': 'FGPct',
                                           'FG3_PCT': 'FG3Pct', 'FT_PCT': 'FTPct', 'OREB': 'OReb', 'DREB': 'DReb',
                                           'REB': 'Reb', 'AST': 'Ast', 'STL': 'Stl', 'BLK': 'Blk', 'TO': 'TOV',
                                           'PTS': 'Pts', 'PLUS_MINUS': 'PlusMinus'}, inplace=True)

    return season_to_date_team_df


def format_for_db(season_to_date_team_df):
    season_to_date_team_df['MIN'] = season_to_date_team_df['MIN'].apply(convert_minutes)

    season_to_date_team_df = transform_pcts(season_to_date_team_df)
    season_to_date_team_df = season_to_date_team_df.reset_index(drop=True)
    season_to_date_team_df = format_columns(season_to_date_team_df)

    return season_to_date_team_df


def upload_to_db(db_choice, df):
    if db_choice == 'local':
        engine = create_engine(f"mysql://{config.mysql_local['user']}:%s@{config.mysql_local['host']}/"
                               f"{config.mysql_local['db']}" % quote_plus(config.mysql_local['passwd']))
        con = engine.connect()

        df.to_sql(con=engine, name='test_daily_boxscore', if_exists='append', index=False)

        print('UPLOADED')
    else:
        print('Error')
        return

    con.close()
    engine.dispose()

    return


def main():
    # local db
    db_choice = 'local'
    # AWS RDS db
    # db_choice = 'AWS'

    clips_id = get_clips_id()

    # get today's game ids
    latest_game_id = get_latest_game_id(clips_id)

    # get today's boxscore
    boxscore_df = get_game_df(clips_id, latest_game_id)

    # format
    boxscore_df = format_for_db(boxscore_df)

    # upload
    upload_to_db(db_choice, boxscore_df)

    return


if __name__ == '__main__':
    main()
