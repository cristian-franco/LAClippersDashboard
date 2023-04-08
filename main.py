from nba_api.stats.endpoints import commonteamroster, leaguegamefinder
from nba_api.stats.static import teams
import pandas as pd
import time
from dash import dash_table
from requests.exceptions import Timeout
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import config
import pymysql

pymysql.install_as_MySQLdb()


def get_clips_id():
    clips = teams.find_team_by_abbreviation('lac')
    clips_id = clips["id"]

    return clips_id


def get_clips_players(clips_id):
    clips_info = commonteamroster.CommonTeamRoster(clips_id, timeout=600)
    clips_players_info = clips_info.common_team_roster.get_data_frame()

    clips_players_df = clips_players_info[["PLAYER", "NUM", "POSITION", "HEIGHT", "WEIGHT", "AGE" , "PLAYER_ID"]]

    return clips_players_df


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


def pull_last_game_db(db_choice, api_latest_game_id):
    if not compare_to_db_latest_game_id(db_choice, api_latest_game_id):
        print('Latest game data not in database')
        return

    if db_choice == 'local':
        engine = create_engine(f"mysql://{config.mysql_local['user']}:%s@{config.mysql_local['host']}/"
                               f"{config.mysql_local['db']}" % quote_plus(config.mysql_local['passwd']))
        con = engine.connect()

        name = 'test_daily_boxscore'

        sql = text('SELECT DISTINCT * FROM ' + name + ' WHERE GameID = ' + str(api_latest_game_id))

        sql_result_df = pd.read_sql(sql, con)

    else:
        print('Error')
        return

    con.close()
    engine.dispose()

    return sql_result_df


def compare_to_db_latest_game_id(db_choice, api_latest_game_id):
    if db_choice == 'local':
        engine = create_engine(f"mysql://{config.mysql_local['user']}:%s@{config.mysql_local['host']}/"
                               f"{config.mysql_local['db']}" % quote_plus(config.mysql_local['passwd']))
        con = engine.connect()

        name = 'test_daily_boxscore'

        sql = text('SELECT DISTINCT GameID FROM ' + name + ' WHERE GameID = ' + str(api_latest_game_id))

        sql_result_df = pd.read_sql(sql, con)

        if sql_result_df.empty:
            return False

        db_latest_game_id = sql_result_df['GameID'].iloc[0]
    else:
        print('Error')
        return

    con.close()
    engine.dispose()

    return int(api_latest_game_id) == db_latest_game_id


def main():
    # local db
    db_choice = 'local'
    # AWS RDS db
    # db_choice = 'AWS'

    clips_id = get_clips_id()
    latest_game_id = get_latest_game_id(clips_id)
    # pull data from db, only what you need (?)
    last_game_df = pull_last_game_db(db_choice, latest_game_id)

    print(last_game_df)

    return last_game_df
    # filter if needed

    # clips_players_df = get_clips_players(clips_id)

    # Last Game Player Boxscores

    # Last Game Team Boxscores

    # Last 3 Games Player Boxscore Averages

    # Last 3 Games Team Boxscore Averages

    # Season To Date Player Boxscore Averages

    # Season To Date Team Boxscores Averages


if __name__ == "__main__":
    main()

