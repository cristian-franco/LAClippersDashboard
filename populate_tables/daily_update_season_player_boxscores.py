from nba_api.stats.endpoints import boxscoretraditionalv2
import pandas as pd
from populate_tables.utils import extract_utils as eu, load_utils as lu, transform_utils as tu
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from requests.exceptions import Timeout
import config
import pymysql

pymysql.install_as_MySQLdb()


def get_latest_game_id(clips_id):
    # contains summer league and preseason games
    games = eu.game_finder(clips_id, 10).get_data_frames()[0]

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


# CHECK
def compare_to_db_latest_game_id(db_choice, api_latest_game_id):
    if db_choice == 'local':
        engine = create_engine(f"mysql://{config.mysql_local['user']}:%s@{config.mysql_local['host']}/"
                               f"{config.mysql_local['db']}" % quote_plus(config.mysql_local['passwd']))
        con = engine.connect()
        
        name = 'test_season_player_boxscores'

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

    clips_id = eu.get_clips_id()

    # get latest game id
    latest_game_id = get_latest_game_id(clips_id)

    if compare_to_db_latest_game_id(db_choice, latest_game_id):
        print('Latest boxscore data already in Database')
        return

    # get latest boxscore
    boxscore_df = get_game_df(clips_id, latest_game_id)

    # format
    boxscore_df = tu.format_for_db(boxscore_df)

    # upload
    lu.upload_to_db(db_choice, boxscore_df, 'test_season_player_boxscores')

    return


if __name__ == '__main__':
    main()
