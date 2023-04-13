from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.static import teams
from populate_tables.utils import extract_utils as eu, load_utils as lu, transform_utils as tu
import pandas as pd
import time
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


def get_team_boxscores(team_id, latest_game_id):
    df = eu.game_finder(team_id, 10).get_data_frames()[0]
    df = df.loc[df['GAME_ID'] == latest_game_id]

    return df


def upload_to_db(db_choice, df):
    if db_choice == 'local':
        engine = create_engine(f"mysql://{config.mysql_local.user}:%s@{config.mysql_local.host}/{config.mysql_local.db}"
                               % quote_plus(config.mysql_local.passwd))
        con = engine.connect()

        df.to_sql(con=engine, name='season_player_stats', if_exists='append', index=False)

        print('UPLOADED')
    else:
        print('Error')
        return

    con.close()
    engine.dispose()

    return


# CHECK
def compare_to_db_latest_game_id(db_choice, api_latest_game_id):
    if db_choice == 'local':
        engine = create_engine(f"mysql://{config.mysql_local['user']}:%s@{config.mysql_local['host']}/"
                               f"{config.mysql_local['db']}" % quote_plus(config.mysql_local['passwd']))
        con = engine.connect()

        name = 'test_season_team_boxscores'

        sql = text('SELECT DISTINCT GameID FROM ' + name + ' WHERE GameID = ' + api_latest_game_id)

        sql_result_df = pd.read_sql(sql, con)

        if sql_result_df.empty:
            return False

        db_latest_game_id = sql_result_df['GameID'].iloc[0]
    else:
        print('Error')
        return

    con.close()
    engine.dispose()

    return api_latest_game_id == db_latest_game_id


def main():
    # local db
    db_choice = 'local'
    # AWS RDS db
    # db_choice = 'AWS'
    table_name = 'test_season_team_boxscores'

    clips_id = eu.get_clips_id()

    # get latest game id
    latest_game_id = get_latest_game_id(clips_id)

    answer = compare_to_db_latest_game_id(db_choice, latest_game_id)

    if compare_to_db_latest_game_id(db_choice, latest_game_id):
        print('Latest boxscore data already in Database')
        return

    team_boxscores_df = get_team_boxscores(clips_id, latest_game_id)

    team_boxscores_df = tu.format_team_boxscores(team_boxscores_df)

    lu.upload_to_db(db_choice, team_boxscores_df, table_name)

    return


if __name__ == '__main__':
    main()
