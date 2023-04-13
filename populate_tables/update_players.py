from nba_api.stats.endpoints import teamplayerdashboard
import pandas as pd
from populate_tables.utils import extract_utils as eu, load_utils as lu, transform_utils as tu
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from requests.exceptions import Timeout
import config
import pymysql

pymysql.install_as_MySQLdb()


def get_players(clips_id):
    tpd = teamplayerdashboard.TeamPlayerDashboard(team_id=clips_id)
    df = tpd.get_data_frames()[1]

    return df


def get_latest_game_id(clips_id):
    # contains summer league and preseason games
    games = eu.game_finder(clips_id, 10).get_data_frames()[0]

    latest_game_id = games['GAME_ID'].iloc[0]

    return latest_game_id


# CHECK
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

    # extract
    clips_id = eu.get_clips_id()
    df = get_players(clips_id)

    # transform
    df = tu.format_player_totals(df)

    # upload
    lu.upload_to_db(db_choice, df, 'season_players')

    return


if __name__ == '__main__':
    main()
