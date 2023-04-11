from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.static import teams
from populate_tables.utils import extract_utils as eu, load_utils as lu, transform_utils as tu
import pandas as pd
import time
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from requests.exceptions import Timeout
import config
import pymysql

pymysql.install_as_MySQLdb()


def get_team_boxscores(team_id):
    df = eu.game_finder(team_id, 10).get_data_frames()[0]

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


def main():
    # local db
    db_choice = 'local'
    # AWS RDS db
    # db_choice = 'AWS'

    table_name = 'season_team_boxscores'

    clips_id = eu.get_clips_id()
    team_boxscores_df = get_team_boxscores(clips_id)

    team_boxscores_df = tu.format_team_boxscores(team_boxscores_df)

    lu.upload_to_db(db_choice, team_boxscores_df, table_name)

    return


if __name__ == '__main__':
    main()
