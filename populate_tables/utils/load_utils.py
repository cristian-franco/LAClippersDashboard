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


def upload_to_db(
    db_choice,
    df,
    table_name
):
    if db_choice == 'local':
        engine_str = (
            f"mysql://{config.mysql_local['user']}:%s@{config.mysql_local['host']}/{config.mysql_local['db']}"
            % quote_plus(config.mysql_local['passwd'])
        )
        engine = create_engine(engine_str)
        con = engine.connect()

        df.to_sql(
            con=engine,
            name=table_name,
            if_exists='append',
            index=False
        )

        print('UPLOADED')

        con.close()
        engine.dispose()
    else:
        print('Error')

    return
