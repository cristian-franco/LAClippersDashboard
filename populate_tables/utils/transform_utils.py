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
