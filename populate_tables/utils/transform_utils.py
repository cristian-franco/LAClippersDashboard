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


def format_minutes(mins):
    if pd.isna(mins):
        return mins

    mins, secs = mins.split(':', 1)

    secs = float(secs) / 60

    mins = float(mins) + secs

    return mins


def transform_minutes(df):
    df['MIN'] = df['MIN'].apply(format_minutes)

    return df


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
    # season_to_date_team_df['MIN'] = season_to_date_team_df['MIN'].apply(convert_minutes)

    # season_to_date_team_df = transform_pcts(season_to_date_team_df)
    season_to_date_team_df = transform_minutes(season_to_date_team_df)
    season_to_date_team_df = season_to_date_team_df.reset_index(drop=True)
    season_to_date_team_df = format_columns(season_to_date_team_df)

    return season_to_date_team_df
# everything above is for season_player_boxscores


# below is for season_team_boxscores
def format_team_boxscores(df):
    df = df.drop(['TEAM_ID', 'SEASON_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME'], axis=1)
    df.rename(columns={'GAME_ID': 'GameID', 'GAME_DATE': 'GameDate', 'MATCHUP': 'Matchup', 'WL': 'WinLoss',
                       'MIN': 'Minutes', 'PTS': 'Pts', 'FG_PCT': 'FGPct', 'FG3_PCT': 'FG3Pct', 'FT_PCT': 'FTPct',
                       'OREB': 'OReb', 'DREB': 'DReb', 'REB': 'Reb', 'AST': 'Ast', 'STL': 'Stl', 'BLK': 'Blk',
                       'PLUS_MINUS': 'PlusMinus'}, inplace=True)

    return df


def format_player_totals(df):
    df.drop(['GROUP_SET', 'NBA_FANTASY_PTS', 'DD2', 'TD3', 'WNBA_FANTASY_PTS', 'NBA_FANTASY_PTS_RANK', 'DD2_RANK',
            'TD3_RANK', 'WNBA_FANTASY_PTS_RANK'], axis=1, inplace=True)

    df.rename(columns={'PLAYER_ID': 'PlayerID', 'PLAYER_NAME': 'PlayerName', 'NICKNAME': 'Nickname', 'GP': 'GamesPlayed',
                       'W': 'Wins', 'L': 'Losses', 'W_PCT': 'WinPct', 'MIN': 'Min', 'FG_PCT': 'FGPct',
                       'FG3_PCT': 'FG3Pct', 'FT_PCT': 'FTPct', 'OREB': 'OReb', 'DREB': 'DReb',
                       'REB': 'Reb', 'AST': 'Ast', 'STL': 'Stl', 'BLK': 'Blk', 'BLKA': 'BlkA', 'PTS': 'Pts',
                       'PLUS_MINUS': 'PlusMinus', 'GP_RANK': 'GamesPlayedRank', 'W_RANK': 'WinsRank',
                       'L_RANK': 'LossesRank', 'W_PCT_RANK': 'WinPctRank', 'MIN_RANK': 'MinRank', 'FGM_RANK': 'FGMRank',
                       'FGA_RANK': 'FGARank', 'FG_PCT_RANK': 'FGPctRank', 'FG3M_RANK': 'FG3MRank',
                       'FG3A_RANK': 'FG3ARank', 'FG3_PCT_RANK': 'FG3PctRank', 'FTM_RANK': 'FTMRank',
                       'FTA_RANK': 'FTARank', 'FT_PCT_RANK': 'FTPctRank', 'OREB_RANK': 'ORebRank',
                       'DREB_RANK': 'DRebRank', 'REB_RANK': 'RebRank', 'AST_RANK': 'AstRank',
                       'TOV_RANK': 'TOVRank', 'STL_RANK': 'StlRank', 'BLK_RANK': 'BlkRank', 'BLKA_RANK': 'BlkaRank',
                       'PF_RANK': 'PFRank', 'PFD_RANK': 'PFDRank', 'PTS_RANK': 'PtsRank',
                       'PLUS_MINUS_RANK': 'PlusMinusRank'}, inplace=True)
    return df
