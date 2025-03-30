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


def get_clips_id():
    clips_info = teams.find_team_by_abbreviation('lac')
    clips_id = clips_info["id"]

    return clips_id


def game_finder(
    team_id,
    int_timeout,
):
    # default param for LeagueGameFinder is current season
    try:
        games_found = leaguegamefinder.LeagueGameFinder(
            team_id_nullable=team_id,
            timeout=int_timeout
        )
    except Timeout:
        print('Timeout caught, retying...')
        time.sleep(5)
        games_found = game_finder(
            team_id,
            int_timeout
        )

    return games_found


def get_current_season_game_ids(
    clips_id
):
    # contains summer league and preseason games
    games = game_finder(
        clips_id,
        10
    ).get_data_frames()[0]
    # preseason is starts with 1,
    # regular season starts with 2,
    # postseason is either 4 or 5, then 4 digit year
    current_season_id = str(games['SEASON_ID'].iloc[0])[1:]
    current_season_id = '2' + current_season_id

    current_season_games = games[
        games.SEASON_ID == current_season_id
    ]
    current_season_games = current_season_games.loc[
        current_season_games['GAME_DATE'] > '2022-10-01'
    ]

    current_season_game_ids = list(current_season_games['GAME_ID'])

    return current_season_game_ids
