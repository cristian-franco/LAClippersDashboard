from nba_api.stats.endpoints import boxscoretraditionalv2
import pandas as pd
from populate_tables.utils import extract_utils as eu, load_utils as lu, transform_utils as tu
from requests.exceptions import Timeout
import pymysql
pymysql.install_as_MySQLdb()


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


def get_season_to_date_box_scores(clips_id, current_season_game_ids):
    game_df = pd.DataFrame()
    tracker = 0
    for game_id in current_season_game_ids:
        try:
            game_df = pd.concat([game_df, get_game_df(clips_id, game_id)], axis=0)
        except Timeout:
            print('Timeout caught')

        tracker += 1
        print(tracker)

    return game_df


def main():
    # local db
    db_choice = 'local'
    # AWS RDS db
    # db_choice = 'AWS'

    clips_id = eu.get_clips_id()
    current_season_game_ids = eu.get_current_season_game_ids(clips_id)

    season_to_date_team_df = get_season_to_date_box_scores(clips_id, current_season_game_ids)

    season_to_date_team_df = tu.format_for_db(season_to_date_team_df)

    lu.upload_to_db(db_choice, season_to_date_team_df, 'season_player_boxscores')

    return


if __name__ == '__main__':
    main()
