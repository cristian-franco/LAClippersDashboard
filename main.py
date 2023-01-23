from nba_api.stats.endpoints import commonteamroster, playercareerstats, leaguegamefinder, teamdashboardbylastngames, \
    boxscoretraditionalv2
from nba_api.stats.static import teams
import streamlit as st
import pandas as pd


def get_clips_id():
    clips = teams.find_team_by_abbreviation('lac')
    clips_id = clips["id"]

    return clips_id


def get_clips_players(clips_id):
    clips_info = commonteamroster.CommonTeamRoster(clips_id, timeout=600)
    clips_players_info = clips_info.common_team_roster.get_data_frame()

    clips_players_df = clips_players_info[["PLAYER", "NUM", "POSITION", "HEIGHT", "WEIGHT", "AGE" , "PLAYER_ID"]]

    return clips_players_df


def get_game_player_stats(clips_id, game_id):
    game_sets = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id, timeout=500)
    game_df = game_sets.get_data_frames()[0]
    game_df = game_df.loc[game_df['TEAM_ID'] == clips_id]
    game_df = game_df.drop(['TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_CITY', 'NICKNAME', 'COMMENT'], axis=1)

    return game_df


def season_to_date(clips_players_df):
    team_season_to_date_df = pd.DataFrame()
    for player in range(len(clips_players_df.index)):
        player_stats = playercareerstats.PlayerCareerStats(per_mode36="Totals", player_id=clips_players_df["PLAYER_ID"]
        [player], timeout=500)
        player_stats_df = player_stats.get_data_frames()[0]
        player_stats_df = player_stats_df.iloc[[-1]]
        team_season_to_date_df = pd.concat([team_season_to_date_df, player_stats_df], ignore_index=True)

    team_season_to_date_df = pd.merge(clips_players_df, team_season_to_date_df, how='right', on='PLAYER_ID')
    team_season_to_date_df = team_season_to_date_df.drop(['LEAGUE_ID', 'TEAM_ID', 'WEIGHT', 'AGE', 'PLAYER_ID',
                                                          'SEASON_ID', 'TEAM_ABBREVIATION', 'PLAYER_AGE', 'HEIGHT',
                                                          'NUM'], axis=1)

    return team_season_to_date_df


def last_three_games_team(clips_id):
    # get single game id's for the clippers for the season?
    # filter down to the most recent three games
    gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=clips_id, timeout=500)
    games = gamefinder.get_data_frames()[0]
    last_three_games_team_df = games.head(3)

    last_three_games_team_df = last_three_games_team_df.drop(['SEASON_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME',
                                                              'GAME_ID'], axis=1)

    return last_three_games_team_df


def last_three_games_individual(clips_id):
    # 2 choices: 1, return stats on last three games (doesn't really make sense since we have the last game anyways
    # 2, return averages over the last three games for the players
    gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=clips_id, timeout=500)
    games = gamefinder.get_data_frames()[0]
    last_three_games_team_df = games.head(3)
    last_three_game_ids = list(last_three_games_team_df['GAME_ID'])

    # get the 3 dataframes of the last three games then combine them into one
    game_df = pd.DataFrame()
    for game_id in last_three_game_ids:
        game_df = pd.concat([game_df, get_game_player_stats(clips_id, game_id)], axis=0)

    # game_df = game_df.drop(['PLAYER_ID'])
    game_df = game_df.groupby(['PLAYER_NAME']).mean()

    return game_df


def last_game_team(clips_id):
    gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=clips_id)
    games = gamefinder.get_data_frames()[0]
    last_game_team_df = games.head(1)

    last_game_team_df = last_game_team_df.drop(['SEASON_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID'],
                                               axis=1)

    return last_game_team_df


def last_game_individual(clips_id):
    gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=clips_id)
    games = gamefinder.get_data_frames()[0]
    last_game_team_df = games.head(1)

    last_game_id = last_game_team_df['GAME_ID'].iloc[0]
    last_game_sets = boxscoretraditionalv2.BoxScoreTraditionalV2(last_game_id) # need to get game_id of last played game
    last_game_df = last_game_sets.get_data_frames()[0]
    last_game_individual_df = last_game_df.loc[last_game_df['TEAM_ID'] == clips_id]
    last_game_individual_df = last_game_individual_df.drop(['TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_CITY', 'GAME_ID',
                                                            'PLAYER_ID', 'NICKNAME', 'COMMENT'],
                                                           axis=1)

    return last_game_individual_df


def refresh_data(clips_id, clips_players_df):
    dataframe_list = []

    season_to_date_df = season_to_date(clips_players_df)
    last_game_team_df = last_game_team(clips_id)
    last_game_individual_df = last_game_individual(clips_id)
    last_three_games_team_df = last_three_games_team(clips_id)
    last_three_games_individual_df = last_three_games_individual(clips_id)

    dataframe_list.append(season_to_date_df)
    dataframe_list.append(last_game_team_df)
    dataframe_list.append(last_game_individual_df)
    dataframe_list.append(last_three_games_team_df)
    dataframe_list.append(last_three_games_individual_df)

    return dataframe_list


def main():
    clips_id = get_clips_id()
    clips_players_df = get_clips_players(clips_id)

    # get all data
    dataframe_list = refresh_data(clips_id, clips_players_df)
    season_to_date_df = dataframe_list[0]
    last_game_team_df = dataframe_list[1]
    last_game_individual_df = dataframe_list[2]
    last_three_games_overview_df = dataframe_list[3]
    last_three_games_individual_df = dataframe_list[4]

    # Last Game Overview - DONE
    st.write('### Last Game - Overview', last_game_team_df)
    # # Last Game Individual Stats - DONE
    st.write('### Last Game - Team', last_game_individual_df)


    # Last 3 Games Overview - DONE
    st.write("### Last 3 Games", last_three_games_overview_df)
    # can add a row / section for averages across the three games
    # Last 3 Games Individual Stats - IN PROGRESS
    st.write("### Last 3 Games - Team", last_three_games_individual_df)


    # # Season To Date Overview
    #
    # Season To Date Individual Stats - DONE
    st.write("### Season To Date - Team", season_to_date_df)


if __name__ == "__main__":
    main()

