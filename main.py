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
    game_sets = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id, timeout=600)
    game_df = game_sets.get_data_frames()[0]
    game_df = game_df.loc[game_df['TEAM_ID'] == clips_id]
    game_df = game_df.drop(['TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_CITY', 'NICKNAME', 'COMMENT'], axis=1)

    return game_df


def season_to_date_overview(clips_players_df):
    # accidentally pulled career stats instead of season to date stats, FIX
    team_season_to_date_df = pd.DataFrame()
    for player in range(len(clips_players_df.index)):
        player_stats = playercareerstats.PlayerCareerStats(per_mode36="Totals", player_id=clips_players_df["PLAYER_ID"]
        [player], timeout=600)
        player_stats_df = player_stats.get_data_frames()[0]
        if not player_stats_df.empty:
            player_stats_df = player_stats_df.iloc[[-1]]
            team_season_to_date_df = pd.concat([team_season_to_date_df, player_stats_df], ignore_index=True)

    team_season_to_date_df = pd.merge(clips_players_df, team_season_to_date_df, how='right', on='PLAYER_ID')
    team_season_to_date_df = team_season_to_date_df.drop(['LEAGUE_ID', 'TEAM_ID', 'WEIGHT', 'AGE', 'PLAYER_ID',
                                                          'SEASON_ID', 'TEAM_ABBREVIATION', 'PLAYER_AGE', 'HEIGHT',
                                                          'NUM'], axis=1)

    return team_season_to_date_df


def season_to_date_team(clips_id, games):
    all_games_ids = list(games['GAME_ID'])

    # get the dataframes of all the games then combine them into one
    game_df = pd.DataFrame()
    for game_id in all_games_ids:
        game_df = pd.concat([game_df, get_game_player_stats(clips_id, game_id)], axis=0)

    # game_df = game_df.drop(['PLAYER_ID'])
    season_to_date_team_df = game_df.groupby(['PLAYER_NAME']).mean()

    return season_to_date_team_df


def last_games(clips_id):
    gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=clips_id, timeout=600)
    games = gamefinder.get_data_frames()[0]

    last_games_list = [last_three_games_team(clips_id, games), last_three_games_individual(clips_id, games),
                       last_game_team(clips_id, games), last_game_individual(clips_id, games)]

    return last_games_list


def last_three_games_team(clips_id, games):
    # get single game id's for the clippers for the season?
    # filter down to the most recent three games
    last_three_games_team_df = games.head(3)

    last_three_games_team_df = last_three_games_team_df.drop(['SEASON_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME',
                                                              'GAME_ID'], axis=1)

    return last_three_games_team_df


def last_three_games_individual(clips_id, games):
    last_three_games_team_df = games.head(3)
    last_three_game_ids = list(last_three_games_team_df['GAME_ID'])

    # get the 3 dataframes of the last three games then combine them into one
    game_df = pd.DataFrame()
    for game_id in last_three_game_ids:
        game_df = pd.concat([game_df, get_game_player_stats(clips_id, game_id)], axis=0)

    # game_df = game_df.drop(['PLAYER_ID'])
    game_df = game_df.groupby(['PLAYER_NAME']).mean()

    return game_df


def last_game_team(clips_id, games):
    last_game_team_df = games.head(1)

    last_game_team_df = last_game_team_df.drop(['SEASON_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID'],
                                               axis=1)

    return last_game_team_df


def last_game_individual(clips_id, games):
    last_game_team_df = games.head(1)

    last_game_id = last_game_team_df['GAME_ID'].iloc[0]
    last_game_sets = boxscoretraditionalv2.BoxScoreTraditionalV2(last_game_id, timeout=600)
    last_game_df = last_game_sets.get_data_frames()[0]
    last_game_individual_df = last_game_df.loc[last_game_df['TEAM_ID'] == clips_id]
    last_game_individual_df = last_game_individual_df.drop(['TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_CITY', 'GAME_ID',
                                                            'PLAYER_ID', 'NICKNAME', 'COMMENT'],
                                                           axis=1)

    return last_game_individual_df


def refresh_data(clips_id, clips_players_df):
    dataframe_list = []

    last_games_list = last_games(clips_id)
    last_game_team_df = last_games_list[0]
    last_game_individual_df = last_games_list[1]
    last_three_games_team_df = last_games_list[2]
    last_three_games_individual_df = last_games_list[3]

    dataframe_list.append(season_to_date_overview(clips_players_df))
    dataframe_list.append(last_game_team_df)
    dataframe_list.append(last_game_individual_df)
    dataframe_list.append(last_three_games_team_df)
    dataframe_list.append(last_three_games_individual_df)
    dataframe_list.append(season_to_date_team(clips_players_df))

    return dataframe_list


def main():
    clips_id = get_clips_id()
    clips_players_df = get_clips_players(clips_id)

    # get all data
    dataframe_list = refresh_data(clips_id, clips_players_df)
    season_to_date_overview_df = dataframe_list[0]
    season_to_date_team_df = dataframe_list[5]
    last_game_team_df = dataframe_list[1]
    last_game_individual_df = dataframe_list[2]
    last_three_games_overview_df = dataframe_list[3]
    last_three_games_individual_df = dataframe_list[4]

    # Last Game Overview - DONE
    st.write('### Last Game - Overview', last_game_team_df)
    # # Last Game Team Stats - DONE
    st.write('### Last Game - Team', last_game_individual_df)


    # Last 3 Games Overview - DONE
    st.write("### Last 3 Games", last_three_games_overview_df)
    # can add a row / section for averages across the three games
    # Last 3 Games Team Stats - DONE
    st.write("### Last 3 Games - Team", last_three_games_individual_df)


    # Season To Date Overview
    # st.write("### Season To Date - Overview", season_to_date_overview_df)
    # Season To Date Team Stats - DONE
    # st.write("### Season To Date - Team", season_to_date_team_df)


if __name__ == "__main__":
    main()

