from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.static import teams
import streamlit as st
import pandas as pd


def season_to_date():
    # stats to return
    #
    clips = teams.find_team_by_abbreviation('lac')

    clips_info = commonteamroster.CommonTeamRoster(clips["id"])
    clips_players_info = clips_info.common_team_roster.get_data_frame()
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    # print(clips_players_info)

    clips_slim_df = clips_players_info[["PLAYER", "NUM", "POSITION", "HEIGHT", "WEIGHT", "AGE" , "PLAYER_ID"]]
    #print(clips_slim_df)

    team_season_to_date_df = pd.DataFrame()
    for player in range(len(clips_slim_df.index)):
        player_stats = playercareerstats.PlayerCareerStats(per_mode36="Totals", player_id=clips_slim_df["PLAYER_ID"][player])
        player_stats_df = player_stats.get_data_frames()[0]
        player_stats_df = player_stats_df.iloc[[-1]]
        team_season_to_date_df = pd.concat([team_season_to_date_df, player_stats_df], ignore_index=True)

    team_season_to_date_df = pd.merge(clips_slim_df, team_season_to_date_df, how='right', on='PLAYER_ID')
    team_season_to_date_df = team_season_to_date_df.drop(['LEAGUE_ID', 'TEAM_ID'], axis=1)

    return team_season_to_date_df
    # Brandon Boston Jr, rookie last year
    # player_stats = playercareerstats.PlayerCareerStats(per_mode36="Totals", player_id=clips_slim_df["PLAYER_ID"][3])
    # player_stats_df = player_stats.get_data_frames()[0]
    # print(player_stats_df)
    # print("Season To Date")


def main():
    df = season_to_date()
    st.write("### Summary Stats", df)


if __name__ == "__main__":
    main()

