from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.static import teams

import pandas as pd

clips = teams.find_team_by_abbreviation('lac')

clips_info = commonteamroster.CommonTeamRoster(clips["id"])
clips_players_info = clips_info.common_team_roster.get_data_frame()
pd.set_option("display.max_rows", None, "display.max_columns", None)
# print(clips_players_info)


clips_slim_df = clips_players_info[["PLAYER", "NUM", "POSITION", "HEIGHT", "WEIGHT", "AGE", "PLAYER_ID"]]
print(clips_slim_df)


player_stats = playercareerstats.PlayerCareerStats(per_mode36="Totals", player_id=clips_slim_df["PLAYER_ID"][2])
player_stats_df = player_stats.get_data_frames()
print(player_stats_df)
# final_df =
# print(final_df)

