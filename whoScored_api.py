import soccerdata as sd
from pathlib import Path
import pandas as pd
import json

from selenium.webdriver.common.devtools.v115.page import print_to_pdf

if __name__ == "__main__":
    # Path to Chrome executable
    # chrome_path = Path("C:/Program Files/Google/Chrome/Application/chrome.exe")

    # # Initialize WhoScored with path to Chrome browser
    ws = sd.WhoScored(leagues="ENG-Premier League", seasons=['2020-2021'], headless=False)
    print(ws._doc_)
    epl_schedule = ws.read_schedule()
    print(epl_schedule.head().game_id)
    print("")
    events_df = ws.read_events(match_id=1485186)
    print("")
    formation_set_df = events_df[events_df['type'] == 'FormationSet']
    print(formation_set_df)
    print("")
    # Select only the columns 'game_id', 'period', 'minute', 'second', and 'qualifiers'
    filtered_df_formation_set = formation_set_df[['game_id', 'period', 'minute', 'second', 'qualifiers']]
    # Display the filtered DataFrame
    print(filtered_df_formation_set)
    print("")
    formation_change_df = events_df[events_df['type'] == 'FormationChange']
    print(formation_change_df)
    print("")
    # Select only the columns 'game_id', 'period', 'minute', 'second', and 'qualifiers'
    filtered_df_formation_change = formation_change_df[['game_id', 'period', 'minute', 'second', 'qualifiers']]
    # Display the filtered DataFrame
    print(filtered_df_formation_change)
    # Extract the qualifiers column as a list of values and print it
    qualifiers_list = filtered_df_formation_change["qualifiers"].tolist()
    # Print the list of qualifiers
    print("This is qualifiers:\n", qualifiers_list)
    # Extracting the dictionary with 'displayName' == 'TeamFormation'
    team_formation_value = None
    for qualifier in qualifiers_list[0]:
        if qualifier['type']['displayName'] == 'TeamFormation':
            team_formation_value = qualifier['type']['value']
            break

    # Output the result
    print("this is the formation value: ",team_formation_value)

# Get the list of all available leagues from WhoScored
leagues = sd.WhoScored.available_leagues()
# Print the list of leagues
#print(leagues)