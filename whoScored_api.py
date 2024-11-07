import json
import os
import sys

import soccerdata as sd
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import sys
import MatchToGameIDs

# Mapping of formation codes to formation names
formations_dict = {
    8: "4-2-3-1",
    23: "4-3-1-2",
    4: "4-3-3",
    15: "4-2-2-2",
    11: "5-4-1",
    10: "5-3-2",
    7: "4-1-4-1",
    13: "3-4-3",
    16: "3-5-1-1",
    17: "3-4-2-1",
    2: "4-4-2",
    6: "4-4-1-1",
    12: "3-5-2"
}

# Initialize global variable for connection
conn = None

def whoscored_call_api_schedule(league_name,season):
    chrome_path = Path("C:/Program Files/Google/Chrome/Application/chrome.exe")
    # for now we will pull only ENG-Premier League - otherwise, change it
    # season format for exm- '2020-2021'
    ws = sd.WhoScored(leagues="ENG-Premier League", seasons=[season], path_to_browser = chrome_path, headless=False)
    schedule = ws.read_schedule()
    return schedule

# Connect to PostgreSQL using environment variables
def connect_to_db():
    global conn
    # Ensure stdout is UTF-8 encoded
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)

    # Load environment variables
    load_dotenv()

    # Retrieve database connection details from environment variables
    dbname = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')

    # Establish a connection to the PostgreSQL database
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print("Database connection successful")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        conn = None


# Function to insert data into the whoScored_events table
def insert_game_events(data):
    global conn
    if conn is None:
        connect_to_db()
    if conn is None:
        print("Connection to database failed")
        return

    with conn.cursor() as cursor:
        insert_query = '''
            INSERT INTO whoScored_events (game_id, team_id, team, period, minute, second, type, formation)
            VALUES %s
            ON CONFLICT (game_id, minute, second) DO NOTHING;
        '''
        execute_values(cursor, insert_query, data)
    conn.commit()
    print("Data inserted into whoScored_events table")


# Function to map the formation based on formations_dict or leave as-is if not in dict
def get_mapped_formation(formation_code):
    return formations_dict.get(int(formation_code), str(formation_code))


# Function to convert period string to integer, with a default value
def convert_period(period):
    if period == "FirstHalf":
        return 1
    elif period == "SecondHalf":
        return 2
    else:
        return 1  # Default to 1 if period is unrecognized
 # fixture_id VARCHAR(255),
 #        event_time INT,
 #        team_id VARCHAR(255),
 #        event_type VARCHAR(255),
 #        detailed_type VARCHAR(255),
 #        main_player_id VARCHAR(255),
 #        secondary_player_id VARCHAR(255),

def insert_to_json(whoScored_array, possessions_json_path,  whoScored_json_path, season_Whoscored):
    with open(possessions_json_path, 'r') as f1:
        possessions_dict = json.load(f1)

    whoscored_to_footballAPI_dict = MatchToGameIDs.mapping_games_footballapi_whoscoredapi(39, int(season_Whoscored[:4]), "ENG-Premier League", season_Whoscored)

    with open(whoScored_json_path, 'r') as f2:
        events_list = json.load(f2)
    # values[1] = game_id, values[4] = home_team_id, values[5] = home_team, values[8] = away_team_id, values[9] = away_team, values[26] = date (YYYY-MM-DD HH:MM:SS+00:00)
    for i in range(len(whoScored_array)):
        temp_possesion_dict = possessions_dict[str(whoScored_array[i][1])]
        for minute, possessions in temp_possesion_dict.items():
            #home_team_id = select from team table where name = values[5] = home_team and season = int(season_Whoscored[:5])
            #awat_team = select from team table where name = values[9] = home_team and season = int(season_Whoscored[:5])
            # inserting events of home and away team
            try:
                events_list.append(
                    [whoscored_to_footballAPI_dict[whoScored_array[i][1]], minute, whoScored_array[i][4], "Possession", possessions[0], None, None])
                events_list.append(
                    [whoscored_to_footballAPI_dict[whoScored_array[i][1]], minute, whoScored_array[i][8], "Possession", possessions[1], None, None])
            except KeyError:
                print("error occurred in " + str(i))


    with open(whoScored_json_path, 'w') as f3:
        json.dump(events_list, f3, indent=4)

# Main code to fetch events and insert them into the database
if __name__ == "__main__":
    # Initialize WhoScored for the 2023-2024 season
    seasons = ['2023-2024']

    stages_id = []
    for season in seasons:

        ws = sd.WhoScored(leagues="ENG-Premier League", seasons=[season], headless=False)

        # Retrieve the schedule for the 2023-2024 season
        epl_schedule = ws.read_schedule()
        print(f"this is schedule for season {season}: \n epl schedule: {epl_schedule}")
        # values[1] = game_id, values[4] = home_team_id, values[5] = home_team, values[8] = away_team_id, values[9] = away_team, values[26] = date (YYYY-MM-DD HH:MM:SS+00:00)
        values = epl_schedule.values
        # Connect to the database
        # connect_to_db()
        # if conn is None:
        #     exit("Failed to connect to the database")

        possessions_json = r'C:\Users\peret\Desktop\possessions_data.json'
        WhoScored_jason = r'C:\Users\peret\Desktop\whoScoredEvents.json'
        # Load the existing data

        insert_to_json(values, possessions_json, WhoScored_jason, season)

        with open(possessions_json, 'r') as file:
            data = json.load(file)
        # If it's an empty dictionary, initialize it as an empty collection (for example, a dictionary)
        # if not data:
        #     data = {}
        #
        # data[game_id] = possessions_dict

        # Write the updated data back to the JSON file
        with open(possessions_json, 'w') as file:
            json.dump(data, file, indent=5)



        # for stage_id in epl_schedule['stage_id'].unique():
        #     stages_id.append(int(stage_id))
        # print(stages_id)

        # Loop through each match in the schedule
        for game_id in epl_schedule['game_id'].unique():
            # Convert game_id to a list to avoid TypeError
            game_id_list = [game_id]  # Convert game_id to a list

            # Read the events for the current match
            events_df = ws.read_events(match_id=game_id_list)  # Pass game_id as a list

            # Filter for FormationSet and FormationChange events
            formation_events_df = events_df[events_df['type'].isin(['FormationSet', 'FormationChange'])]

            # Prepare data for insertion
            data_to_insert = []
            for _, row in formation_events_df.iterrows():
                qualifiers = row['qualifiers']
                formation_value = None

                # Extract the formation value based on TeamFormation
                for qualifier in qualifiers:
                    if qualifier['type']['displayName'] == 'TeamFormation':
                        formation_value = get_mapped_formation(qualifier['value'])
                        break

                # Convert period to integer using the conversion function
                period_value = convert_period(row['period'])

                # Add the tuple to the data list for bulk insert
                data_to_insert.append((
                    row['game_id'],
                    row['team_id'],
                    row['team'],
                    period_value,  # Use converted period value
                    row['minute'],
                    row['second'],
                    row['type'],
                    formation_value
                ))

            # Insert the data into the database
            insert_game_events(data_to_insert)

    # Close the database connection
    if conn:
        conn.close()
        print("Database connection closed")


