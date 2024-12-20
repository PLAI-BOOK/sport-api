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
from connection import *
from db import connectDB

# Load environment variables from .env file
conn = connectDB.get_db_connection()  # Connect to the default postgres database

# Create a cursor object
cur = conn.cursor()

def whoscored_call_api_schedule(league_name,season):
    chrome_path = Path("C:/Program Files/Google/Chrome/Application/chrome.exe")
    # for now we will pull only ENG-Premier League - otherwise, change it
    # season format for exm- '2020-2021'
    ws = sd.WhoScored(leagues=league_name, seasons=[season], path_to_browser = chrome_path, headless=False)
    schedule = ws.read_schedule()
    return schedule


columns = ["game_id","team_id","minute","second","type","outcome_type","player_id"]


# Function to insert data into the whoScored_events table
def insert_game_events(data):

    if len(data) == 0:
        print("No data to insert.")
        return

    with conn.cursor() as cursor:
        insert_query = '''
            INSERT INTO whoscored_events_plus_plus (game_id, team_id, minute, second, type, outcome_type,player_id)
            VALUES %s
            ON CONFLICT (game_id, minute, second) DO NOTHING;
        '''
        execute_values(cursor, insert_query, data)
    conn.commit()
    print("Data inserted into whoScored_events++ table")


def get_all_games_id():
    with conn.cursor() as cursor:
        fetch_query = '''
            SELECT DISTINCT game_id
            FROM whoScored_events;
        '''
        cursor.execute(fetch_query)
        game_ids = [row[0] for row in cursor.fetchall()]
    return game_ids


# Main code to fetch events and insert them into the database
if __name__ == "__main__":
    # Initialize WhoScored for the 2023-2024 season
    seasons = ['2015-2016', '2016-2017', '2017-2018', '2018-2019', '2019-2020', '2020-2021','2021-2022','2022-2023','2023-2024']
    stages_id = []

    # all_games_id_json_path = r"C:\Users\user\Desktop\jsons\all_games_id.json"
    #
    # with open(all_games_id_json_path, 'r') as file:
    #     data = json.load(file)
    #
    # if not data:
    #     data = []
    #
    # for game_id in get_all_games_id():
    #     data.append(game_id)
    #
    # with open(all_games_id_json_path, 'w') as file:
    #     json.dump(data, file, indent=4)



    for season in seasons:

        ws = sd.WhoScored(leagues="ITA-Serie A", seasons=[season], headless=False)

        # Retrieve the schedule for the 2023-2024 season
        epl_schedule = ws.read_schedule()
        # print(f"this is schedule for season {season}: \n epl schedule: {epl_schedule}")
        # epl_schedule.values()


        # for stage_id in epl_schedule['stage_id'].unique():
        #     stages_id.append(int(stage_id))
        # print(stages_id)

        # Loop through each match in the schedule
        for game_id in epl_schedule['game_id'].unique():
            # Convert game_id to a list to avoid TypeError
            game_id_list = [game_id]  # Convert game_id to a list

            # Read the events for the current match
            events_df = ws.read_events(match_id=game_id_list)  # Pass game_id as a list
            if(events_df.empty):
                continue

            data_to_insert = []

            for _, row in events_df.iterrows():
                try:
                    # Normalize data for insertion
                    game_id = int(row['game_id'])
                    team_id = int(row['team_id'])
                    minute = float(row['minute']) if not pd.isna(row['minute']) else 0
                    second = float(row['second']) if not pd.isna(row['second']) else 0  # Replace NaN with 0
                    event_type = row['type']
                    outcome_type = row['outcome_type']
                    player_id = int(row['player_id']) if not pd.isna(row['player_id']) else None

                    data_to_insert.append((game_id, team_id, minute, second, event_type, outcome_type, player_id))

                except ValueError as e:
                    print(f"Skipping row due to error: {e}")
                    continue

            # Bulk insert
            insert_game_events(data_to_insert)
    conn.commit()
    # Close the database connection
    if conn:
        conn.close()
        print("Database connection closed")


