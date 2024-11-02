import soccerdata as sd
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import sys

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

        # # Connect to the database
        # connect_to_db()
        # if conn is None:
        #     exit("Failed to connect to the database")

        for stage_id in epl_schedule['stage_id'].unique():
            stages_id.append(int(stage_id))
        print(stages_id)

        # Loop through each match in the schedule
        # for game_id in epl_schedule['game_id'].unique():
        #     # Convert game_id to a list to avoid TypeError
        #     game_id_list = [game_id]  # Convert game_id to a list
        #
        #     # Read the events for the current match
        #     events_df = ws.read_events(match_id=game_id_list)  # Pass game_id as a list
        #
        #     # Filter for FormationSet and FormationChange events
        #     formation_events_df = events_df[events_df['type'].isin(['FormationSet', 'FormationChange'])]
        #
        #     # Prepare data for insertion
        #     data_to_insert = []
        #     for _, row in formation_events_df.iterrows():
        #         qualifiers = row['qualifiers']
        #         formation_value = None
        #
        #         # Extract the formation value based on TeamFormation
        #         for qualifier in qualifiers:
        #             if qualifier['type']['displayName'] == 'TeamFormation':
        #                 formation_value = get_mapped_formation(qualifier['value'])
        #                 break
        #
        #         # Convert period to integer using the conversion function
        #         period_value = convert_period(row['period'])
        #
        #         # Add the tuple to the data list for bulk insert
        #         data_to_insert.append((
        #             row['game_id'],
        #             row['team_id'],
        #             row['team'],
        #             period_value,  # Use converted period value
        #             row['minute'],
        #             row['second'],
        #             row['type'],
        #             formation_value
        #         ))
        #
        #     # Insert the data into the database
        #     insert_game_events(data_to_insert)

    # Close the database connection
    if conn:
        conn.close()
        print("Database connection closed")


