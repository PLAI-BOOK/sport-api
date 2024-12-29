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

# Function to update the player_name column based on player_id
def update_player_names(events_df):
    if events_df.empty:
        print("No data to update.")
        return

    # Filter rows with valid player_id, player name, and game_id
    player_data = events_df[['player_id', 'player', 'game_id']].dropna().drop_duplicates()

    if player_data.empty:
        print("No valid player data found.")
        return

    # Ensure all player_id and game_id values are strings
    player_data['player_id'] = player_data['player_id'].apply(lambda x: str(int(x)) if isinstance(x, (float, int)) else str(x))
    player_data['game_id'] = player_data['game_id'].apply(lambda x: str(int(x)) if isinstance(x, (float, int)) else str(x))

    # Prepare data for individual updates
    for _, row in player_data.iterrows():
        player_name = row['player']
        player_id = row['player_id']
        game_id = row['game_id']

        update_query = '''
            UPDATE whoscored_events_plus_plus
            SET player_name = %s
            WHERE player_id = %s AND game_id = %s;
        '''

        try:
            with conn.cursor() as cursor:
                cursor.execute(update_query, (player_name, player_id, game_id))
            conn.commit()
            print(f"Updated player_name for player_id={player_id}, game_id={game_id}")
        except psycopg2.Error as e:
            print(f"Error during update for player_id={player_id}, game_id={game_id}: {e}")
            conn.rollback()




# Main code to fetch events and update player names
if __name__ == "__main__":
    leagues = ['ENG-Premier League', 'ESP-La Liga', 'FRA-Ligue 1' ,'GER-Bundesliga', 'ITA-Serie A']
    seasons = ['2015-2016', '2016-2017', '2017-2018', '2018-2019', '2019-2020', '2020-2021',
               '2021-2022', '2022-2023', '2023-2024']

    for league in leagues:
        for season in seasons:
            print(f"Processing league: {league}, season: {season}")
            ws = sd.WhoScored(leagues=league, seasons=[season], headless=False)

            # Retrieve the schedule for the season
            schedule = ws.read_schedule()

            # Loop through each match in the schedule
            for game_id in schedule['game_id'].unique():
                # Read the events for the current match
                events_df = ws.read_events(match_id=[game_id])  # Pass game_id as a list

                if events_df.empty:
                    continue

                # Update player names in the database
                update_player_names(events_df)

    # Close the database connection
    if conn:
        conn.close()
        print("Database connection closed.")



################################################################################################
#need to do la liga 2223, 2324 and 'FRA-Ligue 1', 'GER-Bundesliga', 'ITA-Serie A' all the seasons