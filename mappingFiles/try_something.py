import csv

from mappingFiles.MatchToGameIDs import *
import soccerdata as sd
import pandas as pd
from json_functions.json_func import *

conn = connectDB.get_db_connection(db_name="workingdb")

# Create a cursor object
cur = conn.cursor()

seasons = ['2015-2016', '2016-2017', '2017-2018', '2018-2019', '2019-2020', '2020-2021', '2021-2022', '2022-2023',
           '2023-2024']

leagues_dict = {39:'ENG-Premier League', 140:'ESP-La Liga', 61:'FRA-Ligue 1', 78:'GER-Bundesliga', 135:'ITA-Serie A'}

players_mapping_path = r"C:\Users\user\Documents\GitHub\sport-api\mappingFiles\player_mapping.json"
fixtures_mapping_path = r"C:\Users\user\Documents\GitHub\sport-api\mappingFiles\all_fixtures_mapping.json"
teams_mapping_path = r"C:\Users\user\Documents\GitHub\sport-api\mappingFiles\all_teams_mapping.json"


def export_table_to_csv(table_name, csv_file_path, conn):
    """
    Exports an SQL table to a CSV file.

    Args:
        table_name (str): Name of the SQL table to export.
        csv_file_path (str): Path to save the CSV file.
        conn (psycopg2 connection): Database connection object.
    """
    try:
        # Read SQL table into a pandas DataFrame
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, conn)

        # Export DataFrame to CSV
        df.to_csv(csv_file_path, index=False)
        print(f"Table '{table_name}' exported successfully to {csv_file_path}")

    except Exception as e:
        print(f"Error exporting table '{table_name}' to CSV: {e}")

# todo: change it to read from the table and not from csv
def get_fixture_home_away_dict(csv_path):
    fixture_dict = {}
    with open(csv_path, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row if it exists
        for row in csv_reader:
            key = str(row[0])  # Element at the 0th index
            value = [row[3], row[5]]  # List with elements at the 3rd and 5th indices
            fixture_dict[key] = value
    return fixture_dict

def get_whoscored_league_game_home_away_dict(league_data_frame):
    game_home_away_dict = {}
    games_id = list(league_data_frame['game_id'])
    home_teams_id = list(league_data_frame['home_team_id'])
    away_teams_id = list(league_data_frame['away_team_id'])
    for i in range(len(league_data_frame)):
        game_home_away_dict[str(games_id[i])] = \
            [str(home_teams_id[i]), str(away_teams_id[i])]
    return game_home_away_dict

def create_dict_for_team_and_fixtures(league_id, season_FootballAPI, league_name,season_WhoScored):
    fixtures_dict, teams_dict = mapping_games_footballapi_whoscoredapi(league_id, season_FootballAPI, league_name,season_WhoScored)

    return fixtures_dict, teams_dict

def merge_all_fixtures(input_folder=r'C:\Users\user\Documents\GitHub\sport-api\mappingFiles', output_file='all_fixtures_mapping.json'):
    # List all files in the folder that start with "fixture_mapping_" and end with ".json"
    json_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder)
                  if f.startswith('fixture_mapping_') and f.endswith('.json')]

    if not json_files:
        raise FileNotFoundError(
            f"No files starting with 'fixture_mapping_' and ending with '.json' found in '{input_folder}'.")

    # Merge only the filtered JSON files
    return merge_json_files(json_files, output_file)

def merge_all_teams(input_folder=r'C:\Users\user\Documents\GitHub\sport-api\mappingFiles', output_file='all_teams_mapping.json'):
    """
    Merges all team mapping JSON files into one.

    Args:
        input_folder (str): Folder containing team JSON files (default: current directory).
        output_file (str): Name of the final merged JSON file (default: 'all_teams_mapping.json').
    """
    # List all files in the folder that start with "fixture_mapping_" and end with ".json"
    json_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder)
                  if f.startswith('team_mapping_') and f.endswith('.json')]

    if not json_files:
        raise FileNotFoundError(
            f"No files starting with 'team_mapping_' and ending with '.json' found in '{input_folder}'.")

    # Merge only the filtered JSON files
    return merge_json_files(json_files, output_file)

def merge_events_pp_to_csv(cur,game_to_fixture_dict,players_map_dict,teams_map_dict):
    # todo: change it to the actual mapping dictionaries we have
    # game_to_fixture_dict = merge_all_fixtures()
    # players_map_dict = load_from_json(players_mapping_path)
    # teams_map_dict = merge_all_teams()

    # File name for CSV
    csv_file_path = 'merged_events_pp.csv'

    # Open the CSV file in write mode if it does not exist, otherwise append mode
    file_mode = 'w'

    with open(csv_file_path, file_mode, newline='') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Write the header only if the file was just created
        if file_mode == 'w':
            csvwriter.writerow(['fixture_id', 'event_time', 'team_id', 'event_type', 'detailed_type', 'main_player_id', 'secondary_player_id'])

        # Query to fetch all rows from the whoScored_events_plus_plus table
        query = "SELECT game_id, team_id, minute, second, type, outcome_type, player_id FROM whoScored_events_plus_plus;"
        cur.execute(query)

        # Iterate over the rows
        for row in cur.fetchall():
            game_id = row[0]
            team_id = row[1]
            minute = row[2]
            second = row[3]
            event_type = row[4]
            outcome_type = row[5]
            player_id = row[6]

            # Example mappings (to be provided)
            fixed_game_id = game_to_fixture_dict.get(game_id)
            fixed_team_id = teams_map_dict.get(team_id)
            fixed_player_id = players_map_dict.get(player_id)

            # Ensure mappings exist
            if not fixed_game_id or not fixed_team_id or not fixed_player_id:
                print(f"Missing mapping for game_id: {game_id}, team_id: {team_id}, player_id: {player_id}")
                continue

            # Adjust event time
            if second > 30:
                minute += 1

            # Combine event_type and outcome_type
            detailed_type = f"{event_type}{outcome_type or ''}"

            # Write row to CSV
            csvwriter.writerow([fixed_game_id, minute, fixed_team_id, event_type, detailed_type, fixed_player_id, None])
            print(f"Inserted row into CSV: [{fixed_game_id}, {minute}, {fixed_team_id}, {event_type}, {detailed_type}, {fixed_player_id}, None]")

def main(cur,leagues_dict,seasons):
    # todo: I'll use gal's flow for now - but we must fix it and do the flow better and more organzie

    export_table_to_csv('fixtures','fixtures_table.csv',conn)
    ################################ need to change each time - [0:1], [1:2], [2:3], [3:4], [4:5]
    for i in range(5):
        for league_id in list(leagues_dict.keys())[i:i+1]:
            print("*************************************************")
            league_name = leagues_dict[league_id]
            print('**************************************************')
            for season_WhoScored in seasons:
                print('**************************************************')
                print(f"leauge: {league_name}, season WhoScored: {season_WhoScored}")
                print('**************************************************')
                season_FootballAPI = season_WhoScored[0:4]
                games_id_dict,teams_dict = mapping_games_footballapi_whoscoredapi(league_id, season_FootballAPI, league_name,
                                                                       season_WhoScored)

    game_to_fixture_dict = merge_all_fixtures()
    players_map_dict = load_from_json(players_mapping_path)
    teams_map_dict = merge_all_teams()
    merge_events_pp_to_csv(cur, game_to_fixture_dict, players_map_dict, teams_map_dict)


def do_on():
    main(cur,leagues_dict,seasons)


# game_to_fixture_dict = merge_all_fixtures()
# players_map_dict = load_from_json(players_mapping_path)
# teams_map_dict = merge_all_teams()
game_to_fixture_dict = load_from_json(fixtures_mapping_path)
players_map_dict = load_from_json(players_mapping_path)
teams_map_dict = load_from_json(teams_mapping_path)
merge_events_pp_to_csv(cur, game_to_fixture_dict, players_map_dict, teams_map_dict)


