import csv

from MatchToGameIDs import *
#from whoScored_api import  *
import soccerdata as sd
import pandas as pd


target_csv_path = r"C:\Users\user\Documents\GitHub\EDA\backup_2024-12-01\merged_events.csv"

whoscored_events_csv_path = r"C:\Users\user\Documents\GitHub\EDA\backup_2024-12-01\whoscored_events.csv"

fixtures_csv_path = r"C:\Users\user\Documents\GitHub\EDA\backup_2024-12-01\fixtures.csv"

whoscored_possession_json_path = r"C:\Users\user\Desktop\jsons\current_possession_data.json"

seasons = ['2015-2016', '2016-2017', '2017-2018', '2018-2019', '2019-2020', '2020-2021', '2021-2022', '2022-2023',
           '2023-2024']

leagues_dict = {39:'ENG-Premier League', 140:'ESP-La Liga', 61:'FRA-Ligue 1', 78:'GER-Bundesliga', 135:'ITA-Serie A'}

def get_dict_from_json(json_path):
    with open(json_path) as json_file:
        data = json.load(json_file)
    return data

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



if __name__ == "__main__":
    problematic_games_id = []
    unproccessed_possesions_games_id = []
    possessions_dict = get_dict_from_json(whoscored_possession_json_path)
    fixture_home_away_dict = get_fixture_home_away_dict(fixtures_csv_path)
    games_id = list(possessions_dict.keys())
    ######### Shapira, I need you to run each league in different run, 0 - 5 in the leagues_dict.keys()[i]
    ######### After each run please save the arrays that are printed!!!
    ######### the csv where it all saved for the EDA is in this address : C:\Users\user\Documents\GitHub\EDA\backup_2024-12-01\merged_events.csv
    ######### directory path - C:\Users\user\Documents\GitHub\EDA\backup_2024-12-01\
    ######### Name: merged_events.csv

    ################################ need to change each time - [0:1], [1:2], [2:3], [3:4], [4:5]
    for league_id in list(leagues_dict.keys())[0:1]:
        league_name = leagues_dict[league_id]
        for season_WhoScored in seasons:
            print(f"leauge: {league_name}, season WhoScored: {season_WhoScored}")
            season_FootballAPI = season_WhoScored[0:4]
            games_id_dict = mapping_games_footballapi_whoscoredapi(league_id, season_FootballAPI, league_name,
                                                                   season_WhoScored)
            ws = sd.WhoScored(leagues=league_name, seasons=[season_WhoScored], headless=False)
            epl_schedule = ws.read_schedule()
            whoscored_league_game_home_away_dict = get_whoscored_league_game_home_away_dict(epl_schedule)


            for game_id in epl_schedule['game_id'].unique():
                try:
                    fixture_id = str(games_id_dict[game_id])
                except Exception as e:
                    problematic_games_id.append(str(game_id))
                    continue
                df = pd.read_csv(whoscored_events_csv_path)

                # Filter events with specific game id
                filtered_rows = df[df['game_id'] == game_id]
                # insert and convert events from whoscored to footballAIP
                for i in range(len(filtered_rows)):
                    # Data to append
                    data_as_list = list(filtered_rows.iloc[i])
                    is_home = whoscored_league_game_home_away_dict[str(game_id)][0] == data_as_list[2]

                    if is_home:
                        team_id = fixture_home_away_dict[fixture_id][0]
                    else:
                        team_id = fixture_home_away_dict[fixture_id][1]

                    new_data = {
                        'fixture_id': [str(fixture_id)],
                        'event_time': [data_as_list[4]],
                        'team_id': [str(team_id)],
                        'event_type': [str(data_as_list[6])],
                        'detailed_type': [str(data_as_list[7])],
                        'main_player_id': [None],
                        'secondary_player_id': [None]
                    }
                    new_df = pd.DataFrame(new_data)

                    # Append to an existing CSV
                    new_df.to_csv(target_csv_path, mode='a', header=False, index=False)

                # adding possessions to events
                if str(game_id) not in games_id:
                    unproccessed_possesions_games_id.append(str(game_id))
                    continue
                game_possession_dict = possessions_dict[str(game_id)]
                home_team_id = fixture_home_away_dict[fixture_id][0]
                away_team_id = fixture_home_away_dict[fixture_id][1]
                for minute in list(game_possession_dict.keys()):
                    # if minute[2] == '+':
                    #     minute = str(int(minute[:2]) + int(minute[3:]))

                    home_possession = game_possession_dict[minute][0]
                    away_possession = game_possession_dict[minute][1]

                    home_data = {
                        'fixture_id': [str(fixture_id)],
                        'event_time': [minute],
                        'team_id': [str(home_team_id)],
                        'event_type': ["Possession"],
                        'detailed_type': [str(home_possession)],
                        'main_player_id': [None],
                        'secondary_player_id': [None]
                    }

                    away_data = {
                        'fixture_id': [str(fixture_id)],
                        'event_time': [minute],
                        'team_id': [str(away_team_id)],
                        'event_type': ["Possession"],
                        'detailed_type': [str(away_possession)],
                        'main_player_id': [None],
                        'secondary_player_id': [None]
                    }

                    home_df = pd.DataFrame(home_data)
                    away_df = pd.DataFrame(away_data)
                    # Append to an existing CSV
                    home_df.to_csv(target_csv_path, mode='a', header=False, index=False)
                    away_df.to_csv(target_csv_path, mode='a', header=False, index=False)




    print(problematic_games_id)
    print(unproccessed_possesions_games_id)
