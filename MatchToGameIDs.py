from connection import *
import soccerdata as sd
import time
from whoScored_api import *

COUNT_PULL_REQUESTS = 0
COUNT_PULL_REQUESTS_PER_DAY = 37250

def football_api_pull_fixtures_data(league_id,season):
    params = f"/fixtures?league={league_id}&season={season}"
    # Fetch all pages of data from the API
    fixtures_data = call_api_counter_caller(params)
    return fixtures_data

def football_api_json_extraction(fixtures_data):
    # Initialize dictionary to store extracted data
    fixtures_dict = {}

    # Extract data and store in the dictionary
    for fixture in fixtures_data[0]["response"]:
        fixture_id = fixture["fixture"]["id"]
        # format "2022-07-06T18:15:00+00:00"
        full_date = fixture["fixture"]["date"]
        # format - "YYYY-MM-DD"
        date = full_date[:10]
        home_team = fixture["teams"]["home"]["name"]
        away_team = fixture["teams"]["away"]["name"]

        fixtures_dict[fixture_id] = (date, home_team, away_team)

    return fixtures_dict

#for now we use only the league - ENG-Premier League
# season format is 'XXXX-XXXX' - for exm: '2023-2024'
def whoscored_api_pull_fixtures_data(league_name,season):
    fixtures_data = whoscored_call_api_schedule(league_name,season)
    print(fixtures_data)
    return fixtures_data

def whoscored_api_df_extraction(fixtures_data):
    # Initialize a dictionary to store the extracted data
    fixtures_dict = {}

    # Iterate over each row in the DataFrame
    for index, row in fixtures_data.iterrows():
        # Extract `game_id`
        game_id = row['game_id']

        # format "2022-07-06T18:15:00+00:00"
        full_date = row['start_time']
        # format - "YYYY-MM-DD"
        date = full_date[:10]
        home_team = row['home_team']
        away_team = row['away_team']

        # Add to the dictionary
        fixtures_dict[game_id] = (date,home_team,away_team)
    return fixtures_dict


# params: league_id - the league id for footballAPI, season_FootballAPI - the season by the format of footballAPI, league_name - the name of the league according to whoScoredAPI,season_WhoScored - the season by the format of whoScoredAPI
def mapping_games_footballapi_whoscoredapi(league_id, season_FootballAPI, league_name, season_WhoScored):
    # creating the dictionaries
    whoscored_dict = whoscored_api_df_extraction(whoscored_api_pull_fixtures_data(league_name=league_name,season=season_WhoScored))
    footballapi_dict= football_api_json_extraction(football_api_pull_fixtures_data(league_id=league_id,season=season_FootballAPI))

    # Resulting dictionary to map game_id to fixture_id
    mapping_dict = {}

    # Iterate over whoscored_dict and try to find matching entries in footballapi_dict
    for game_id, game_info in whoscored_dict.items():
        fixture_id = None
        for fid, f_info in footballapi_dict.items():
            # first checks that the date is similar
            if f_info[0] == game_info[0] :
                # check if one team played the game
                if(f_info[1] == game_info[1] or f_info[2] == game_info[2]):
                    fixture_id = fid
                    break
                elif (f_info[1].lower() in game_info[1].lower() or game_info[1].lower() in f_info[1].lower() or f_info[2].lower() in game_info[2].lower() or game_info[2].lower() in f_info[2].lower()):
                    fixture_id = fid
                    break

        # Check for duplicate mapping
        if game_info in mapping_dict.values():
            print(f"Error: Duplicate match found for {game_info} on {game_id} from footballAPI dict")
        else:
            # Map game_id to fixture_id
            mapping_dict[game_id] = fixture_id


    return mapping_dict

# this function responsible to make sure we won't pull more than 450 requests in minute
def call_api_counter_caller(params):
    global COUNT_PULL_REQUESTS,COUNT_PULL_REQUESTS_PER_DAY
    COUNT_PULL_REQUESTS += 1
    COUNT_PULL_REQUESTS_PER_DAY+=1
    if COUNT_PULL_REQUESTS_PER_DAY == 75000:
        print("stoppppppppp aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        print(COUNT_PULL_REQUESTS_PER_DAY)
        print("stoppppppppp aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    if COUNT_PULL_REQUESTS == 449:
        print("!!!!!!!!! API requests is over 450 in minute, I'm going to sleep for 100 seconds !!!!!!!!!")
        time.sleep(100)
        COUNT_PULL_REQUESTS = 0
        call_api(params)
    return call_api(params)


# dict_map = mapping_games_footballapi_whoscoredapi(39,2023,"ENG-Premier League",'2023-2024')
# print(dict_map)