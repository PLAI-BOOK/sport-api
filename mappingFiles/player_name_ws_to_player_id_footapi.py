import time
from whoScored_api import *
from connection import *
from db import connectDB
import unicodedata
from fuzzywuzzy import fuzz


conn = connectDB.get_db_connection(db_name="workingdb")

# Create a cursor object
cur = conn.cursor()

COUNT_PULL_REQUESTS = 0
COUNT_PULL_REQUESTS_PER_DAY = 37250


import unicodedata

def clean_string(input_string):
    """
    Normalizes a string to remove diacritical marks (e.g., č -> c, š -> s)
    and converts it to lowercase.

    Args:
        input_string (str): The input string to normalize.

    Returns:
        str: The cleaned, normalized string.
    """
    # Normalize the string to decompose special characters
    normalized = unicodedata.normalize('NFD', input_string)
    # Remove diacritical marks and convert to lowercase
    return ''.join(char for char in normalized if not unicodedata.combining(char)).lower()


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


def get_all_players_from_players(cur):
    try:
        # Dictionary to store players
        players_dict = {}
        # SQL query to fetch player_id, firstname, and lastname
        query = "SELECT player_id, firstname, lastname FROM Players"
        cur.execute(query)

        # Fetch all rows
        rows = cur.fetchall()

        # Populate the dictionary
        players_dict = {row[0]: f"{row[1]} {row[2]}" for row in rows}
        print("Players dictionary created successfully!")
        return players_dict

    except Exception as ex:
        print(f"exception caught while getting players dictionary from players: {ex}")


def get_all_players_from_pp(cur):
    players_dict = {}
    try:

        # SQL query to fetch player_id and player_name
        query = "SELECT player_id, player_name FROM whoScored_events_plus_plus WHERE player_id IS NOT NULL"
        cur.execute(query)

        # Fetch all rows
        rows = cur.fetchall()

        # Populate the dictionary
        players_dict = {row[0]: row[1] for row in rows}
        print("Players dictionary from whoScored_events_plus_plus created successfully!")
        return players_dict

    except Exception as ex:
        print(f"exception caught while getting players dictionary from PP: {ex}")


def clean_dict(input_dict):
    """
    Removes entries with None as key or value from a dictionary.

    Args:
        input_dict (dict): The input dictionary to be cleaned.

    Returns:
        dict: A cleaned dictionary with no None keys or values.
    """
    return {k: v for k, v in input_dict.items() if k is not None and v is not None}

# without fuzzy
def map_players2(dict_footapi, dict_pp):
    # Clean dictionaries to remove None keys or values
    dict_footapi = clean_dict(dict_footapi)
    dict_pp = clean_dict(dict_pp)

    mapping = {}
    unmatched_pp = []  # List for unmatched players in dict_pp

    # Iterate through the second dictionary (smaller table)
    for pp_id, pp_name in dict_pp.items():
        pp_name_cleaned = clean_string(pp_name)  # Normalize and clean name
        pp_name_words = set(pp_name_cleaned.split())  # Split into words for comparison
        matched = False

        for footapi_id, footapi_name in dict_footapi.items():
            footapi_name_cleaned = clean_string(footapi_name)  # Normalize and clean name
            footapi_name_words = set(footapi_name_cleaned.split())  # Split into words for comparison

            # Check if all words in the smaller name exist in the larger name
            if pp_name_words.issubset(footapi_name_words) or footapi_name_words.issubset(pp_name_words):
                mapping[footapi_id] = pp_id  # Map player_id from footapi to pp_id
                matched = True
                break  # Stop searching once a match is found

        if not matched:
            unmatched_pp.append((pp_id, pp_name))  # Add unmatched pp player

    return mapping, unmatched_pp

# with fuzzy
def map_players(dict_footapi, dict_pp, fuzzy_threshold=0.7):
    # Clean dictionaries to remove None keys or values
    dict_footapi = clean_dict(dict_footapi)
    dict_pp = clean_dict(dict_pp)

    mapping = {}
    unmatched_pp = []  # List for unmatched players in dict_pp

    # Iterate through the second dictionary (smaller table)
    for pp_id, pp_name in dict_pp.items():
        pp_name_cleaned = clean_string(pp_name)  # Normalize and clean name
        pp_name_words = set(pp_name_cleaned.split())  # Split into words for comparison
        matched = False

        for footapi_id, footapi_name in dict_footapi.items():
            footapi_name_cleaned = clean_string(footapi_name)  # Normalize and clean name
            footapi_name_words = set(footapi_name_cleaned.split())  # Split into words for comparison

            # Check if all words in the smaller name exist in the larger name
            if pp_name_words.issubset(footapi_name_words) or footapi_name_words.issubset(pp_name_words):
                mapping[footapi_id] = pp_id  # Map player_id from footapi to pp_id
                matched = True
                break  # Stop searching once a match is found

        if not matched:
            unmatched_pp.append((pp_id, pp_name))  # Add unmatched pp player

    # Fuzzy matching for unmatched players
    for pp_id, pp_name in unmatched_pp[:]:  # Iterate over a copy of unmatched_pp
        pp_name_cleaned = clean_string(pp_name)  # Normalize and clean name

        for footapi_id, footapi_name in dict_footapi.items():
            footapi_name_cleaned = clean_string(footapi_name)  # Normalize and clean name

            # Calculate fuzzy match score
            score = fuzz.token_set_ratio(pp_name_cleaned, footapi_name_cleaned)

            if score >= fuzzy_threshold:  # If score meets or exceeds the threshold
                mapping[footapi_id] = pp_id  # Map player_id from footapi to pp_id
                unmatched_pp.remove((pp_id, pp_name))  # Remove from unmatched list
                break  # Stop searching once a match is found

    return mapping, unmatched_pp

def save_to_json(data, file_path):
    """
    Saves a dictionary to a JSON file.

    Args:
        data (dict): The dictionary to save.
        file_path (str): The path to the JSON file.
    """
    try:
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data successfully saved to {file_path}")
    except Exception as e:
        print(f"Error saving data to JSON: {e}")

def load_from_json(file_path):
    """
    Loads a dictionary from a JSON file.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: The loaded dictionary.
    """
    try:
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
        print(f"Data successfully loaded from {file_path}")
        return data
    except FileNotFoundError:
        print(f"No file found at {file_path}. Returning an empty dictionary.")
        return {}
    except Exception as e:
        print(f"Error loading data from JSON: {e}")
        return {}


dict_footapi = get_all_players_from_players(cur)
dict_pp = get_all_players_from_pp(cur)
dict_map_players, unmatched_pp= map_players(dict_footapi, dict_pp,fuzzy_threshold=0.7)

# Save the mapping to a JSON file
file_path = "player_mapping.json"
save_to_json(dict_map_players, file_path)

print(f"this is the number of correct mapping: {len(dict_map_players)}")
print(unmatched_pp)
print(f"this is the number of unmatch players from pp: {len(unmatched_pp)}")

