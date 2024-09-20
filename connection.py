import http.client
import json
import sys
from dotenv import load_dotenv
import os
conn=None
api_key=""
base_url=""
headers=""

# this will make the api call easier
# this will connect to the api
def _connect():
    global conn,api_key,base_url,headers
    # Ensure stdout is UTF-8 encoded
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
    # Load the .env file
    load_dotenv()
    # test
    api_key =os.getenv('API_KEY')
    base_url = os.getenv('BASE_URL')
    conn = http.client.HTTPSConnection(base_url)
    headers = {
        'x-rapidapi-host': base_url,
        'x-rapidapi-key': api_key
    }

# this will make the api call and save the response to a file in the directory you call it from with a dynamic name
def call_api(params):
    global conn,headers
    if not conn:
        _connect()
    try:
        conn.request("GET", params, headers=headers)
        res = conn.getresponse()
        data = res.read()
        # Decode the JSON response
        response_data = json.loads(data.decode("utf-8"))
    except Exception as e:
        print(f"Error: {e}")
        return
    # create good filename
    safe_filename = f"{base_url}{params.replace('?', '_').replace('&', '_').replace('=', '-').replace('/', '-')}.json"
    try:
        # Save the JSON response to a file with a dynamic name
        with open(safe_filename, 'w', encoding='utf-8') as file:
            json.dump(response_data, file, ensure_ascii=False, indent=4)
        # omri added - return the response
        return response_data
    except Exception as e:
        print(f"Error: {e}")
        return