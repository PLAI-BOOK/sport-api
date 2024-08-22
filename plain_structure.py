import http.client
import json
import sys
from dotenv import load_dotenv
import os


# this is a demo for calling the api and saving the response to a file
# the demo uses leagues?season=2023 as a demo, you can change that to whatever endpoint you want to call!
# it will save the response to a file with a dynamic name based on the endpoint called

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

# Adding parameters to the request (e.g., for a specific season and current leagues)
params = "/leagues?season=2023"

conn.request("GET", params, headers=headers)

res = conn.getresponse()
data = res.read()

# Decode the JSON response
response_data = json.loads(data.decode("utf-8"))

# create good filename
safe_filename = f"{base_url}{params.replace('?', '_').replace('&', '_').replace('=', '-').replace('/', '-')}.json"

# Save the JSON response to a file with a dynamic name
with open(safe_filename, 'w', encoding='utf-8') as file:
    json.dump(response_data, file, ensure_ascii=False, indent=4)