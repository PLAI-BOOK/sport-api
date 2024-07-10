import http.client
import requests
import json
from pprint import pprint

# test
api_key = "25a0d1c185e936e0e116132504637d1c"
base_url = "v3.football.api-sports.io"

print(api_key)


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

# Pretty print the JSON response
response_data = json.loads(data.decode("utf-8"))
pprint(response_data)

url = "https://v3.football.api-sports.io/leagues?season=2023"

payload={}

response = requests.request("GET", url, headers=headers, data=payload)

# Pretty print the JSON response from the second request
response_data = response.json()
pprint(response_data)