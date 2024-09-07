import json
from time import sleep

from connection import call_api

path = "../group_ids.json"
with open(path, encoding='utf-8') as f:
    data = json.load(f)
    for team_id in data:
        params = f"/teams/statistics?team={team_id}&season=2022&league=2"  # Change as needed
        call_api(params)
        sleep(10)