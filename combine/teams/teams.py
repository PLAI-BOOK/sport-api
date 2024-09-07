import json
from time import sleep

from connection import call_api

path = "../group_ids.json"
with open(path, encoding='utf-8') as f:
    data = json.load(f)
    for team_id in data:
        params = f"/teams?id={team_id}"
        call_api(params)
        sleep(10)