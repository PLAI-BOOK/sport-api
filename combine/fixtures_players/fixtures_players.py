import json
from time import sleep

from connection import call_api

path = "../fixture_ids.json"
with open(path, encoding='utf-8') as f:
    data = json.load(f)
    for fixture_id in data:
        params = f"/fixtures/players?fixture={fixture_id}"
        call_api(params)
        sleep(10)