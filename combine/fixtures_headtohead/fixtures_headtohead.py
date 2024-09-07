import json
from time import sleep
from connection import call_api

path = "../group_ids_in_play.json"
with open(path, encoding='utf-8') as f:
    data = json.load(f)
    for home,away in data:
        params = f"/fixtures/headtohead?h2h={home}-{away}"
        call_api(params)
        sleep(10)