import json
from time import sleep

from connection import call_api

if __name__=="__main__":
    # Specify the API endpoint, headers, and directory to save the file
    # call if you dont have the file
    # params = "/fixtures?league=2&season=2022"  # Change as needed
    # call_api(params)

    path = "v3.football.api-sports.io-fixtures_league-2_season-2022.json"
    group_ids_in_play = []
    group_ids=set()
    fixture_ids = []
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
        fixtures = data['response']
        for fixture in fixtures:
            group_ids_in_play.append((fixture['teams']['home']['id'], fixture['teams']['away']['id']))
            group_ids.add(fixture['teams']['home']['id'])
            group_ids.add(fixture['teams']['away']['id'])
            fixture_ids.append(fixture['fixture']['id'])
    # Write group_ids_in_play to JSON file
    with open('group_ids_in_play.json', 'w', encoding='utf-8') as f:
        json.dump(group_ids_in_play, f, ensure_ascii=False, indent=4)

    # Write group_ids to JSON file (convert set to list)
    with open('group_ids.json', 'w', encoding='utf-8') as f:
        json.dump(list(group_ids), f, ensure_ascii=False, indent=4)

    # Write fixture_ids to JSON file
    with open('fixture_ids.json', 'w', encoding='utf-8') as f:
        json.dump(fixture_ids, f, ensure_ascii=False, indent=4)

