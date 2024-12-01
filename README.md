# sport-api

you need to create .env and put in 
API_KEY
BASE_URL

also you need to run:
pip install -r requirements.txt

to check a new endpoint look at easy_structure.py and do the same on a new folder with good name!
it will print the result in the folder in a new json file 


to call somethins with some parameters use something like:
params = "/teams/statistics?team=33&season=2019&league=39"

the leagues and sesons we need to pull:
seasons_dict = {2015:'2015-2016',2016:'2016-2017',2017:'2017-2018',2018:'2018-2019',2019:'2019-2020',2020:'2020-2021',2021:'2021-2022',2022:'2022-2023',2023:'2023-2024'}
leagues_dict = {39:'ENG-Premier League', 140:'ESP-La Liga', 61:'FRA-Ligue 1', 78:'GER-Bundesliga', 135:'ITA-Serie A'}


need to build new db to secondary leagues
ids: 
909 - 2022  - done

take from 2015:
94 - pulled 2015 - 2017 then problem from 2018 idk why
207 - league not found XD
218 - pulled 2015, 2016, 2017,2018,2019,2020, 2021
88


383 - 2016  - done