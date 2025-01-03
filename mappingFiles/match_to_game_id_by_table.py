import time
from whoScored_api import *
from connection import *
from db import connectDB

conn = connectDB.get_db_connection(db_name="workingdb")

# Create a cursor object
cur = conn.cursor()

COUNT_PULL_REQUESTS = 0
COUNT_PULL_REQUESTS_PER_DAY = 37250

seasons = ['2015-2016', '2016-2017', '2017-2018', '2018-2019', '2019-2020', '2020-2021', '2021-2022', '2022-2023',
           '2023-2024']

leagues_dict = {39:'ENG-Premier League', 140:'ESP-La Liga', 61:'FRA-Ligue 1', 78:'GER-Bundesliga', 135:'ITA-Serie A'}