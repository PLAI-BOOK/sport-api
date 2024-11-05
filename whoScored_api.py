import soccerdata as sd
from pathlib import Path
import pandas as pd

# first install all this
# pip install soccerdata
# pip install packaging
# pip install setuptools
# pip install --upgrade undetected-chromedriver
# pip install selenium==4.12.0

def whoscored_call_api_schedule(league_name,season):
    chrome_path = Path("C:/Program Files/Google/Chrome/Application/chrome.exe")
    # for now we will pull only ENG-Premier League - otherwise, change it
    # season format for exm- '2020-2021'
    ws = sd.WhoScored(leagues="ENG-Premier League", seasons=[season], path_to_browser=chrome_path, headless=False)
    schedule = ws.read_schedule()
    return schedule





# Soccer Data API - https://soccerdata.readthedocs.io/en/latest/reference/whoscored.html - working only on WhoScored for now
# WhoScored - https://www.whoscored.com/
# example - but use this league and season!!!
if __name__ == "__main__":
    print("blah")
    # Path to Chrome executable
    # chrome_path = Path("C:/Program Files/Google/Chrome/Application/chrome.exe")
    # #
    # # Initialize WhoScored with path to Chrome browser
    # ws = sd.WhoScored(leagues="ENG-Premier League", seasons=['2023-2024'], path_to_browser=chrome_path,headless=False)
    # print(ws.__doc__)
    # # epl_schedule = ws.read_schedule()
    # example of events - should be written into table name events_whoscored
    # events_df = ws.read_events(match_id=1485186)
