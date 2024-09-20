import requests
import psycopg2
from connection import *
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

# Establish the connection
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME").lower(),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

# Create a cursor object
cur = conn.cursor()

# Step 1: Fetch all available leagues
def fetch_all_leagues():
    # params = "/leagues"
    #only for checking
    params = "/leagues?id=39"
    data = call_api(params)
    print("league data retrived successfully this is the data: ", data["response"])
    leagues = []
    for league in data['response']:
        league_id = league['league']['id']
        league_name = league['league']['name']
        leagues.append({
            'league_id': league_id,
            'league_name': league_name,
            'seasons': [season['year'] for season in league['seasons']]
        })
    print("this is leagues data: ",leagues)
    return leagues

# Step 2: Pull Fixtures Data for each league and season
def pull_fixtures(league_id, season_year):
    params = f"/fixtures?league={league_id}&season={season_year}"
    data = call_api(params)
    if not data['response']:
        print("data is empty on pull_fixtures")
        return

    for fixture in data['response']:
        fixture_id = fixture['fixture']['id']
        league_id = fixture['league']['id']
        league_name = fixture['league']['name']
        home_team_id = fixture['teams']['home']['id']
        home_team_name = fixture['teams']['home']['name']
        away_team_id = fixture['teams']['away']['id']
        away_team_name = fixture['teams']['away']['name']
        goals_half_time_home = fixture['score']['halftime']['home']
        goals_half_time_away = fixture['score']['halftime']['away']
        goals_full_time_home = fixture['score']['fulltime']['home']
        goals_full_time_away = fixture['score']['fulltime']['away']

        # Updated print statement with all values
        print(f"Fixture ID: {fixture_id}, League ID: {league_id}, League Name: {league_name}, "
              f"Home Team ID: {home_team_id}, Home Team Name: {home_team_name}, "
              f"Away Team ID: {away_team_id}, Away Team Name: {away_team_name}, "
              f"Goals HT Home: {goals_half_time_home}, Goals HT Away: {goals_half_time_away}, "
              f"Goals FT Home: {goals_full_time_home}, Goals FT Away: {goals_full_time_away}")

        # Insert fixture data into the Fixtures table
        cur.execute('''
            INSERT INTO Fixtures (
                fixture_id, league_id, league_name, 
                home_team_id, home_team_name, 
                away_team_id, away_team_name, 
                goals_half_time_home, goals_half_time_away, 
                goals_full_time_home, goals_full_time_away
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fixture_id) DO NOTHING
        ''', (
            fixture_id, league_id, league_name,
            home_team_id, home_team_name,
            away_team_id, away_team_name,
            goals_half_time_home, goals_half_time_away,
            goals_full_time_home, goals_full_time_away
        ))

def column_exists(stat_type, team_type):
    # Query the information schema to check if the column exists
    cur.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'fixtures'
        AND column_name = %s
    """, (f"{stat_type}_{team_type}",))
    return cur.fetchone() is not None  # Return True if the column exists

# Step 3: Pull Fixture Statistics
def pull_fixture_statistics(fixture_id):
    params = f"/fixtures/statistics?fixture={fixture_id}"
    data = call_api(params)
    if not data['response']:
        print("data is empty on pull_fixture_statistics")
        return

    # Fetch home and away team IDs from the database for this fixture
    cur.execute("SELECT home_team_id, away_team_id FROM Fixtures WHERE fixture_id = %s", (fixture_id,))
    teams = cur.fetchone()

    if teams:
        home_team_id, away_team_id = teams

    for team_stats in data['response']:
        team_id = team_stats['team']['id']

        # Determine if it's home or away team
        if team_id == home_team_id:
            team_type = 'home'
        elif team_id == away_team_id:
            team_type = 'away'
        else:
            print(f"Unknown team {team_id} for fixture {fixture_id}")
            continue

        # Loop through statistics and store them (e.g., shots, fouls, possession)
        for stat in team_stats['statistics']:
            if stat['type'] == "Passes %":
                stat_type = "passes_percentage"
            else:
                stat_type = stat['type'].lower().replace(' ', '_')  # Standardize the stat name
            stat_value = stat['value']

            # Convert percentage strings to floats if necessary
            if isinstance(stat_value, str) and '%' in stat_value:
                stat_value = float(stat_value.replace('%', '')) / 100.0  # Convert "34%" to 0.34

            # Check if the column exists before updating
            if column_exists(stat_type, team_type):
                # Insert fixture statistics data into the Fixtures table
                cur.execute(f'''
                    UPDATE Fixtures
                    SET {stat_type}_{team_type} = %s
                    WHERE fixture_id = %s
                ''', (stat_value, fixture_id))
            else:
                print(f"Column {stat_type}_{team_type} not found, skipping update.")


# Step 4: Pull Fixture Lineups
def pull_fixture_lineups(fixture_id):
    params = f"/fixtures/lineups?fixture={fixture_id}"
    data = call_api(params)
    if not data['response']:
        print("data is empty on pull_fixture_lineups")
        return
    # Assume the first team is the home team, and the second is the away team
    home_team_lineup = data['response'][0]
    print(home_team_lineup)
    away_team_lineup = data['response'][1]
    print(away_team_lineup)

    # Process home team lineup
    home_team_id = home_team_lineup['team']['id']
    home_formation = home_team_lineup['formation']
    print(home_formation)
    home_start_xi = [player['player']['id'] for player in home_team_lineup['startXI']]
    home_substitutes = [player['player']['id'] for player in home_team_lineup['substitutes']]

    # Process away team lineup
    away_team_id = away_team_lineup['team']['id']
    away_formation = away_team_lineup['formation']
    print(away_formation)
    away_start_xi = [player['player']['id'] for player in away_team_lineup['startXI']]
    away_substitutes = [player['player']['id'] for player in away_team_lineup['substitutes']]

    # Insert home team lineup into the Fixtures table
    cur.execute('''
        UPDATE Fixtures
        SET formation_home = %s, start_xi_home = %s, substitutions_home = %s
        WHERE fixture_id = %s
    ''', (home_formation, home_start_xi, home_substitutes, fixture_id))

    # Insert away team lineup into the Fixtures table
    cur.execute('''
        UPDATE Fixtures
        SET formation_away = %s, start_xi_away = %s, substitutions_away = %s
        WHERE fixture_id = %s
    ''', (away_formation, away_start_xi, away_substitutes, fixture_id))

    print(f"Lineups updated for fixture {fixture_id}")

# Step 5: Pull Fixture Events
def pull_fixture_events(fixture_id):
    params = f"/fixtures/events?fixture={fixture_id}"
    data = call_api(params)
    if not data['response']:
        print("data is empty on pull_fixture_events")
        return

    for event in data['response']:
        event_time = event['time']['elapsed']
        team_id = event['team']['id']
        event_type = event['type']
        detail = event['detail']
        main_player_id = event['player']['id']
        secondary_player_id = event['assist']['id'] if event['assist'] else None

        # Insert event data into the Events table
        cur.execute('''
            INSERT INTO Events (
                fixture_id, event_time, team_id, event_type, 
                detailed_type, main_player_id, secondary_player_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        ''', (
            fixture_id, event_time, team_id, event_type,
            detail, main_player_id, secondary_player_id
        ))

# Step 6: Pull Team Data
def pull_team_data(team_id,season,league_id):
    params =f"/teams?id={team_id}"
    data = call_api(params)
    if not data['response']:
        print("data is empty on pull_team_data")
        return

    for team in data['response']:
        team_id = team['team']['id']
        team_name = team['team']['name']
        # Insert team data into the Teams table
        cur.execute('''
            INSERT INTO Teams (
                team_id, team_name, season, league_id
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (team_id, season, league_id) DO NOTHING
        ''', (team_id, team_name, season, league_id))


# Step 7: Pull Team Statistics
def pull_team_statistics(team_id, season_year, league_id):
    params = f"/teams/statistics?team={team_id}&season={season_year}&league={league_id}"
    data = call_api(params)
    if not data['response']:
        print("data is empty on pull_team_statistics")
        return

    form = data['response']['form']
    games_played_home = data['response']['fixtures']['played']['home']
    games_played_away = data['response']['fixtures']['played']['away']
    wins_home = data['response']['fixtures']['wins']['home']
    wins_away = data['response']['fixtures']['wins']['away']
    draws_home = data['response']['fixtures']['draws']['home']
    draws_away = data['response']['fixtures']['draws']['away']
    losses_home = data['response']['fixtures']['loses']['home']
    losses_away = data['response']['fixtures']['loses']['away']

    # Insert team statistics into the Teams table
    cur.execute('''
        UPDATE Teams
        SET 
            games_played_home = %s, games_played_away = %s, 
            wins_home = %s, wins_away = %s, 
            draws_home = %s, draws_away = %s, 
            losses_home = %s, losses_away = %s, 
            team_form = %s
        WHERE team_id = %s
    ''', (
        games_played_home, games_played_away,
        wins_home, wins_away,
        draws_home, draws_away,
        losses_home, losses_away,
        form, team_id
    ))

# Main function to pull data for all leagues and seasons
def main():
    # Step 1: Fetch all leagues
    leagues = fetch_all_leagues()

    # for start to check if it works only for leagueID = 2, season = 2022

    for league in leagues:
        league_id = league['league_id']
        league_name = league['league_name']

        for season in league['seasons']:
            # remove the break when we want to pull all the seasons
            if season != 2023:
                continue
            print(f"Processing league: {league_name} ({league_id}), Season: {season}")

            # Step 2: Pull fixtures for the league and season
            pull_fixtures(league_id, season)

            # Fetch fixture IDs from the database for a given league_id
            cur.execute("SELECT fixture_id FROM Fixtures WHERE league_id = %s",
                        (str(league_id),))  # Use league_id in the query
            fixture_ids = cur.fetchall()  # Fetch all fixture IDs

            print(fixture_ids)

            for fixture_id in fixture_ids:
                # Step 3: Pull statistics for each fixture
                pull_fixture_statistics(fixture_id[0])
                print("fixture statistics pulled correctly")

                # Step 4: Pull lineups for each fixture
                pull_fixture_lineups(fixture_id[0])

                # Step 5: Pull events for each fixture
                pull_fixture_events(fixture_id[0])

                # Fetch teams involved in each fixture
                cur.execute("SELECT home_team_id, away_team_id FROM Fixtures WHERE fixture_id = %s", (fixture_id[0],))
                teams = cur.fetchone()

                # Step 6: Pull team data and statistics for home and away teams
                for team_id in teams:
                    pull_team_data(team_id, season, league_id)
                    pull_team_statistics(team_id, season, league_id)

    # Commit all changes to the database
    conn.commit()


# Run the main function
if __name__ == "__main__":
    main()

# Close the cursor and connection
cur.close()
conn.close()