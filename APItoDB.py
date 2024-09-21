import psycopg2
from connection import *
from dotenv import load_dotenv
from db import connectDB

# Load environment variables from .env file
load_dotenv()
conn = connectDB.get_db_connection()  # Connect to the default postgres database

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
        # manu added:
        goals_extra_time_home= fixture['score']['extratime']['home']
        goals_extra_time_away=fixture['score']['extratime']['away']
        goals_penalty_home=fixture['score']['penalty']['home']
        goals_penalty_away=fixture['score']['penalty']['away']
        league_round = fixture['league']['round']


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
                goals_full_time_home, goals_full_time_away,goals_extra_time_home,
                goals_extra_time_away,goals_penalty_home,goals_penalty_away,league_round
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)
            ON CONFLICT (fixture_id) DO NOTHING
        ''', (
            fixture_id, league_id, league_name,
            home_team_id, home_team_name,
            away_team_id, away_team_name,
            goals_half_time_home, goals_half_time_away,
            goals_full_time_home, goals_full_time_away,goals_extra_time_home,
            goals_extra_time_away,goals_penalty_home,goals_penalty_away,league_round
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
        if str(team_id) == str(home_team_id):
            team_type = 'home'
        elif str(team_id) == str(away_team_id):
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
                conn.commit()
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

# take data for each player
def takePlayers(season_year):
    cur.execute("SELECT team_id FROM Teams")
    team_ids = cur.fetchall()

    for team_id in team_ids:
        params = f"/players?team={team_id[0]}&season={season_year}"
        data = call_api(params)

        if not data['response']:
            print("Data is empty on takePlayers")
            return

        for item in data['response']:
            player = item['player']
            player_id = player['id']
            firstname = player['firstname']
            lastname = player['lastname']

            # Player stats from each league
            for stats in item['statistics']:
                # Extract relevant data from the response
                appearances = stats['games']['appearences'] if stats['games']['appearences'] is not None else 0
                lineups = stats['games']['lineups'] if stats['games']['lineups'] is not None else 0
                minutes = stats['games']['minutes'] if stats['games']['minutes'] is not None else 0
                position = stats['games']['position']
                rating = stats['games']['rating'] if stats['games']['rating'] is not None else None
                captain = stats['games']['captain']

                substitutions_in = stats['substitutes']['in'] if stats['substitutes']['in'] is not None else 0
                substitutions_out = stats['substitutes']['out'] if stats['substitutes']['out'] is not None else 0
                bench_appearances = stats['substitutes']['bench'] if stats['substitutes']['bench'] is not None else 0

                total_shots = stats['shots']['total'] if stats['shots']['total'] is not None else 0
                shots_on_target = stats['shots']['on'] if stats['shots']['on'] is not None else 0
                total_goals = stats['goals']['total'] if stats['goals']['total'] is not None else 0
                assists = stats['goals']['assists'] if stats['goals']['assists'] is not None else 0
                goals_conceded =  stats['goals']['conceded'] if stats['goals']['assists'] is not None else 0
                saves = stats['goals']['saves'] if stats['goals']['saves'] is not None else 0

                total_passes = stats['passes']['total'] if stats['passes']['total'] is not None else 0
                key_passes = stats['passes']['key'] if stats['passes']['key'] is not None else 0
                pass_accuracy = stats['passes']['accuracy'] if stats['passes']['accuracy'] is not None else 0

                total_tackles = stats['tackles']['total'] if stats['tackles']['total'] is not None else 0
                blocks = stats['tackles']['blocks'] if stats['tackles']['blocks'] is not None else 0
                interceptions = stats['tackles']['interceptions'] if stats['tackles']['interceptions'] is not None else 0

                total_duels = stats['duels']['total'] if stats['duels']['total'] is not None else 0
                duels_won = stats['duels']['won'] if stats['duels']['won'] is not None else 0
                dribble_attempts = stats['dribbles']['attempts'] if stats['dribbles']['attempts'] is not None else 0
                successful_dribbles = stats['dribbles']['success'] if stats['dribbles']['success'] is not None else 0
                dribbled_past = stats['dribbles']['past'] if stats['dribbles']['past'] is not None else 0

                fouls_drawn = stats['fouls']['drawn'] if stats['fouls']['drawn'] is not None else 0
                fouls_committed = stats['fouls']['committed'] if stats['fouls']['committed'] is not None else 0
                yellow_cards = stats['cards']['yellow'] if stats['cards']['yellow'] is not None else 0
                yellow_red_cards = stats['cards']['yellowred'] if stats['cards']['yellowred'] is not None else 0
                red_cards = stats['cards']['red'] if stats['cards']['red'] is not None else 0

                penalties_won = stats['penalty']['won'] if stats['penalty']['won'] is not None else 0
                penalties_committed = stats['penalty']['commited'] if stats['penalty']['commited'] is not None else 0
                penalties_scored = stats['penalty']['scored'] if stats['penalty']['scored'] is not None else 0
                penalties_missed = stats['penalty']['missed'] if stats['penalty']['missed'] is not None else 0
                penalties_saved = stats['penalty']['saved'] if stats['penalty']['saved'] is not None else 0


                # Insert data into the Players table
                cur.execute('''
                    INSERT INTO Players (
                        player_id,firstname,lastname, appearances, lineups, minutes_played, position, rating, captain,
                        substitutions_in, substitutions_out, bench_appearances, total_shots, shots_on_target,
                        total_goals, assists, goals_conceded, saves, total_passes, key_passes, pass_accuracy,
                        total_tackles, blocks, interceptions, total_duels, duels_won, dribble_attempts,
                        successful_dribbles, dribbled_past, fouls_drawn, fouls_committed, yellow_cards,
                        yellow_red_cards, red_cards, penalties_won, penalties_committed, penalties_scored,
                        penalties_missed, penalties_saved
                    )
                    VALUES (%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (player_id) DO UPDATE SET
                        appearances = EXCLUDED.appearances,
                        firstname = EXCLUDED.firstname,
                        lastname = EXCLUDED.lastname,
                        lineups = EXCLUDED.lineups,
                        minutes_played = EXCLUDED.minutes_played,
                        position = EXCLUDED.position,
                        rating = EXCLUDED.rating,
                        captain = EXCLUDED.captain,
                        substitutions_in = EXCLUDED.substitutions_in,
                        substitutions_out = EXCLUDED.substitutions_out,
                        bench_appearances = EXCLUDED.bench_appearances,
                        total_shots = EXCLUDED.total_shots,
                        shots_on_target = EXCLUDED.shots_on_target,
                        total_goals = EXCLUDED.total_goals,
                        assists = EXCLUDED.assists,
                        goals_conceded = EXCLUDED.goals_conceded,
                        saves = EXCLUDED.saves,
                        total_passes = EXCLUDED.total_passes,
                        key_passes = EXCLUDED.key_passes,
                        pass_accuracy = EXCLUDED.pass_accuracy,
                        total_tackles = EXCLUDED.total_tackles,
                        blocks = EXCLUDED.blocks,
                        interceptions = EXCLUDED.interceptions,
                        total_duels = EXCLUDED.total_duels,
                        duels_won = EXCLUDED.duels_won,
                        dribble_attempts = EXCLUDED.dribble_attempts,
                        successful_dribbles = EXCLUDED.successful_dribbles,
                        dribbled_past = EXCLUDED.dribbled_past,
                        fouls_drawn = EXCLUDED.fouls_drawn,
                        fouls_committed = EXCLUDED.fouls_committed,
                        yellow_cards = EXCLUDED.yellow_cards,
                        yellow_red_cards = EXCLUDED.yellow_red_cards,
                        red_cards = EXCLUDED.red_cards,
                        penalties_won = EXCLUDED.penalties_won,
                        penalties_committed = EXCLUDED.penalties_committed,
                        penalties_scored = EXCLUDED.penalties_scored,
                        penalties_missed = EXCLUDED.penalties_missed,
                        penalties_saved = EXCLUDED.penalties_saved
                ''', (player_id,firstname,lastname, appearances, lineups, minutes, position, rating, captain, substitutions_in,
                      substitutions_out, bench_appearances, total_shots, shots_on_target, total_goals, assists,
                      goals_conceded, saves, total_passes, key_passes, pass_accuracy, total_tackles, blocks,
                      interceptions, total_duels, duels_won, dribble_attempts, successful_dribbles, dribbled_past,
                      fouls_drawn, fouls_committed, yellow_cards, yellow_red_cards, red_cards, penalties_won,
                      penalties_committed, penalties_scored, penalties_missed, penalties_saved
                      ))

    # Commit the transaction to the database
    conn.commit()
    print("Players data inserted successfully!")

def check_fexturs_statistic():
    # Fetch fixture IDs from the database for a given league_id
    cur.execute("SELECT fixture_id FROM Fixtures WHERE league_id = %s",
                ("39",))  # Use league_id in the query
    fixture_ids = cur.fetchall()  # Fetch all fixture IDs

    print(fixture_ids)

    for fixture_id in fixture_ids:
        # Step 3: Pull statistics for each fixture
        pull_fixture_statistics(fixture_id[0])

# Run the main function
if __name__ == "__main__":
    # main()
    # takePlayers(2023)
    check_fexturs_statistic()


# Close the cursor and connection
cur.close()
conn.close()