
import time

from connection import *
from db import connectDB

COUNT_PULL_REQUESTS = 0
COUNT_PULL_REQUESTS_PER_DAY = 37250
# Load environment variables from .env file
# choose the database you wanna connect to
# conn = connectDB.get_db_connection(db_name="second_league")
conn = connectDB.get_db_connection(db_name="workingdb")

# Create a cursor object
cur = conn.cursor()


# this function responsible to make sure we won't pull more than 450 requests in minute
def call_api_counter_caller(params):
    global COUNT_PULL_REQUESTS,COUNT_PULL_REQUESTS_PER_DAY
    COUNT_PULL_REQUESTS += 1
    COUNT_PULL_REQUESTS_PER_DAY+=1
    if COUNT_PULL_REQUESTS_PER_DAY == 75000:
        print("stoppppppppp aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        print(COUNT_PULL_REQUESTS_PER_DAY)
        print("stoppppppppp aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    if COUNT_PULL_REQUESTS == 449:
        print("!!!!!!!!! API requests is over 450 in minute, I'm going to sleep for 100 seconds !!!!!!!!!")
        time.sleep(100)
        COUNT_PULL_REQUESTS = 0
        call_api(params)
    return call_api(params)



# Step 1: Fetch all available leagues
def fetch_all_leagues(league_id):
    # params for leagues (you can modify as needed)
    params = f"/leagues?id={league_id}"  # Adjust this to retrieve all leagues if necessary

    # Call the API to fetch all pages of data
    all_data = call_api_counter_caller(params)

    leagues = []  # List to store all league information

    # Loop through all responses returned from the API (from multiple pages)
    for data in all_data:
        # print("League data retrieved successfully, this is the data: ", data["response"])

        # Loop through each league in the 'response' key from each page
        for league in data['response']:
            league_id = league['league']['id']
            league_name = league['league']['name']
            # Collect seasons for each league
            leagues.append({
                'league_id': league_id,
                'league_name': league_name,
                'seasons': [season['year'] for season in league['seasons']]
            })

    # Print final leagues data for debugging
    # print("This is the leagues data: ", leagues)

    return leagues

# Step 2: Pull Fixtures Data for each league and season
def pull_fixtures(league_id, season_year):
    params = f"/fixtures?league={league_id}&season={season_year}"
    # Fetch all pages of data from the API
    all_data = call_api_counter_caller(params)

    # Check if no data is returned from any page
    if not all_data or not all_data[0]['response']:
        print("data is empty on pull_fixtures")
        return

    # Process each page of fixture data
    for page_data in all_data:
        try:
            for fixture in page_data['response']:
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
                # print(f"Fixture ID: {fixture_id}, League ID: {league_id}, League Name: {league_name}, "
                #       f"Home Team ID: {home_team_id}, Home Team Name: {home_team_name}, "
                #       f"Away Team ID: {away_team_id}, Away Team Name: {away_team_name}, "
                #       f"Goals HT Home: {goals_half_time_home}, Goals HT Away: {goals_half_time_away}, "
                #       f"Goals FT Home: {goals_full_time_home}, Goals FT Away: {goals_full_time_away}")

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
        except Exception as e:
            print(f"Error occurred for fixture {fixture_id}: {e}")
            return

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

    # Fetch all pages of data from the API
    all_data = call_api_counter_caller(params)

    # Check if the response is empty for any page
    if not all_data or not all_data[0]['response']:
        print("Data is empty on pull_fixture_statistics")
        return
    try:
        fixture_id = str(fixture_id)
        # Fetch home and away team IDs from the database for this fixture
        cur.execute("SELECT home_team_id, away_team_id FROM Fixtures WHERE fixture_id = %s", (fixture_id,))
        teams = cur.fetchone()
    except Exception as e:
        print(e)
        return
    if teams:
        home_team_id, away_team_id = teams
    else:
        print(f"No teams found for fixture {fixture_id}")
        return

    # Loop through all pages of data
    for page_data in all_data:
        for team_stats in page_data['response']:
            try:
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
            except Exception as e:
                print(f"Error occurred for fixture {fixture_id}: {e}")
                return
    # print(f"Statistics for fixture {fixture_id} have been successfully processed.")

# Step 4: Pull Fixture Lineups
def pull_fixture_lineups(fixture_id):
    params = f"/fixtures/lineups?fixture={fixture_id}"

    # Fetch all pages of data from the API
    all_data = call_api_counter_caller(params)

    # Check if the response is empty
    if not all_data or not all_data[0]['response']:
        print("Data is empty on pull_fixture_lineups")
        return

    # Process each page (even though lineups are usually single-page)
    for page_data in all_data:
        if not page_data['response']:
            continue
        try:
            # Assume the first team is the home team, and the second is the away team
            home_team_lineup = page_data['response'][0]
            # print(home_team_lineup)
            away_team_lineup = page_data['response'][1]
            # print(away_team_lineup)

            # Process home team lineup
            home_team_id = home_team_lineup['team']['id']
            home_formation = home_team_lineup['formation']
            # print(home_formation)
            home_start_xi = [str(player['player']['id']) for player in home_team_lineup['startXI']]
            home_substitutes = [str(player['player']['id']) for player in home_team_lineup['substitutes']]

            # Process away team lineup
            away_team_id = away_team_lineup['team']['id']
            away_formation = away_team_lineup['formation']
            # print("*"*20)
            # print(f"this is the home team subs {home_substitutes}")
            # print(away_formation)
            away_start_xi = [str(player['player']['id']) for player in away_team_lineup['startXI']]
            away_substitutes = [(player['player']['id']) for player in away_team_lineup['substitutes']]

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
        except Exception as e:
            print(f"Error occurred for fixture {fixture_id}: {e}")
            return
    # print(f"Lineups updated for fixture {fixture_id}")


# Step 5: Pull Fixture Events
def pull_fixture_events(fixture_id):
    params = f"/fixtures/events?fixture={fixture_id}"

    # Fetch all pages of data from the API
    all_data = call_api_counter_caller(params)

    # Check if the response is empty
    if not all_data or not all_data[0]['response']:
        print("Data is empty on pull_fixture_events")
        return

    # Process each page of events data
    for page_data in all_data:
        if not page_data['response']:
            continue
        try:
            for event in page_data['response']:
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
        except Exception as e:
            print(f"Error occurred for fixture {fixture_id}: {e}")
            return
    # print(f"Events for fixture {fixture_id} have been successfully processed.")


# Step 6: Pull Team Data
def pull_team_data(team_id, season, league_id):
    params = f"/teams?id={team_id}"

    # Fetch all pages of data from the API
    all_data = call_api_counter_caller(params)

    # Check if the response is empty
    if not all_data or not all_data[0]['response']:
        print("Data is empty on pull_team_data")
        return

    # Process each page of team data
    for page_data in all_data:
        if not page_data['response']:
            continue
        try:
            for team in page_data['response']:
                team_id = str(team['team']['id'])
                team_name = team['team']['name']
                season = str(season)
                league_id = str(league_id)
                stadium_capacity = team['venue']['capacity']
                # print(f"log!!!! {team_name} stadium capacity is {stadium_capacity}")



                # Insert team data into the Teams table
                cur.execute('''
                    INSERT INTO Teams (
                        team_id, team_name, season, league_id, stadium_capacity
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (team_id, season, league_id) DO NOTHING
                ''', (team_id, team_name, season, league_id, stadium_capacity))
        except Exception as e:
            print(f"Error occurred for team {team_id}: {e}")
            return
    # print(f"Team data for team {team_id} has been successfully processed.")


# Step 7: Pull Team Statistics
def pull_team_statistics(team_id, season_year, league_id):
    params = f"/teams/statistics?team={team_id}&season={season_year}&league={league_id}"

    # Fetch all pages of data from the API
    all_data = call_api_counter_caller(params)

    # Check if the response is empty
    if not all_data or not all_data[0]['response']:
        print("Data is empty on pull_team_statistics")
        return
    try:
        # Process each page of statistics data
        for page_data in all_data:
            if not page_data['response']:
                continue

            response = page_data['response']

            # Team form and fixture statistics
            form = response.get('form', None)
            games_played_home = response['fixtures']['played'].get('home', 0)
            games_played_away = response['fixtures']['played'].get('away', 0)
            wins_home = response['fixtures']['wins'].get('home', 0)
            wins_away = response['fixtures']['wins'].get('away', 0)
            draws_home = response['fixtures']['draws'].get('home', 0)
            draws_away = response['fixtures']['draws'].get('away', 0)
            losses_home = response['fixtures']['loses'].get('home', 0)
            losses_away = response['fixtures']['loses'].get('away', 0)

            # Goals per interval
            goals_scored = response['goals']['for']['minute']
            goals_conceded = response['goals']['against']['minute']

            # Clean sheets and failed to score
            clean_sheets = response['clean_sheet'].get('total', 0)
            failed_to_score = response['failed_to_score'].get('total', 0)

            # Convert penalty success rate to float, removing '%' symbol and handling null
            penalty_success_rate = response['penalty']['scored'].get('percentage', '0%').replace('%', '')
            penalty_success_rate = float(penalty_success_rate) if penalty_success_rate else 0.0

            # Over/Under data
            over_under = response['goals']['for']['under_over']

            # Yellow and Red Cards per interval
            yellow_cards = response['cards']['yellow']
            red_cards = response['cards']['red']

            # Update the Teams table with new statistics
            cur.execute('''
                UPDATE Teams
                SET 
                    games_played_home = %s, games_played_away = %s, 
                    wins_home = %s, wins_away = %s, 
                    draws_home = %s, draws_away = %s, 
                    losses_home = %s, losses_away = %s, 
                    team_form = %s,
                    goals_scored_0_15 = %s, goals_scored_16_30 = %s, goals_scored_31_45 = %s, 
                    goals_scored_46_60 = %s, goals_scored_61_75 = %s, goals_scored_76_90 = %s,
                    goals_scored_91_105 = %s, goals_scored_106_120 = %s,
                    goals_conceded_0_15 = %s, goals_conceded_16_30 = %s, goals_conceded_31_45 = %s,
                    goals_conceded_46_60 = %s, goals_conceded_61_75 = %s, goals_conceded_76_90 = %s,
                    goals_conceded_91_105 = %s, goals_conceded_106_120 = %s,
                    clean_sheets = %s, failed_to_score = %s, 
                    penalty_success_rate = %s,
                    over_0_5 = %s, under_0_5 = %s,
                    over_1_5 = %s, under_1_5 = %s,
                    over_2_5 = %s, under_2_5 = %s,
                    over_3_5 = %s, under_3_5 = %s,
                    over_4_5 = %s, under_4_5 = %s,
                    yellow_cards_0_15 = %s, yellow_cards_16_30 = %s, yellow_cards_31_45 = %s,
                    yellow_cards_46_60 = %s, yellow_cards_61_75 = %s, yellow_cards_76_90 = %s,
                    yellow_cards_91_105 = %s, yellow_cards_106_120 = %s,
                    red_cards_0_15 = %s, red_cards_16_30 = %s, red_cards_31_45 = %s,
                    red_cards_46_60 = %s, red_cards_61_75 = %s, red_cards_76_90 = %s,
                    red_cards_91_105 = %s, red_cards_106_120 = %s
                WHERE team_id = %s AND season = %s AND league_id = %s
            ''', (
                games_played_home, games_played_away,
                wins_home, wins_away,
                draws_home, draws_away,
                losses_home, losses_away,
                form,
                goals_scored['0-15']['total'] or 0, goals_scored['16-30']['total'] or 0, goals_scored['31-45']['total'] or 0,
                goals_scored['46-60']['total'] or 0, goals_scored['61-75']['total'] or 0, goals_scored['76-90']['total'] or 0,
                goals_scored['91-105']['total'] or 0, goals_scored['106-120']['total'] or 0,
                goals_conceded['0-15']['total'] or 0, goals_conceded['16-30']['total'] or 0, goals_conceded['31-45']['total'] or 0,
                goals_conceded['46-60']['total'] or 0, goals_conceded['61-75']['total'] or 0, goals_conceded['76-90']['total'] or 0,
                goals_conceded['91-105']['total'] or 0, goals_conceded['106-120']['total'] or 0,
                clean_sheets, failed_to_score, penalty_success_rate,
                over_under['0.5']['over'], over_under['0.5']['under'],
                over_under['1.5']['over'], over_under['1.5']['under'],
                over_under['2.5']['over'], over_under['2.5']['under'],
                over_under['3.5']['over'], over_under['3.5']['under'],
                over_under['4.5']['over'], over_under['4.5']['under'],
                yellow_cards['0-15']['total'] or 0, yellow_cards['16-30']['total'] or 0, yellow_cards['31-45']['total'] or 0,
                yellow_cards['46-60']['total'] or 0, yellow_cards['61-75']['total'] or 0, yellow_cards['76-90']['total'] or 0,
                yellow_cards['91-105']['total'] or 0, yellow_cards['106-120']['total'] or 0,
                red_cards['0-15']['total'] or 0, red_cards['16-30']['total'] or 0, red_cards['31-45']['total'] or 0,
                red_cards['46-60']['total'] or 0, red_cards['61-75']['total'] or 0, red_cards['76-90']['total'] or 0,
                red_cards['91-105']['total'] or 0, red_cards['106-120']['total'] or 0,
                team_id, str(season_year), str(league_id)
            ))
    except Exception as e:
        print(f"Error occurred for team {team_id}: {e}")
        return
    # print(f"Team statistics for team {team_id} have been successfully processed.")


# Main function to pull data for all leagues and seasons
def main():
    # Step 1: Fetch all leagues
    leagues = fetch_all_leagues("253")
    for league in leagues:
        try:
            league_id = league['league_id']
            league_name = league['league_name']

            for season in league['seasons']:
                # # remove the break when we want to pull all the seasons ,
                if season not in [2015]:
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

                    try:
                        # Fetch teams involved in each fixture
                        cur.execute("SELECT home_team_id, away_team_id FROM Fixtures WHERE fixture_id = %s", (fixture_id[0], ))
                        teams = cur.fetchone()
                    except Exception as e:
                        print(f"Error occurred for team {team_id}: {e} at line 496 and {fixture_id[0]} ")



                    # Step 6: Pull team data and statistics for home and away teams
                    for team_id in teams:
                        pull_team_data(team_id, season, league_id)
                        pull_team_statistics(team_id, season, league_id)
        except Exception as e:
            conn.rollback()
            print(f"transaction rollback due to error: {e}")
            print(e)
            continue
    # Commit all changes to the database
    conn.commit()

# pull data for each player
def pull_players(season_year, league_id):
    # Fetch all team IDs from the Teams table
    cur.execute("SELECT team_id FROM Teams WHERE season=%s",(str(season_year),))
    team_ids = cur.fetchall()

    for team_id in team_ids:
        params = f"/players?team={team_id[0]}&season={season_year}&league={league_id}"

        # Fetch all pages of player data for the team
        all_data = call_api_counter_caller(params)

        if not all_data or not all_data[0]['response']:
            print(f"Data is empty for team {team_id[0]} in pull_players")
            continue  # Move to the next team if no data is available
        try:

            # Process each page of player data
            for page_data in all_data:
                if not page_data['response']:
                    continue

                for item in page_data['response']:
                    player = item['player']
                    player_id = player['id']
                    firstname = player['firstname']
                    lastname = player['lastname']
                    age=player['age']
                    height =player['height'][:-3] if player['height'] else None
                    weight=player['weight'][:-3] if player['weight'] else None

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
                                season,player_id,firstname,lastname,age,height,weight, appearances, lineups, minutes_played, position, rating, captain,
                                substitutions_in, substitutions_out, bench_appearances, total_shots, shots_on_target,
                                total_goals, assists, goals_conceded, saves, total_passes, key_passes, pass_accuracy,
                                total_tackles, blocks, interceptions, total_duels, duels_won, dribble_attempts,
                                successful_dribbles, dribbled_past, fouls_drawn, fouls_committed, yellow_cards,
                                yellow_red_cards, red_cards, penalties_won, penalties_committed, penalties_scored,
                                penalties_missed, penalties_saved
                            )
                            VALUES (%s,%s, %s,%s, %s, %s, %s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (season, player_id) DO NOTHING
                        ''', (season_year,player_id,firstname,lastname,age,height,weight, appearances, lineups, minutes, position, rating, captain, substitutions_in,
                              substitutions_out, bench_appearances, total_shots, shots_on_target, total_goals, assists,
                              goals_conceded, saves, total_passes, key_passes, pass_accuracy, total_tackles, blocks,
                              interceptions, total_duels, duels_won, dribble_attempts, successful_dribbles, dribbled_past,
                              fouls_drawn, fouls_committed, yellow_cards, yellow_red_cards, red_cards, penalties_won,
                              penalties_committed, penalties_scored, penalties_missed, penalties_saved
                              ))
        except Exception as e:
            print(f"Error occurred for player {player_id}: {e}")
            continue
    # Commit the transaction to the database
    conn.commit()
    # print("Players data inserted successfully!")

#todo: pull data of injuries for each player
def pull_injuries():
    print("Fetching fixture IDs...")
    cur.execute("SELECT fixture_id FROM Fixtures")
    fixture_ids = cur.fetchall()  # Returns a list of tuples

    for fixture_id_row in fixture_ids:
        fixture_id = fixture_id_row[0]  # Extract fixture_id from the tuple
        params = f"/injuries?fixture={fixture_id}"

        print(f"Calling API with params: {params}")
        all_data = call_api_counter_caller(params)

        if not all_data:
            print(f"No data returned for fixture_id: {fixture_id}")
            continue

        for data in all_data:
            for entrance in data['response']:
                try:
                    # Extract required fields
                    player_id = entrance['player']['id']
                    team_id = entrance['team']['id']
                    league_id = entrance['league']['id']
                    league_season = entrance['league']['season']
                    photo = entrance['player']['photo']
                    type_info = entrance['player']['type']
                    reason=entrance['player']['reason']
                    # Insert data into the database
                    cur.execute("""
                        INSERT INTO injuries (
                            fixture_id, player_id, team_id, league_id, season, photo, type_info,reason
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
                        ON CONFLICT (fixture_id, player_id, team_id, league_id, season) DO NOTHING;
                    """, (fixture_id, player_id, team_id, league_id, league_season, photo, type_info,reason))
                except Exception as e:
                    print(f"Error inserting data for player_id {player_id}: {e}")

        # Commit once after processing all data for the current fixture_id
        conn.commit()
        print(f"Data for fixture_id {fixture_id} committed.")

    print("Injuries data processing completed.")


# if fixture_ids array is not empty, run all over the league, otherwise run only on the arrat
# only run pull_fixture_statistics to fix it
def check_fixtures_statistic(fixture_ids):
    if(len(fixture_ids) == 0 ):
        # Fetch fixture IDs from the database for a given league_id
        cur.execute("SELECT fixture_id FROM Fixtures WHERE league_id = %s",
                    ("39",))  # Use league_id in the query
        fixture_ids = cur.fetchall()  # Fetch all fixture IDs

        print(fixture_ids)
    else:
        for fixture_id in fixture_ids:
            # Step 3: Pull statistics for each fixture
            pull_fixture_statistics(fixture_id)

# Run the main function
if __name__ == "__main__":
    # main()
    # you need main to run players
    # pull_players(2020,39)
    # pull_players(2021, 39)
    # pull_players(2022, 39)
    # pull_players(2023, 39)
    # missing_data_yellow = [710930, 868319, 710682, 710693, 710876, 592764, 592208, 592153, 592237, 592753, 592243, 868109, 592794, 592273, 868316, 592857, 592799, 868270, 710571, 592754, 1035489, 592246, 592774, 710695, 592755, 710679, 868082, 868088, 867950, 868113, 592321, 1035037, 710858, 710795, 868058, 868093, 868107, 592849, 592228, 868233, 592769, 592250, 592327, 592289, 1035375, 710556, 868195, 868221, 592211, 1035519, 710879, 710604, 710753, 710747, 710823, 710635, 1035059, 592729, 710717, 1035309, 868064, 592866, 710762, 592728, 710558, 1035520, 592340, 868242, 710614, 710734, 592850, 592780, 710686]
    # missing_data_red = [868115, 592733, 868309, 592274, 710926, 592266, 710925, 710918, 592180, 868228, 1035367, 1035504, 1035434, 1035376, 868239, 710877, 592170, 868314, 710677, 710874, 868203, 1035319, 710827, 1035127, 710819, 592324, 710721, 710930, 868319, 1035390, 710932, 710929, 710682, 1035393, 868322, 592737, 1035450, 1035461, 592816, 710876, 868166, 1035509, 1035458, 710684, 592239, 710691, 868062, 710839, 592795, 1035044, 1035420, 868245, 868039, 867952, 592149, 710624, 710719, 710687, 1035086, 592283, 592285, 1035365, 1035054, 592852, 1035462, 592764, 1035496, 1035335, 1035312, 1035067, 592168, 592178, 592167, 592746, 1035296, 1035355, 1035400, 592848, 1035066, 592171, 1035075, 592262, 1035428, 1035323, 592191, 592345, 592200, 592193, 592823, 1035494, 1035089, 592770, 592208, 592206, 592202, 592817, 592874, 592291, 868185, 1035154, 868324, 592214, 592220, 868061, 868036, 1035130, 592223, 592231, 592225, 592237, 592753, 1035510, 1035474, 1035343, 1035142, 592243, 1035413, 868008, 868138, 592258, 868085, 592252, 1035051, 868216, 867988, 592254, 592259, 868278, 868013, 1035539, 592334, 1035513, 592264, 867977, 592748, 592263, 868109, 1035299, 592794, 592279, 592273, 1035111, 867993, 592840, 1035302, 867946, 1035455, 1035423, 868316, 592857, 592778, 592293, 1035353, 1035152, 1035347, 592758, 1035082, 592775, 592792, 1035358, 1035479, 1035392, 710755, 592311, 592860, 1035477, 867984, 868144, 1035134, 1035409, 592315, 592313, 592148, 592834, 592828, 1035057, 592333, 592784, 592799, 592341, 868080, 592203, 592820, 868267, 1035399, 592346, 592343, 592342, 1035099, 592858, 710672, 1035411, 1035357, 1035407, 592360, 867990, 592194, 1035085, 1035437, 1035541, 592730, 592809, 868270, 592270, 1035295, 710792, 1035530, 1035451, 1035076, 868236, 1035078, 710571, 592207, 592744, 867996, 710873, 592747, 592751, 592314, 592325, 592754, 592836, 710572, 710760, 868271, 1035553, 1035468, 868276, 592226, 868220, 1035140, 868005, 868146, 1035128, 1035489, 868004, 868022, 592851, 592246, 592863, 592826, 868252, 868244, 1035410, 868173, 592157, 710735, 592774, 592768, 868132, 1035039, 1035106, 867969, 592175, 868083, 868003, 1035529, 868119, 592275, 592307, 592787, 1035351, 592322, 710690, 868294, 1035139, 1035512, 592798, 868031, 592230, 1035097, 592797, 592142, 868130, 867949, 1035107, 592811, 1035444, 592362, 592765, 592310, 1035354, 868155, 867965, 867956, 592242, 867957, 868320, 592320, 1035340, 867963, 867955, 867971, 1035144, 1035073, 1035500, 592872, 592854, 1035109, 710695, 868277, 592347, 592755, 867972, 868151, 592869, 710741, 867978, 710805, 710562, 868131, 710685, 1035371, 868317, 868049, 868046, 868051, 868230, 592294, 592829, 868288, 868045, 1035310, 1035297, 1035100, 1035101, 592155, 592839, 592875, 710696, 1035060, 592793, 868261, 592318, 592815, 867999, 710808, 868037, 710872, 710712, 1035115, 1035527, 1035493, 1035123, 868191, 868179, 710733, 1035118, 868070, 868067, 868075, 1035379, 592772, 1035329, 1035490, 1035430, 868066, 592166, 868073, 592865, 1035318, 868193, 868019, 710679, 1035079, 868160, 710759, 1035377, 592217, 592855, 1035072, 710739, 592292, 592147, 868082, 592150, 1035370, 868026, 868235, 592300, 592162, 868076, 592835, 868033, 592189, 868243, 1035369, 592179, 592176, 868096, 868137, 710670, 868273, 1035523, 1035506, 592188, 592827, 592247, 710724, 868248, 1035526, 592236, 710934, 592197, 592196, 868077, 868081, 868224, 868234, 592215, 592219, 592216, 592212, 1035471, 592299, 868280, 1035129, 868268, 1035551, 867954, 710763, 710928, 1035517, 592235, 592864, 868088, 867947, 867951, 1035545, 710935, 867964, 867959, 867974, 867960, 867950, 868114, 868206, 710673, 710611, 868113, 1035294, 867973, 867985, 867975, 868065, 1035314, 592204, 710771, 1035147, 868000, 867986, 867987, 1035381, 592326, 592321, 710772, 710766, 710847, 868001, 592336, 592338, 868023, 868024, 592319, 592335, 1035514, 592234, 592344, 868028, 592339, 592349, 868034, 868092, 1035524, 592762, 592361, 1035508, 710886, 868139, 1035436, 868275, 592727, 592734, 592218, 592743, 710720, 710678, 592862, 868192, 868104, 1035445, 868207, 1035532, 710856, 1035306, 710703, 1035315, 867997, 710858, 592738, 592731, 868040, 710777, 1035344, 1035417, 710740, 868157, 710620, 868044, 592199, 868156, 592256, 710702, 868168, 710795, 868063, 868058, 592847, 710737, 868162, 1035362, 868250, 592767, 592771, 1035419, 1035389, 710804, 710713, 1035449, 592312, 868072, 592785, 868133, 710799, 1035432, 868305, 1035473, 868283, 710800, 592796, 710814, 592773, 710806, 710588, 1035052, 868091, 710788, 592810, 868094, 868089, 1035098, 592853, 592873, 1035077, 592871, 592308, 868093, 867966, 868152, 1035457, 868111, 868101, 868108, 868098, 868107, 868041, 1035092, 1035359, 868112, 1035304, 592844, 592213, 592192, 868097, 710596, 868180, 868134, 868129, 592838, 868311, 868122, 868150, 710723, 1035547, 1035311, 868255, 1035124, 710768, 710765, 1035081, 1035349, 710794, 868121, 868227, 868145, 1035071, 868147, 868149, 1035133, 868213, 1035515, 592145, 592143, 592151, 868183, 868313, 1035406, 592272, 592161, 592317, 592849, 868048, 710831, 868249, 868222, 592184, 592288, 710809, 710807, 710812, 710631, 1035386, 868184, 868172, 868057, 868176, 1035327, 592365, 710627, 868187, 868012, 868196, 868128, 592205, 592209, 868204, 710910, 868290, 868199, 868136, 592228, 1035131, 592290, 868232, 868175, 868215, 868143, 868043, 592284, 1035305, 710688, 1035418, 1035388, 867976, 592304, 592301, 592303, 868050, 592779, 1035360, 1035465, 1035424, 592789, 592297, 710818, 710825, 710842, 868238, 592366, 868178, 1035105, 710836, 710832, 868158, 1035151, 868253, 868140, 868233, 1035080, 868272, 1035414, 592163, 1035499, 592769, 592187, 710708, 710816, 710826, 868282, 868291, 868105, 592805, 710835, 710846, 868304, 1035143, 710848, 1035441, 868154, 710638, 1035522, 1035412, 867958, 1035366, 868325, 1035472, 592250, 710567, 710776, 592240, 1035439, 868256, 1035093, 1035087, 868265, 867989, 710568, 867992, 868035, 1035442, 1035041, 1035110, 1035324, 592281, 868210, 1035040, 592830, 1035396, 1035038, 592370, 868141, 710566, 868125, 868116, 1035068, 592296, 868117, 592327, 868120, 592289, 710577, 710576, 868127, 868135, 710584, 1035136, 1035333, 710616, 710749, 592269, 1035316, 592814, 592358, 592353, 592159, 1035552, 592295, 710748, 592843, 592306, 592330, 710585, 710810, 1035375, 592355, 1035384, 592350, 868018, 592141, 710556, 868188, 710561, 592831, 592186, 592364, 868259, 592756, 710565, 1035476, 710557, 710559, 1035531, 592741, 592368, 710593, 592287, 592276, 710595, 867982, 710586, 592185, 710592, 710590, 710600, 710599, 710613, 710598, 868014, 1035348, 592248, 592181, 868266, 592173, 710834, 868170, 1035112, 868079, 868142, 710609, 710625, 710606, 710622, 1035498, 868279, 868284, 592802, 710621, 710623, 710628, 710845, 868171, 868169, 710865, 710626, 710641, 710636, 592786, 868177, 1035484, 868007, 710653, 710647, 710651, 710649, 710664, 710659, 868189, 1035070, 867998, 868181, 868195, 710663, 710655, 710660, 1035525, 1035415, 868186, 1035464, 868198, 868201, 592244, 868208, 868221, 868299, 868214, 592211, 710668, 868254, 868301, 867948, 868209, 868194, 1035135, 868303, 710692, 1035469, 710669, 1035519, 710675, 710746, 1035368, 710602, 592201, 867983, 592818, 868318, 710689, 592229, 1035352, 710700, 1035480, 710704, 710705, 710701, 710694, 592800, 592298, 868226, 868295, 710698, 710879, 710699, 710697, 710892, 1035495, 710919, 710706, 710710, 1035156, 1035528, 592169, 710604, 592745, 868032, 868017, 710730, 710597, 710603, 710752, 868300, 710742, 1035382, 1035385, 1035298, 710753, 592819, 710608, 592253, 710747, 710680, 1035542, 592790, 868200, 868038, 710933, 868099, 710894, 868069, 1035408, 710757, 867953, 710770, 868217, 710824, 710780, 592742, 592806, 710774, 710769, 710648, 1035320, 710781, 710744, 710783, 710790, 868219, 710789, 1035544, 592842, 710801, 710793, 710852, 1035047, 710796, 592777, 710803, 592160, 1035538, 868084, 592352, 710815, 710731, 1035317, 710811, 710813, 1035046, 710931, 1035435, 710820, 592726, 710821, 710823, 710822, 592822, 1035398, 868010, 710619, 592752, 1035459, 710635, 868153, 592760, 1035074, 868257, 710630, 1035478, 592241, 1035402, 1035114, 710828, 710575, 710830, 710843, 1035313, 710652, 710829, 710833, 592245, 1035438, 1035549, 710633, 710840, 867981, 710853, 1035482, 710857, 1035063, 592190, 592165, 710860, 710861, 1035391, 1035426, 868298, 868237, 868269, 710869, 710738, 592305, 710859, 868205, 868165, 1035475, 868053, 710871, 868118, 592222, 868059, 710629, 1035518, 710707, 592776, 710867, 710870, 710883, 1035059, 1035452, 710849, 710882, 867994, 710851, 710884, 592351, 592735, 1035374, 710798, 710850, 710727, 1035405, 868163, 710891, 710903, 710893, 710902, 1035303, 710914, 868251, 710787, 710907, 592729, 1035502, 592302, 710905, 1035497, 868307, 710644, 710637, 710640, 710645, 710642, 868223, 592868, 592156, 710745, 710764, 1035126, 1035149, 1035053, 1035061, 868229, 1035125, 710717, 592359, 592227, 868274, 1035330, 592825, 592804, 710751, 592265, 1035416, 1035309, 592369, 710838, 710844, 868167, 1035346, 592329, 868263, 592238, 592783, 868159, 592354, 710657, 710868, 710904, 710662, 592759, 1035334, 1035516, 710665, 592763, 710671, 868042, 868281, 868056, 1035540, 710899, 710908, 1035138, 868054, 710913, 1035096, 1035069, 868292, 710895, 592232, 1035486, 868246, 868055, 592750, 868021, 1035448, 1035338, 592268, 868321, 868064, 868297, 1035501, 592866, 1035373, 1035102, 1035321, 592841, 710573, 868016, 592278, 1035322, 592736, 710862, 710866, 710863, 710875, 592286, 1035328, 1035491, 592861, 868052, 1035431, 1035548, 592255, 710880, 1035145, 710887, 592337, 710802, 868126, 592257, 710797, 710901, 1035534, 1035088, 592164, 710896, 710898, 710897, 710778, 868258, 868100, 710912, 1035492, 710915, 710909, 592328, 710762, 1035325, 868289, 710922, 710885, 710923, 1035404, 868287, 592846, 1035155, 1035337, 592332, 592812, 1035120, 710581, 592870, 710582, 867967, 710725, 1035425, 868285, 710711, 867962, 710718, 868068, 710722, 1035454, 1035108, 1035380, 1035401, 592821, 868106, 868315, 592728, 1035546, 868123, 868124, 592781, 1035440, 710728, 868087, 592146, 1035141, 868006, 868312, 868323, 592761, 1035153, 1035485, 1035387, 710736, 592859, 710676, 710683, 1035447, 592732, 710681, 868286, 710560, 710563, 710558, 1035520, 868002, 1035536, 1035331, 710564, 710569, 710578, 867979, 592791, 710587, 710570, 710583, 868074, 1035045, 592757, 592340, 868015, 710610, 710612, 592174, 868296, 868242, 710615, 710607, 710614, 710761, 710758, 592172, 592144, 868262, 1035356, 1035332, 592749, 1035503, 1035481, 868293, 868306, 710734, 592850, 710767, 868020, 868202, 1035042, 710775, 710617, 710654, 1035507, 1035466, 1035363, 1035511, 592780, 1035397, 710634, 868102, 1035378, 1035132, 1035521, 592740, 868071, 867995, 1035043, 1035372, 1035345, 868247, 1035453, 868148, 710888, 710889, 1035535, 710881, 710900, 1035403, 710601, 710916, 710920, 710854, 868310, 1035488, 710921, 1035443, 868308, 868264, 592348, 592233, 1035062, 1035433, 1035460, 592331, 868047, 710924, 1035137, 867968, 868240, 868086, 1035090, 710686, 868241, 867991, 710666, 710782, 1035483, 710784, 710779, 710667, 592803, 868190, 710715, 1035361, 710709]
    # missing_data_offside = [710925, 592180, 868203, 710721, 1035509, 592852, 592200, 592291, 868008, 592267, 592860, 1035301, 1035300, 592754, 1035489, 868083, 1035537, 710695, 1035527, 592236, 592216, 710928, 592837, 592234, 710858, 710620, 710800, 868089, 592192, 710817, 868213, 710807, 868184, 868196, 868199, 1035131, 710818, 868325, 592327, 1035350, 592295, 710559, 1035531, 592248, 868079, 710845, 710701, 710716, 710781, 710796, 1035402, 710652, 710633, 710860, 710870, 710883, 710844, 592238, 868054, 710913, 1035322, 1035328, 1035491, 868124, 710589, 710570, 868015, 592749, 868264, 1035062]
    # missing_data_set = set()
    # missing_data_set.update(missing_data_offside,missing_data_red,missing_data_offside)
    # check_fixtures_statistic(missing_data_set)
    # think if we want to add a column of league to each one
    #
    # for i in range(0,2):
    #     pull_players(2015+i, 88)

    # pull_injuries()


    print("bla, activate main maybe")


# Close the cursor and connection
cur.close()
conn.close()