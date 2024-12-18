import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
from db import connectDB

# Load environment variables from .env file
load_dotenv()

# Step 1: Try to create the database; if it exists, move on
try:
    conn = connectDB.get_db_connection()  # Connect to the default postgres database
    if conn is None:
        print("Error: Unable to connect to the default database.")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    DB_NAME = os.getenv("DB_NAME").lower()

    # Check if the database already exists
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
    exists = cur.fetchone()
    if not exists:
        # Database doesn't exist, so create it
        cur.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"Database {DB_NAME} created successfully!")
    else:
        print(f"Database {DB_NAME} already exists.")



    # Wait a few seconds to ensure that the creation is recognized (a brief delay can help)
    import time
    time.sleep(2)

except Exception as e:
    print(f"Error while connecting to the default database: {e}")

# Step 2: Connect to the newly created or existing database and create the tables
try:
    # SQL to create the Fixtures table
    create_fixtures_table = '''
    CREATE TABLE IF NOT EXISTS Fixtures (
        fixture_id VARCHAR(255) PRIMARY KEY,
        league_id VARCHAR(255),
        league_name VARCHAR(255),
        home_team_id VARCHAR(255),
        home_team_name VARCHAR(255),
        away_team_id VARCHAR(255),
        away_team_name VARCHAR(255),
        goals_half_time_home INT,
        goals_half_time_away INT,
        goals_full_time_home INT,
        goals_full_time_away INT,
        goals_extra_time_home INT,
        goals_extra_time_away INT,
        goals_penalty_home INT,
        goals_penalty_away INT,
        league_round VARCHAR(255),
        expected_goals_home FLOAT,
        shots_on_goal_home INT,
        shots_off_goal_home INT,
        shots_insidebox_home INT,
        shots_outsidebox_home INT,
        total_shots_home INT,
        blocked_shots_home INT,
        fouls_home INT,
        corner_kicks_home INT,
        offsides_home INT,
        ball_possession_home DOUBLE PRECISION,
        yellow_cards_home INT,
        red_cards_home INT,
        goalkeeper_saves_home INT,
        total_passes_home INT,
        passes_accurate_home INT,
        passes_percentage_home DOUBLE PRECISION,
        expected_goals_away FLOAT,
        shots_on_goal_away INT,
        shots_off_goal_away INT,
        shots_insidebox_away INT,
        shots_outsidebox_away INT,
        total_shots_away INT,
        blocked_shots_away INT,
        fouls_away INT,
        corner_kicks_away INT,
        offsides_away INT,
        ball_possession_away DOUBLE PRECISION,
        yellow_cards_away INT,
        red_cards_away INT,
        goalkeeper_saves_away INT,
        total_passes_away INT,
        passes_accurate_away INT,
        passes_percentage_away DOUBLE PRECISION,
        formation_home VARCHAR(255),
        formation_away VARCHAR(255),
        start_xi_home TEXT[],
        substitutions_home TEXT[],
        coach_home_team VARCHAR(255),
        start_xi_away TEXT[],
        substitutions_away TEXT[],
        coach_away_team VARCHAR(255)
    );
    '''

    # SQL to create the Events table
    create_events_table = '''
    CREATE TABLE IF NOT EXISTS Events (
        fixture_id VARCHAR(255),
        event_time INT,
        team_id VARCHAR(255),
        event_type VARCHAR(255),
        detailed_type VARCHAR(255),
        main_player_id VARCHAR(255),
        secondary_player_id VARCHAR(255),
        PRIMARY KEY (fixture_id, event_time, team_id, event_type),
        FOREIGN KEY (fixture_id) REFERENCES Fixtures (fixture_id) ON DELETE CASCADE
    );
    '''

    # SQL to create the Teams table
    create_teams_table = '''
CREATE TABLE IF NOT EXISTS Teams (
    team_id VARCHAR(255),
    team_name VARCHAR(255),
    season VARCHAR(255),
    league_id VARCHAR(255),
    team_form VARCHAR(255),
    games_played_home INT,
    games_played_away INT,
    wins_home INT,
    losses_home INT,
    draws_home INT,
    wins_away INT,
    losses_away INT,
    draws_away INT,
    stadium_capacity INT,
    team_country VARCHAR(255),
    goals_scored_0_15 INT,
    goals_scored_16_30 INT,
    goals_scored_31_45 INT,
    goals_scored_46_60 INT,
    goals_scored_61_75 INT,
    goals_scored_76_90 INT,
    goals_scored_91_105 INT,
    goals_scored_106_120 INT,
    goals_conceded_0_15 INT,
    goals_conceded_16_30 INT,
    goals_conceded_31_45 INT,
    goals_conceded_46_60 INT,
    goals_conceded_61_75 INT,
    goals_conceded_76_90 INT,
    goals_conceded_91_105 INT,
    goals_conceded_106_120 INT,
    clean_sheets INT,
    failed_to_score INT,
    penalty_success_rate DOUBLE PRECISION,
    over_0_5 INT,
    under_0_5 INT,
    over_1_5 INT,
    under_1_5 INT,
    over_2_5 INT,
    under_2_5 INT,
    over_3_5 INT,
    under_3_5 INT,
    over_4_5 INT,
    under_4_5 INT,
    yellow_cards_0_15 INT,
    yellow_cards_16_30 INT,
    yellow_cards_31_45 INT,
    yellow_cards_46_60 INT,
    yellow_cards_61_75 INT,
    yellow_cards_76_90 INT,
    yellow_cards_91_105 INT,
    yellow_cards_106_120 INT,
    red_cards_0_15 INT,
    red_cards_16_30 INT,
    red_cards_31_45 INT,
    red_cards_46_60 INT,
    red_cards_61_75 INT,
    red_cards_76_90 INT,
    red_cards_91_105 INT,
    red_cards_106_120 INT,
    lineups_per_game INT[],
    PRIMARY KEY (team_id, season, league_id)
);
    '''

    # # SQL to create the Players table
    # create_players_table = '''
    # CREATE TABLE IF NOT EXISTS Players (
    #     player_id VARCHAR(255) PRIMARY KEY,
    #     firstname VARCHAR(255),
    #     lastname VARCHAR(255),
    #     age INT,
    #     height INT,
    #     weight INT,
    #     appearances INT,
    #     lineups INT,
    #     minutes_played INT,
    #     position VARCHAR(255),
    #     rating FLOAT,
    #     captain BOOLEAN,
    #     substitutions_in INT,
    #     substitutions_out INT,
    #     bench_appearances INT,
    #     total_shots INT,
    #     shots_on_target INT,
    #     total_goals INT,
    #     assists INT,
    #     goals_conceded INT,
    #     saves INT,
    #     total_passes INT,
    #     key_passes INT,
    #     pass_accuracy INT,
    #     total_tackles INT,
    #     blocks INT,
    #     interceptions INT,
    #     total_duels INT,
    #     duels_won INT,
    #     dribble_attempts INT,
    #     successful_dribbles INT,
    #     dribbled_past INT,
    #     fouls_drawn INT,
    #     fouls_committed INT,
    #     yellow_cards INT,
    #     yellow_red_cards INT,
    #     red_cards INT,
    #     penalties_won INT,
    #     penalties_committed INT,
    #     penalties_scored INT,
    #     penalties_missed INT,
    #     penalties_saved INT
    # );
    # '''

    # SQL to create the Players table
    create_players_table = '''
    CREATE TABLE IF NOT EXISTS Players (
        season VARCHAR(255),
        player_id VARCHAR(255),
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        age INT,
        height VARCHAR(255),
        weight VARCHAR(255),
        appearances INT,
        lineups INT,
        minutes_played INT,
        position VARCHAR(255),
        rating FLOAT,
        captain BOOLEAN,
        substitutions_in INT,
        substitutions_out INT,
        bench_appearances INT,
        total_shots INT,
        shots_on_target INT,
        total_goals INT,
        assists INT,
        goals_conceded INT,
        saves INT,
        total_passes INT,
        key_passes INT,
        pass_accuracy INT,
        total_tackles INT,
        blocks INT,
        interceptions INT,
        total_duels INT,
        duels_won INT,
        dribble_attempts INT,
        successful_dribbles INT,
        dribbled_past INT,
        fouls_drawn INT,
        fouls_committed INT,
        yellow_cards INT,
        yellow_red_cards INT,
        red_cards INT,
        penalties_won INT,
        penalties_committed INT,
        penalties_scored INT,
        penalties_missed INT,
        penalties_saved INT,
        PRIMARY KEY (season, player_id)
    );
    '''

    # SQL to create the whoScored_events table
    create_whoScored_events_table = '''
    CREATE TABLE IF NOT EXISTS whoScored_events (
        game_id VARCHAR(255) NOT NULL,
        team VARCHAR(255),
        team_id VARCHAR(255),
        period INT NOT NULL,
        minute INT NOT NULL,
        second INT NOT NULL,
        type VARCHAR(50) NOT NULL,
        formation VARCHAR(255),
        PRIMARY KEY (game_id, minute, second)
    );
    '''

    # Execute the SQL statements
    cur.execute(create_fixtures_table)
    cur.execute(create_events_table)
    cur.execute(create_teams_table)
    cur.execute(create_players_table)
    cur.execute(create_whoScored_events_table)

    # Commit the transaction
    conn.commit()
    print("Tables created successfully!")

    # Close the cursor and connection
    cur.close()
    conn.close()


# if you want to add the new who scored this is the build:
# create_whoScored_events_table = '''
#     CREATE TABLE IF NOT EXISTS whoScored_events_plus_plus (
#         game_id VARCHAR(255) NOT NULL,
#         team_id VARCHAR(255),
#         minute INT NOT NULL,
# 		second INT NOT NULL,
#         type VARCHAR(50) NOT NULL,
#         outcome_type VARCHAR(255),
# 		player_id VARCHAR(255),
#         PRIMARY KEY (game_id, minute, second)
#     );
#     '''

except Exception as e:
    print(f"Error while connecting to the {DB_NAME} database: {e}")
