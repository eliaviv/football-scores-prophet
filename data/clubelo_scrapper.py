import requests
import requests_cache
import pandas as pd
import sqlite3
from io import StringIO
import time
from fuzzywuzzy import process

# Setup caching for requests
requests_cache.install_cache('elo_cache', expire_after=None)

# Fetch Elo ratings from the API for a given date with caching
def fetch_elo_ratings(date):
    url = f"http://api.clubelo.com/{date}"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))

# Match team names using fuzzy matching from Elo results
def match_team_name(name, elo_data):
    teams = elo_data['Club'].tolist()
    match, score = process.extractOne(name, teams)
    if score > 80:  # Adjust the threshold as needed
        return match
    return None

# Update SQLite database with home and away Elo ratings for each match
def scrap_clubelo_to_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Add columns for home_elo and away_elo if they do not exist
    try:
        cursor.execute("ALTER TABLE matches ADD COLUMN home_elo REAL")
        cursor.execute("ALTER TABLE matches ADD COLUMN away_elo REAL")
    except sqlite3.OperationalError:
        # Columns may already exist; ignore error
        pass

    # Fetch existing matches
    query = "SELECT game_id, home, away, date FROM matches"
    matches = pd.read_sql_query(query, conn)

    for _, match in matches.iterrows():
        game_id = match['game_id']
        home = match['home']
        away = match['away']
        date = match['date']
        print(f"Fetching Elo ratings for match {game_id}, {date}, {home} vs {away}")

        # Fetch Elo ratings for the specific match date
        elo_data = fetch_elo_ratings(date)

        # Match team names to Elo data
        home_elo_team = match_team_name(home, elo_data)
        away_elo_team = match_team_name(away, elo_data)

        # Get Elo ratings from the matched teams
        home_elo = elo_data.loc[elo_data['Club'] == home_elo_team, 'Elo'].values
        away_elo = elo_data.loc[elo_data['Club'] == away_elo_team, 'Elo'].values

        home_elo_value = home_elo[0] if len(home_elo) > 0 else None
        away_elo_value = away_elo[0] if len(away_elo) > 0 else None

        # Update Elo ratings in the database
        cursor.execute(
            "UPDATE matches SET home_elo = ?, away_elo = ? WHERE game_id = ?",
            (home_elo_value, away_elo_value, game_id)
        )

        # Throttle the requests to avoid hitting the API too hard
        time.sleep(1)  # Adjust the sleep time as needed

    # Commit and close connection
    conn.commit()
    conn.close()
    print("Clubelo update complete")
