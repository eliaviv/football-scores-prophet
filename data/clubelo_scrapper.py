import requests
import requests_cache
import pandas as pd
import sqlite3
from io import StringIO
import time
from fuzzywuzzy import process

# Setup caching for requests
requests_cache.install_cache('elo_cache', expire_after=3600)  # Cache expires after 1 hour

# Fetch Elo ratings from the API for a given date with caching
def fetch_elo_ratings(date):
    url = f"http://clubelo.com/API/{date}"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text), delimiter='\t')

# Match team names using fuzzy matching
def match_team_name(name, choices):
    match, score = process.extractOne(name, choices)
    if score > 80:  # Adjust the threshold as needed
        return match
    return None

# Update SQLite database with home and away Elo ratings for each match
def update_database(db_path):
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
    query = "SELECT id, home, away, date FROM matches"
    matches = pd.read_sql_query(query, conn)

    # Create a set of all team names from matches for fuzzy matching
    team_names = set(matches['home'].unique()).union(set(matches['away'].unique()))

    for _, match in matches.iterrows():
        match_id = match['id']
        home = match['home']
        away = match['away']
        date = match['date']
        print(f"fetching for match {match_id}")
        # Fetch Elo ratings for the specific match date
        elo_data = fetch_elo_ratings(date)

        # Process Elo ratings into a dictionary for quick lookups
        elo_dict = {}
        for _, row in elo_data.iterrows():
            elo_dict[row['Club']] = row['Elo']

        # Find best match for home and away teams
        home_elo_team = match_team_name(home, team_names)
        away_elo_team = match_team_name(away, team_names)

        home_elo = elo_dict.get(home_elo_team, None) if home_elo_team else None
        away_elo = elo_dict.get(away_elo_team, None) if away_elo_team else None

        # Update Elo ratings in the database
        cursor.execute(
            "UPDATE matches SET home_elo = ?, away_elo = ? WHERE id = ?",
            (home_elo, away_elo, match_id)
        )

        # Throttle the requests to avoid hitting the API too hard
        time.sleep(1)  # Adjust the sleep time as needed

    # Commit and close connection
    conn.commit()
    conn.close()
    print("done")
