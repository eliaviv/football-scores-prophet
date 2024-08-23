import time
from io import StringIO

import pandas as pd
import requests
import requests_cache
from fuzzywuzzy import process

requests_cache.install_cache('elo_cache', expire_after=None)

SPECIAL_MATCHES = {
    "Arminia": "Bielefeld",
    "Athletic Club": "Bilbao",
    "Atlético Madrid": "Atletico",
    "Greuther Fürth": "Fuerth",
    "Köln": "Koeln",
    "Manchester Utd": "Man United"
}


# Match team names using fuzzy matching from Elo results
def scrap_clubelo_to_database(db_client):
    db_client.add_elo_columns_if_not_exists()
    matches_df = db_client.find_all_matches_filtered()

    for _, match in matches_df.iterrows():
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

        home_elo_value = home_elo[0] if len(home_elo) > 0 else 0
        away_elo_value = away_elo[0] if len(away_elo) > 0 else 0

        print(f"Elo values: {home_elo_value}, {away_elo_value}")

        db_client.update_elo_ratings(home_elo_value, away_elo_value, game_id)

        # Throttle the requests to avoid hitting the API too hard
        time.sleep(1)

    db_client.commit_changes()

    print("Clubelo update complete")


def fetch_elo_ratings(date):
    url = f"http://api.clubelo.com/{date}"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def match_team_name(name, elo_data):
    if name in SPECIAL_MATCHES:
        return SPECIAL_MATCHES[name]
    teams = elo_data['Club'].tolist()
    match, score = process.extractOne(name, teams)
    if score > 80:  # Adjust the threshold as needed
        return match
    return None
