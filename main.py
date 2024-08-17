import pandas as pd

from data.clubelo_scrapper import scrap_clubelo_to_database
from data.fbref_scraper import scrap_fbref
from db import sqlite_client
from service.data_organizer import add_players_data, agg_prev_games, fix_xscore

MAX_RETRIES = 10


def scrap_data(db_client, retries):
    try:
        scrap_fbref(db_client)
    except Exception as e:
        print(f'Error: {e}')
        retries += 1
        if retries < MAX_RETRIES:
            scrap_data(db_client, retries)


def naive_score():
    df = pd.read_csv('output/matches_with_players.csv')

    # Calculate accuracy
    # accuracy = (df['Score'] == df['xScore']).mean()
    accuracy = (df['Score'] == df['xScoreElo']).mean()

    # Print the accuracy
    print(f"Baseline Accuracy: {accuracy:.2%}")


def main():
    db_client = sqlite_client.SQLiteClient()

    # scrap_data(db_client, 0)
    # add_players_data(db_client)
    # agg_prev_games(db_client)
    # fix_xscore()
    naive_score()
    # scrap_clubelo_to_database('db/matches.db')


if __name__ == '__main__':
    main()
