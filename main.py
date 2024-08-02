from data.fbref_scraper import scrap_fbref
from db import sqlite_client
from service.data_organizer import add_players_data, agg_prev_games

MAX_RETRIES = 10


def scrap_data(db_client, retries):
    try:
        scrap_fbref(db_client)
    except Exception as e:
        print(f'Error: {e}')
        retries += 1
        if retries < MAX_RETRIES:
            scrap_data(db_client, retries)


def main():
    # db_client = sqlite_client.SQLiteClient()

    # scrap_data(db_client, 0)
    # add_players_data(db_client)
    agg_prev_games()

if __name__ == '__main__':
    main()
