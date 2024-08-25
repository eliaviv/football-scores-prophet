from db import sqlite_client
from service import scrapping_manager, data_organizer


def main():
    db_client = sqlite_client.SQLiteClient()
    # scrapping_manager.scrap_data(db_client)
    data_organizer.prepare_matches_for_modeling(db_client)


if __name__ == '__main__':
    main()
