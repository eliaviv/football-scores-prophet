from service.scrappers import sofifa_scraper, fbref_scraper, clubelo_scrapper

MAX_RETRIES = 10


def scrap_data(db_client):
    sofifa_scraper.scrap_sofifa()
    scrap_fbref_with_retries(db_client, 0)
    clubelo_scrapper.scrap_clubelo_to_database(db_client)


def scrap_fbref_with_retries(db_client, retries):
    try:
        fbref_scraper.scrap_fbref(db_client)
    except Exception as e:
        print(f'Error: {e}')
        retries += 1
        if retries < MAX_RETRIES:
            scrap_fbref_with_retries(db_client, retries)