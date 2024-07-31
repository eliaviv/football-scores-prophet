from data.fbref_scraper import scrap_fbref

MAX_RETRIES = 10


def main():
    retries = 0
    try:
        scrap_fbref()
    except:
        retries += 1
        if retries < MAX_RETRIES:
            scrap_fbref()


if __name__ == '__main__':
    main()
