import pandas as pd

from data.fbref_scraper import scrap_fbref, match_prev_league, agg_prev_games
from data.fifa_scraper import scrap_fifa


def main():
    # scrap_fifa()
    # fifa_df = load_fifa()
    # scrap_fbref(fifa_df)
    # match_prev_league()
    agg_prev_games()

def load_fifa():
    fifa_df = pd.read_csv('output/fifa_24_ratings.csv')
    return fifa_df


if __name__ == '__main__':
    main()
