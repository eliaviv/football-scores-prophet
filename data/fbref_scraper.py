import os
import time
from functools import reduce

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup as soup

LEAGUES = {
    'Premier-League': '9',
    'La-Liga': '12',
    'Serie-A': '11',
    'Ligue-1': '13',
    'Bundesliga': '20'
}
SEASONS = ['2017-2018', '2018-2019', '2019-2020', '2020-2021', '2021-2022', '2022-2023', '2023-2024']
OUTPUT_PATH = 'output'


def scrap_fbref():
    url, league, season = get_data_info()
    matches = get_matches_data(url)
    # match_links = get_match_links(url, league)
    # player_data(match_links, league, season)

    # export data
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    matches.reset_index(drop=True).to_csv(f'{OUTPUT_PATH}/{league.lower()}_{season.lower()}_matches_data.csv', header=True,
                                          index=False, mode='w')

    print('Data collected!')


def get_data_info():
    while True:
        # select league [Premier League / La Liga / Serie A / Ligue 1 / Bundesliga]
        # league = input('Select League (Premier League / La Liga / Serie A / Ligue 1 / Bundesliga): ')
        league = 'Premier-League'
        league = league.replace(' ', '-')

        # check if input valid
        if league not in LEAGUES:
            print('League not valid, try again')
            continue

        league_id = LEAGUES[league]
        break

    while True:
        # select season after 2017 as XG only available from 2017,
        # season = input('Select Season (2017-2018, 2018-2019, 2019-2020, 2020-2021, 2021-2022, 2022-2023, 2023-2024): ')
        season = '2023-2024'

        # check if input valid
        if season not in SEASONS:
            print('Season not valid, try again')
            continue
        break

    url = f'https://fbref.com/en/comps/{league_id}/{season}/schedule/{season}-{league}-Scores-and-Fixtures'
    return url, league, season


def get_matches_data(url):
    print('Getting matches data...')
    tables = pd.read_html(url)
    matches = arrange_matches_data(tables)
    matches = matches[
        ['Game ID', 'Wk', 'Day', 'Date', 'Time', 'Home', 'xG Home', 'G Home', 'Away', 'xG Away', 'G Away', 'Score']]
    print('Matches data collected...')
    return matches


def arrange_matches_data(tables):
    matches = tables[0][['Wk', 'Day', 'Date', 'Time', 'Home', 'xG', 'Away', 'xG.1', 'Score']].dropna()
    matches.rename(columns={'xG': 'xG Home', 'xG.1': 'xG Away'}, inplace=True)
    score_split = matches['Score'].str.split('–', expand=True)
    home_score = score_split[0]
    away_score = score_split[1]
    matches['G Home'] = home_score
    matches['G Away'] = away_score
    conditions = [
        matches['G Home'] > matches['G Away'],
        matches['G Home'] == matches['G Away'],
        matches['G Home'] < matches['G Away']
    ]
    choices = [1, 0, -1]
    matches['Score'] = np.select(conditions, choices)
    matches['Game ID'] = matches.index
    return matches


def get_match_links(url, league):
    print('Getting player data...')
    # access and download content from url containing all fixture links    
    match_links = []
    html = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    links = soup(html.content, "html.parser").find_all('a')

    # filter list to return only needed links
    key_words_good = ['/en/matches/', f'{league}']
    for l in links:
        href = l.get('href', '')
        if all(x in href for x in key_words_good):
            if 'https://fbref.com' + href not in match_links:
                match_links.append('https://fbref.com' + href)

    return match_links


def player_data(match_links, league, season):
    # loop through all fixtures
    player_data = pd.DataFrame([])
    for count, link in enumerate(match_links):
        try:
            tables = pd.read_html(link)
            for table in tables:
                try:
                    table.columns = table.columns.droplevel()
                except Exception:
                    continue

            # get player data
            def get_team_1_player_data():
                # outfield and goal keeper data stored in seperate tables 
                data_frames = [tables[3], tables[9]]

                # merge outfield and goal keeper data
                df = reduce(lambda left, right: pd.merge(left, right,
                                                         on=['Player', 'Nation', 'Age', 'Min'], how='outer'),
                            data_frames).iloc[:-1]

                # assign a home or away value
                return df.assign(home=1, game_id=count)

            # get second teams  player data        
            def get_team_2_player_data():
                data_frames = [tables[10], tables[16]]
                df = reduce(lambda left, right: pd.merge(left, right,
                                                         on=['Player', 'Nation', 'Age', 'Min'], how='outer'),
                            data_frames).iloc[:-1]
                return df.assign(home=0, game_id=count)

            # combine both team data and export all match data to csv
            t1 = get_team_1_player_data()
            t2 = get_team_2_player_data()
            player_data = pd.concat([player_data, pd.concat([t1, t2]).reset_index()])

            print(f'{count + 1}/{len(match_links)} matches collected')
            player_data.to_csv(f'{league.lower()}_{season.lower()}_player_data.csv',
                               header=True, index=False, mode='w')
        except:
            print(f'{link}: error')

        # sleep for 3 seconds after every game to avoid IP being blocked
        time.sleep(3)
