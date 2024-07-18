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


def scrap_fbref(fifa_df):
    url, league, season = get_data_info()
    matches = get_matches_data(url)
    match_links = get_match_links(url, league)
    add_players_data(matches, match_links, fifa_df)

    # export data
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    matches.reset_index(drop=True).to_csv(f'{OUTPUT_PATH}/{league.lower()}_{season.lower()}_matches_data.csv',
                                          header=True,
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


def add_players_data(matches, match_links, fifa_df):
    matches['Home Avg Players Score'] = ''
    matches['Away Avg Players Score'] = ''
    for count, link in enumerate(match_links):
        tables = pd.read_html(link)
        for table in tables:
            try:
                table.columns = table.columns.droplevel()
            except:
                continue

        home_team_players_df = get_team_player_data([tables[3], tables[9]])
        away_team_players_df = get_team_player_data([tables[10], tables[16]])

        home_team_avg_score = calculate_avg_score(home_team_players_df['Player'].tolist(), fifa_df)
        away_team_avg_score = calculate_avg_score(away_team_players_df['Player'].tolist(), fifa_df)

        matches.loc[count, 'Home Avg Players Score'] = home_team_avg_score
        matches.loc[count, 'Away Avg Players Score'] = away_team_avg_score

        # sleep for 3 seconds after every game to avoid IP being blocked
        time.sleep(3)


def get_team_player_data(df):
    return reduce(lambda left, right:
                  pd.merge(left, right,
                           on=['Player', 'Nation', 'Age', 'Min'], how='outer'),
                  df).iloc[:-1]


def calculate_avg_score(players, fifa_df):
    total_score = 0
    for player_name in players:
        player = find_player(player_name, fifa_df)
        if player['Overall'].empty:
            player_overall = 70
        elif player['Overall'].shape[0] > 1:
            player_overall = int(player['Overall'].values[0])
        else:
            player_overall = int(player['Overall'])
        total_score += player_overall

    return round(total_score / len(players), 2)


def find_player(player_name, fifa_df):
    return fifa_df[fifa_df['Name'].apply(lambda x: all(part.lower() in x.lower() for part in player_name.split()))]
