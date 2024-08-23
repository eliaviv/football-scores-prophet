import random
import time
from datetime import datetime
from functools import reduce

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup as soup

LEAGUES = {
    # 'Premier-League': '9',
    # 'Serie-A': '11',
    # 'La-Liga': '12',
    # 'Ligue-1': '13',
    # 'Bundesliga': '20'
}
SEASONS = ['2019-2020', '2020-2021', '2021-2022', '2022-2023', '2023-2024']


def scrap_fbref(db_client):
    for league in LEAGUES.keys():
        for season in SEASONS:
            print(f'Start scrapping league: {league} and season: {season}')

            url = f'https://fbref.com/en/comps/{LEAGUES[league]}/{season}/schedule/{season}-{league}-Scores-and-Fixtures'
            matches_df = get_matches_data(url, league, season)
            for i, match_row in matches_df.iterrows():
                if not db_client.find_matches_by_date_time_home_away(match_row['Date'], match_row['Time'],
                                                                     match_row['Home'], match_row['Away']).empty:
                    continue

                home_team_players_df, away_team_players_df = get_players_data(match_row['Match Link'])
                db_client.persist_match(match_row)
                db_client.persist_players(home_team_players_df, match_row['Game ID'], 1)
                db_client.persist_players(away_team_players_df, match_row['Game ID'], 0)

                print(f'Done match number: {i + 1}')

                time.sleep(5)

            print(f'Data collected for league: {league} and season: {season}')


def get_matches_data(url, league, season):
    print('Getting matches data...')
    tables = pd.read_html(url)
    matches_df = arrange_matches_data(tables, league, season)
    match_links = get_match_links(url, league)
    add_match_links_to_match_df(match_links, matches_df)
    matches_df = matches_df[
        ['Game ID', 'Wk', 'Day', 'Date', 'Time', 'Home', 'xG Home', 'G Home', 'Away', 'xG Away', 'G Away', 'League', 'Season', 'Match Link', 'Score']]

    return matches_df


def add_match_links_to_match_df(match_links, matches_df):
    if matches_df.shape[0] > len(match_links):
        raise ValueError('Not enough match links')

    if matches_df.shape[0] == len(match_links):
        matches_df['Match Link'] = match_links
        return

    for i, match_row in matches_df.iterrows():
        row_found = False
        match_row_indices_to_delete = []
        for match_link in match_links:
            match_link_details = match_link.split('/')[-1].split('-')
            league_length = len(match_row['League'].split('-'))
            link_date = convert_date_format('-'.join(match_link_details[-league_length - 3:-league_length]))
            link_league = '-'.join(match_link_details[-league_length:])
            if match_row['Date'] == link_date and match_row['League'] == link_league:
                row_found = True
                break

        if row_found:
            matches_df.loc[i, 'Match Link'] = match_link
            match_links.remove(match_link)
        else:
            match_row_indices_to_delete.append(i)


def arrange_matches_data(tables, league, season):
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
    matches['Game ID'] = [random.randint(10**(10-1), (10**10)-1) for x in range(len(matches))]
    matches['Score'] = np.select(conditions, choices)
    matches['League'] = [league] * len(matches)
    matches['Season'] = [season] * len(matches)
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


def get_players_data(match_link):
    tables = pd.read_html(match_link)
    for table in tables:
        try:
            table.columns = table.columns.droplevel()
        except Exception as e:
            continue

    try:
        home_team_players_df = get_team_player_data([tables[3], tables[9]])
        away_team_players_df = get_team_player_data([tables[10], tables[16]])
    except Exception as e:
        return None

    return home_team_players_df, away_team_players_df


def get_team_player_data(df):
    return reduce(lambda left, right:
                  pd.merge(left, right,
                           on=['Player', 'Nation', 'Age', 'Min'], how='outer'),
                  df).iloc[:-1]


def convert_date_format(date_str):
    date_obj = datetime.strptime(date_str, "%B-%d-%Y")
    new_date_str = date_obj.strftime("%Y-%m-%d")
    return new_date_str
