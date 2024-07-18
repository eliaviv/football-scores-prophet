import time
import urllib
from urllib import request

import pandas as pd
from bs4 import BeautifulSoup as Soup

COLUMN = ['ID', 'Name', 'Age', 'Position', 'Overall', 'Potential', 'Team']

FIFA_IDENTIFIERS = {
    # 'fifa_24': '240047',
    'fifa_23': '230054',
    'fifa_22': '220069',
    'fifa_21': '210064',
    'fifa_20': '200061'
}


def scrap_fifa():
    base_url = 'https://sofifa.com/players?r={fifa_identifier}&col=oa&sort=desc'
    for fifa_name in FIFA_IDENTIFIERS:
        done, fifa_df, ids_map, offset = init()
        extended_url = base_url.format(fifa_identifier=FIFA_IDENTIFIERS[fifa_name])
        extended_url += '&offset={offset}'
        while True:
            url = extended_url.format(offset=str(offset * 60))
            players_data = get_players_data(url)
            for player_data in players_data:
                player_info = player_data.findAll('td')
                id = player_info[0].find('img').get('id')

                if id in ids_map:
                    ids_map[id] += 1
                else:
                    ids_map[id] = 1

                if ids_map[id] > 10:
                    done = True
                    break

                if ids_map[id] > 2:
                    continue

                player_df = create_player_df(id, player_info)
                fifa_df = pd.concat([fifa_df, player_df], ignore_index=True)

            if done:
                break

            offset += 1
            time.sleep(0.2)

        fifa_df.to_csv(f'output/{fifa_name}_ratings.csv', index=False)


def init():
    fifa_df = pd.DataFrame(columns=COLUMN)
    ids_map = {}
    done = False
    offset = 0
    return done, fifa_df, ids_map, offset


def get_players_data(url):
    req = urllib.request.Request(url)
    req.add_header('User-Agent',
                   'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0')
    req.add_header('Accept',
                   'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8')
    req.add_header('Accept-Language', 'en-US,en;q=0.5')
    p_html = urllib.request.urlopen(req).read().decode('utf-8')
    data = Soup(p_html, 'html.parser')
    table = data.find('tbody')
    trs = table.findAll('tr')
    return trs


def create_player_df(id, player_info):
    name = player_info[1].contents[1].attrs['data-tippy-content']
    age = player_info[2].text
    try:
        position = player_info[1].findAll('a')[2].text
    except:
        position = player_info[1].findAll('a')[1].text
    overall = player_info[3].next_element.contents[0]
    potential = player_info[4].next_element.contents[0]
    team = player_info[5].contents[3].string.strip()
    player_df = pd.DataFrame([[id, name, age, position, overall, potential, team]])
    player_df.columns = COLUMN
    return player_df
