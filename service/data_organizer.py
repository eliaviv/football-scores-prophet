import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from sklearn.preprocessing import MinMaxScaler

import pandas as pd

from db.sqlite_client import SQLiteClient
from service.spi_matcher import add_fivethirtyeight_spi_data

RESOURCES_PATH = 'resources'
OUTPUT_PATH = 'output'


def prepare_matches_for_modeling(db_client):
    matches_df = load_matches_df(db_client)

    add_bets(matches_df)
    add_elo_xscore(matches_df)
    add_players_data(matches_df)
    add_fivethirtyeight_spi_data(matches_df)
    add_aggregated_data(matches_df)

    matches_df = matches_df.round(2)

    export_data(matches_df, f'{OUTPUT_PATH}/matches_for_modeling.csv')

    print('All data was added to matches.df')


def load_matches_df(db_client):
    return db_client.find_all_matches()


def add_bets(matches_df):
    print('Adding bets')

    all_bets_dict = load_bets()

    for bets_key in all_bets_dict:
        season = '-'.join(bets_key.split('-')[-2:])
        league = '-'.join(bets_key.split('-')[:-2])
        bets_df = all_bets_dict[bets_key]

        condition = (matches_df['League'] == league) & (matches_df['Season'] == season)
        filtered_df = matches_df[condition]
        if filtered_df.shape[0] == bets_df.shape[0]:
            matches_df.loc[condition, 'B365H'] = bets_df['B365H'].tolist()
            matches_df.loc[condition, 'B365D'] = bets_df['B365D'].tolist()
            matches_df.loc[condition, 'B365A'] = bets_df['B365A'].tolist()

    matches_df.dropna()

    def calculate_xscore(row):
        if row['B365H'] < row['B365D'] and row['B365H'] < row['B365A']:
            return 1
        elif row['B365A'] < row['B365D'] and row['B365A'] < row['B365H']:
            return -1
        else:
            return 0

    matches_df['xScore'] = matches_df.apply(calculate_xscore, axis=1)


def add_elo_xscore(matches_df):
    print('Adding elo score')

    def calculate_elo_xscore(row):
        home_win_threshold = 0.52
        away_win_threshold = 0.48
        home_elo, away_elo = row['home_elo'], row['away_elo']

        # Calculate the probability of a home win using the Elo formula
        probability_home_win = 1 / (1 + 10 ** ((away_elo - home_elo) / 400))

        # Determine the expected score based on the probability
        if probability_home_win > home_win_threshold:
            return 1  # Home win
        elif probability_home_win < away_win_threshold:
            return -1  # Away win
        else:
            return 0  # Draw

    matches_df['xScoreElo'] = matches_df.apply(calculate_elo_xscore, axis=1)


def add_players_data(matches_df):
    print('Adding players data')

    all_fifa_dict = load_all_fifa()

    init_players_data_columns(matches_df)

    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(process_match_row, i, row, all_fifa_dict) for i, row in
                   matches_df.iterrows()]

        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
                print(f'Done Adding players data for match {result["index"] + 1}')

    for result in results:
        matches_df.loc[result['index'], 'Home Avg Players Score'] = result['Home Avg Players Score']
        matches_df.loc[result['index'], 'Away Avg Players Score'] = result['Away Avg Players Score']
        matches_df.loc[result['index'], 'Home Star Player Count'] = result['Home Star Player Count']
        matches_df.loc[result['index'], 'Away Star Player Count'] = result['Away Star Player Count']
        matches_df.loc[result['index'], 'Players Found %'] = result['Players Found %']

    scaler = MinMaxScaler()
    columns_to_normalize = ['Home Avg Players Score', 'Away Avg Players Score']
    matches_df[columns_to_normalize] = scaler.fit_transform(matches_df[columns_to_normalize])
    matches_df[columns_to_normalize] *= 100

    def calculate_xpower(row):
        if row['Home Avg Players Score'] - row['Away Avg Players Score'] > 10:
            return 1
        elif row['Away Avg Players Score'] - row['Home Avg Players Score'] > 10:
            return -1
        else:
            return 0

    matches_df['xPower'] = matches_df.apply(calculate_xpower, axis=1)

    def calculate_xsuperpower(row):
        if row['Home Star Player Count'] > row['Away Star Player Count']:
            return 1
        elif row['Away Star Player Count'] > row['Home Star Player Count']:
            return -1
        else:
            return 0

    matches_df['xSuperPower'] = matches_df.apply(calculate_xsuperpower, axis=1)


def process_match_row(i, match_row, all_fifa_dict):
    db_client = SQLiteClient()
    statistics = [0, 0, 0]
    season = match_row['Season']
    game_id = match_row['Game ID']

    fifa_df = get_relevant_fifa(all_fifa_dict, season)

    home_team_players_df = load_team_player_data(db_client, game_id, 1)
    away_team_players_df = load_team_player_data(db_client, game_id, 0)

    home_team_avg_score = calculate_avg_score(home_team_players_df['Player'].tolist(), fifa_df, season, statistics)
    away_team_avg_score = calculate_avg_score(away_team_players_df['Player'].tolist(), fifa_df, season, statistics)
    home_star_player_count = count_star_players(home_team_players_df['Player'].tolist(), fifa_df, season)
    away_star_player_count = count_star_players(away_team_players_df['Player'].tolist(), fifa_df, season)

    result = {
        'index': i,
        'Home Avg Players Score': home_team_avg_score,
        'Away Avg Players Score': away_team_avg_score,
        'Home Star Player Count': home_star_player_count,
        'Away Star Player Count': away_star_player_count,
        'Players Found %': round(((statistics[1] + statistics[2]) / (statistics[0] + statistics[1] + statistics[2])), 2)
    }

    return result


def load_all_fifa():
    all_fifa_dict = {}
    fifa_folder = RESOURCES_PATH + '/fifa'
    for filename in os.listdir(fifa_folder):
        fifa_input_file = fifa_folder + '/' + filename
        all_fifa_dict['_'.join(filename.split('_')[:2])] = pd.read_csv(fifa_input_file)

    return all_fifa_dict


def init_players_data_columns(matches_df):
    matches_df['Home Avg Players Score'] = ''
    matches_df['Away Avg Players Score'] = ''
    matches_df['Home Star Player Count'] = ''
    matches_df['Away Star Player Count'] = ''
    matches_df['Players Found %'] = ''


def get_relevant_fifa(all_fifa_dict, season):
    year = season.split('-')[0]
    year_shortcut = year[2] + year[3]
    return all_fifa_dict[f'fifa_{year_shortcut}']


def load_team_player_data(db_client, match_id, is_home):
    return db_client.find_players_by_match_id_and_is_home(match_id, is_home)


def count_star_players(players, fifa_df, season):
    count = 0
    for player_name in players:
        player = find_player(player_name, fifa_df, season)
        if player['Overall'].empty:
            player_overall = 70
        elif player['Overall'].shape[0] > 1:
            player_overall = int(player['Overall'].values[0])
        else:
            player_overall = int(player['Overall'].values[0])
        if player_overall >= 85:
            count += 1

    return count


def calculate_avg_score(players, fifa_df, season, statistics):
    total_score = 0
    extra_weights = 0
    for player_name in players:
        player = find_player(player_name, fifa_df, season)
        if player['Overall'].empty:
            player_overall = 70
            statistics[0] += 1
        elif player['Overall'].shape[0] > 1:
            player_overall = int(player['Overall'].values[0])
            statistics[1] += 1
        else:
            player_overall = int(player['Overall'].values[0])
            statistics[2] += 1

        # 1 weight is already counted
        if player_overall >= 90:
            extra_weights += 9
            total_score += player_overall * 10
        elif player_overall >= 85:
            extra_weights += 4
            total_score += player_overall * 5
        else:
            total_score += player_overall

    return round(total_score / (len(players) + extra_weights), 2)


player_cache = {}


def find_player(player_name, fifa_df, season):
    cache_key = (player_name, season)
    if cache_key in player_cache:
        return player_cache[cache_key]

    # Perform the lookup
    found_player = fifa_df[
        fifa_df['Name'].apply(lambda x: all(part.lower() in x.lower() for part in player_name.split()))
    ]

    # Cache the result
    player_cache[cache_key] = found_player
    return found_player


# Function to calculate expected score from Bets


def match_prev_league():
    years = [2019, 2020, 2021, 2022, 2023]
    for year in years:
        # every match will be matched with the previous league's stats
        matches_file_name = f'premier-league_{year}-{year + 1}_matches_data.csv'
        teams_file_name = f'premier-league_{year - 1}-{year}_teams_data.csv'
        matches = pd.read_csv(f'./resources/{matches_file_name}')
        teams = pd.read_csv(f'./resources/{teams_file_name}')

        # Specify the columns you want to keep from the teams data
        desired_columns = ['Squad', 'Rk', 'MP', 'W', 'D', 'L', 'GF', 'GA', 'Pts', 'Pts/MP']

        # Select and rename the desired columns for home and away teams
        teams_home = teams[desired_columns].copy()
        teams_home.columns = [f'Prev_{col}_Home' if col != 'Squad' else col for col in teams_home.columns]

        teams_away = teams[desired_columns].copy()
        teams_away.columns = [f'Prev_{col}_Away' if col != 'Squad' else col for col in teams_away.columns]

        # Merge the Home team stats with matches
        matches = matches.merge(teams_home, left_on='Home', right_on='Squad', how='left')

        # Merge the Away team stats with matches
        matches = matches.merge(teams_away, left_on='Away', right_on='Squad', how='left')

        # Drop the duplicated 'Squad' columns
        matches = matches.drop(columns=['Squad_x', 'Squad_y'])

        # Save the updated DataFrame to a new CSV file
        output_file_name = f'./resources/premier-league_{year}-{year + 1}_matches_with_stats.csv'

        # Check if the file exists and remove it if it does
        if os.path.exists(output_file_name):
            os.remove(output_file_name)

        matches.to_csv(output_file_name, index=False)

        print(f'Merged stats for year {year}-{year + 1} and saved to {output_file_name}')


# Function to calculate expected score from Elo score


def load_premier_league_data(start_year, end_year, base_path='./resources'):
    # Initialize DataFrame with the first year's data
    df = pd.read_csv(f'{base_path}/premier-league_{start_year}-{start_year + 1}_matches_data.csv')

    # Loop through the remaining years and concatenate the data
    for year in range(start_year + 1, end_year + 1):
        matches_file_name = f'premier-league_{year}-{year + 1}_matches_data.csv'
        matches = pd.read_csv(f'{base_path}/{matches_file_name}')
        df = pd.concat([df, matches], axis=0, ignore_index=True)

    return df


def load_bets():
    all_bets_dict = {}
    bets_folder = RESOURCES_PATH + '/bets'
    for filename in os.listdir(bets_folder):
        bets_input_file = bets_folder + '/' + filename
        all_bets_dict[filename.split('.')[0]] = pd.read_csv(bets_input_file)

    return all_bets_dict


def add_aggregated_data(matches_df):
    print('Adding aggregated data')

    def calculate_points(row):
        if row['G Home'] > row['G Away']:
            return 3, 0  # Home win
        elif row['G Home'] < row['G Away']:
            return 0, 3  # Away win
        else:
            return 1, 1  # Draw

    matches_df[['Home Points', 'Away Points']] = matches_df.apply(calculate_points, axis=1, result_type='expand')
    matches_df['xG Home Diff'] = matches_df['G Home'] - matches_df['xG Home']
    matches_df['xG Away Diff'] = matches_df['G Away'] - matches_df['xG Away']

    def calculate_avg(lst, n=50):
        if len(lst) == 0:
            return 0
        else:
            return sum(lst[-n:]) / len(lst[-n:])

    team_stats_history = {
        team: {'points': [], 'goals_for': [], 'goals_against': [], 'xG_diff': []}
        for team in pd.concat([matches_df['Home'], matches_df['Away']]).unique()
    }

    head_to_head_stats = {
        (home, away): {'points': [], 'goals_for': [], 'goals_against': []}
        for home in matches_df['Home'].unique() for away in matches_df['Away'].unique()
    }

    initialize_aggregated_columns(matches_df)

    for index, row in matches_df.iterrows():
        home_team = row['Home']
        away_team = row['Away']

        matches_df.iloc[index, matches_df.columns.get_loc('Home Avg Points')] = calculate_avg(team_stats_history[home_team]['points'])
        matches_df.iloc[index, matches_df.columns.get_loc('Away Avg Points')] = calculate_avg(team_stats_history[away_team]['points'])
        matches_df.iloc[index, matches_df.columns.get_loc('Home Avg Goals For')] = calculate_avg(team_stats_history[home_team]['goals_for'])
        matches_df.iloc[index, matches_df.columns.get_loc('Away Avg Goals For')] = calculate_avg(team_stats_history[away_team]['goals_for'])
        matches_df.iloc[index, matches_df.columns.get_loc('Home Avg Goals Against')] = calculate_avg(team_stats_history[home_team]['goals_against'])
        matches_df.iloc[index, matches_df.columns.get_loc('Away Avg Goals Against')] = calculate_avg(team_stats_history[away_team]['goals_against'])
        matches_df.iloc[index, matches_df.columns.get_loc('Home Matches Played')] = len(team_stats_history[home_team]['points'])
        matches_df.iloc[index, matches_df.columns.get_loc('Away Matches Played')] = len(team_stats_history[away_team]['points'])
        matches_df.iloc[index, matches_df.columns.get_loc('xG Home Avg Diff')] = calculate_avg(team_stats_history[home_team]['xG_diff'])
        matches_df.iloc[index, matches_df.columns.get_loc('xG Away Avg Diff')] = calculate_avg(team_stats_history[away_team]['xG_diff'])

        if len(team_stats_history[home_team]['points']) == 0:
            matches_df.iloc[index, matches_df.columns.get_loc('Home Points/Match')] = 0
        else:
            matches_df.iloc[index, matches_df.columns.get_loc('Home Points/Match')] = sum(team_stats_history[home_team]['points']) / len(team_stats_history[home_team]['points'])

        if len(team_stats_history[away_team]['points']) == 0:
            matches_df.iloc[index, matches_df.columns.get_loc('Away Points/Match')] = 0
        else:
            matches_df.iloc[index, matches_df.columns.get_loc('Away Points/Match')] = sum(team_stats_history[away_team]['points']) / len(team_stats_history[away_team]['points'])

        # Calculate form indicators (last 5 matches)
        matches_df.iloc[index, matches_df.columns.get_loc('Home Form Points')] = calculate_avg(team_stats_history[home_team]['points'], 5)
        matches_df.iloc[index, matches_df.columns.get_loc('Away Form Points')] = calculate_avg(team_stats_history[away_team]['points'], 5)
        matches_df.iloc[index, matches_df.columns.get_loc('Home Form Goals For')] = calculate_avg(team_stats_history[home_team]['goals_for'], 5)
        matches_df.iloc[index, matches_df.columns.get_loc('Away Form Goals For')] = calculate_avg(team_stats_history[away_team]['goals_for'], 5)
        matches_df.iloc[index, matches_df.columns.get_loc('Home Form Goals Against')] = calculate_avg(team_stats_history[home_team]['goals_against'], 5)
        matches_df.iloc[index, matches_df.columns.get_loc('Away Form Goals Against')] = calculate_avg(team_stats_history[away_team]['goals_against'], 5)
        matches_df.iloc[index, matches_df.columns.get_loc('xG Home Form Diff')] = calculate_avg(team_stats_history[home_team]['xG_diff'], 5)
        matches_df.iloc[index, matches_df.columns.get_loc('xG Away Form Diff')] = calculate_avg(team_stats_history[away_team]['xG_diff'], 5)

        # Calculate head-to-head statistics (last 5 matches)
        matches_df.iloc[index, matches_df.columns.get_loc('Home Head-to-Head Points')] = calculate_avg(head_to_head_stats[(home_team, away_team)]['points'], 5)
        matches_df.iloc[index, matches_df.columns.get_loc('Away Head-to-Head Points')] = calculate_avg(head_to_head_stats[(away_team, home_team)]['points'], 5)
        matches_df.iloc[index, matches_df.columns.get_loc('Home Head-to-Head Goals For')] = calculate_avg(head_to_head_stats[(home_team, away_team)]['goals_for'], 5)
        matches_df.iloc[index, matches_df.columns.get_loc('Away Head-to-Head Goals For')] = calculate_avg(head_to_head_stats[(away_team, home_team)]['goals_for'], 5)
        matches_df.iloc[index, matches_df.columns.get_loc('Home Head-to-Head Goals Against')] = calculate_avg(head_to_head_stats[(home_team, away_team)]['goals_against'], 5)
        matches_df.iloc[index, matches_df.columns.get_loc('Away Head-to-Head Goals Against')] = calculate_avg(head_to_head_stats[(away_team, home_team)]['goals_against'], 5)

        # Update stats history
        team_stats_history[home_team]['points'].append(row['Home Points'])
        team_stats_history[home_team]['goals_for'].append(row['G Home'])
        team_stats_history[home_team]['goals_against'].append(row['G Away'])
        team_stats_history[home_team]['xG_diff'].append(row['xG Home Diff'])
        team_stats_history[away_team]['points'].append(row['Away Points'])
        team_stats_history[away_team]['goals_for'].append(row['G Away'])
        team_stats_history[away_team]['goals_against'].append(row['G Home'])
        team_stats_history[away_team]['xG_diff'].append(row['xG Away Diff'])

        # Update head-to-head stats history
        head_to_head_stats[(home_team, away_team)]['points'].append(row['Home Points'])
        head_to_head_stats[(home_team, away_team)]['goals_for'].append(row['G Home'])
        head_to_head_stats[(home_team, away_team)]['goals_against'].append(row['G Away'])
        head_to_head_stats[(away_team, home_team)]['points'].append(row['Away Points'])
        head_to_head_stats[(away_team, home_team)]['goals_for'].append(row['G Away'])
        head_to_head_stats[(away_team, home_team)]['goals_against'].append(row['G Home'])


def initialize_aggregated_columns(matches_df):
    matches_df['Home Avg Points'] = 0
    matches_df['Away Avg Points'] = 0
    matches_df['Home Avg Goals For'] = 0
    matches_df['Away Avg Goals For'] = 0
    matches_df['Home Avg Goals Against'] = 0
    matches_df['Away Avg Goals Against'] = 0
    matches_df['Home Matches Played'] = 0
    matches_df['Away Matches Played'] = 0
    matches_df['Home Points/Match'] = 0
    matches_df['Away Points/Match'] = 0
    matches_df['Home Form Points'] = 0
    matches_df['Away Form Points'] = 0
    matches_df['Home Form Goals For'] = 0
    matches_df['Away Form Goals For'] = 0
    matches_df['Home Form Goals Against'] = 0
    matches_df['Away Form Goals Against'] = 0
    matches_df['Home Head-to-Head Points'] = 0
    matches_df['Away Head-to-Head Points'] = 0
    matches_df['Home Head-to-Head Goals For'] = 0
    matches_df['Away Head-to-Head Goals For'] = 0
    matches_df['Home Head-to-Head Goals Against'] = 0
    matches_df['Away Head-to-Head Goals Against'] = 0
    matches_df['xG Home Avg Diff'] = 0
    matches_df['xG Home Form Diff'] = 0
    matches_df['xG Away Avg Diff'] = 0
    matches_df['xG Away Form Diff'] = 0


def export_data(matches_df, path):
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    matches_df.reset_index(drop=True).to_csv(path, header=True, index=False, mode='w')
