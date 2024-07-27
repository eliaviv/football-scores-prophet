import os


def load_fifa(season):
    year = season.split('-')[1]
    year_short_cut = year[2] + year[3]
    fifa_input_file = RESOURCES_PATH + f'/fifa_{year_short_cut}_ratings.csv'
    fifa_df = pd.read_csv(fifa_input_file)
    return fifa_df


def export_data():
    # export data
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    matches.reset_index(drop=True).to_csv(f'{OUTPUT_PATH}/{league.lower()}_{season.lower()}_matches_data.csv',
                                          header=True,
                                          index=False, mode='w')


def add_players_data(matches, match_links, fifa_df):
    statistics = [0, 0, 0]
    matches['Home Avg Players Score'] = ''
    matches['Away Avg Players Score'] = ''
    matches['Home Star Player Count'] = ''
    matches['Away Star Player Count'] = ''
    matches['Players Found %'] = ''
    for count, link in enumerate(match_links):
        tables = pd.read_html(link)
        for table in tables:
            try:
                table.columns = table.columns.droplevel()
            except:
                continue

        try:
            home_team_players_df = get_team_player_data([tables[3], tables[9]])
            away_team_players_df = get_team_player_data([tables[10], tables[16]])
        except Exception as e:
            continue

        home_team_avg_score = calculate_avg_score(home_team_players_df['Player'].tolist(), fifa_df, statistics)
        away_team_avg_score = calculate_avg_score(away_team_players_df['Player'].tolist(), fifa_df, statistics)
        home_star_player_count = count_star_players(home_team_players_df['Player'].tolist(), fifa_df)
        away_star_player_count = count_star_players(away_team_players_df['Player'].tolist(), fifa_df)

        matches.loc[count, 'Home Avg Players Score'] = home_team_avg_score
        matches.loc[count, 'Away Avg Players Score'] = away_team_avg_score
        matches.loc[count, 'Home Star Player Count'] = home_star_player_count
        matches.loc[count, 'Away Star Player Count'] = away_star_player_count

        matches.loc[count, 'Players Found %'] = round(((statistics[1] + statistics[2]) /
                                                       (statistics[0] + statistics[1] + statistics[2])), 2)
        statistics = [0, 0, 0]

        print(f'Done match number: {count + 1}')

        # sleep for 2 seconds after every game to avoid IP being blocked
        time.sleep(2)


def get_team_player_data(df):
    return reduce(lambda left, right:
                  pd.merge(left, right,
                           on=['Player', 'Nation', 'Age', 'Min'], how='outer'),
                  df).iloc[:-1]


def count_star_players(players, fifa_df):
    count = 0
    for player_name in players:
        player = find_player(player_name, fifa_df)
        if player['Overall'].empty:
            player_overall = 70
        elif player['Overall'].shape[0] > 1:
            player_overall = int(player['Overall'].values[0])
        else:
            player_overall = int(player['Overall'].values[0])
        if player_overall >= 85:
            count += 1

    return count


def calculate_avg_score(players, fifa_df, statistics):
    total_score = 0
    extra_weights = 0
    for player_name in players:
        player = find_player(player_name, fifa_df)
        if player['Overall'].empty:
            player_overall = 70
            statistics[0] += 1
        elif player['Overall'].shape[0] > 1:
            player_overall = int(player['Overall'].values[0])
            statistics[1] += 1
        else:
            player_overall = int(player['Overall'].values[0])
            statistics[2] += 1
        total_score += player_overall

        # TODO: Check this with Orel
        # # 1 weight is already counted
        # if player_overall >= 90:
        #     extra_weights += 9
        #     total_score += player_overall * 10
        # elif player_overall >= 85:
        #     extra_weights += 4
        #     total_score += player_overall * 5
        # else:
        #     total_score += player_overall

    return round(total_score / (len(players) + extra_weights), 2)


def find_player(player_name, fifa_df):
    return fifa_df[fifa_df['Name'].apply(lambda x: all(part.lower() in x.lower() for part in player_name.split()))]


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


def load_premier_league_data(start_year, end_year, base_path='./resources'):
    # Initialize DataFrame with the first year's data
    df = pd.read_csv(f'{base_path}/premier-league_{start_year}-{start_year + 1}_matches_data.csv')

    # Loop through the remaining years and concatenate the data
    for year in range(start_year + 1, end_year + 1):
        matches_file_name = f'premier-league_{year}-{year + 1}_matches_data.csv'
        matches = pd.read_csv(f'{base_path}/{matches_file_name}')
        df = pd.concat([df, matches], axis=0, ignore_index=True)

    return df


def agg_prev_games():
    df = load_premier_league_data(2019, 2023)

    # Function to calculate points from score
    def calculate_points(row):
        if row['G Home'] > row['G Away']:
            return 3, 0  # Home win
        elif row['G Home'] < row['G Away']:
            return 0, 3  # Away win
        else:
            return 1, 1  # Draw

    # Function to calculate expected score from xG
    def calculate_xscore(row):
        if row['xG Home'] > row['xG Away']:
            return 1  # Home win
        elif row['xG Home'] < row['xG Away']:
            return -1  # Away win
        else:
            return 0  # Draw

    # Calculate points and xScore for each match
    df[['Home Points', 'Away Points']] = df.apply(calculate_points, axis=1, result_type='expand')
    df['xScore'] = df.apply(calculate_xscore, axis=1)

    # Combine Home and Away points into a single dictionary
    team_stats_history = {team: {'points': [], 'goals_for': [], 'goals_against': []} for team in
                          pd.concat([df['Home'], df['Away']]).unique()}

    # Function to calculate average of last N matches
    def calculate_avg(lst, n=50):
        if len(lst) == 0:
            return 0
        else:
            return sum(lst[-n:]) / len(lst[-n:])

    # Initialize head-to-head statistics
    head_to_head_stats = {(home, away): {'points': [], 'goals_for': [], 'goals_against': []} for home in
                          df['Home'].unique() for away in df['Away'].unique()}

    # Lists to store the aggregated stats for each match
    home_avg_points = []
    away_avg_points = []
    home_avg_goals_for = []
    away_avg_goals_for = []
    home_avg_goals_against = []
    away_avg_goals_against = []
    home_matches_played = []
    away_matches_played = []
    home_points_per_match = []
    away_points_per_match = []

    # Form indicators
    home_form_points = []
    away_form_points = []
    home_form_goals_for = []
    away_form_goals_for = []
    home_form_goals_against = []
    away_form_goals_against = []

    # Head-to-head statistics
    home_head_to_head_points = []
    away_head_to_head_points = []
    home_head_to_head_goals_for = []
    away_head_to_head_goals_for = []
    home_head_to_head_goals_against = []
    away_head_to_head_goals_against = []

    # Iterate over each match and calculate aggregated stats
    for index, row in df.iterrows():
        home_team = row['Home']
        away_team = row['Away']

        # Calculate averages
        home_avg_points.append(calculate_avg(team_stats_history[home_team]['points']))
        away_avg_points.append(calculate_avg(team_stats_history[away_team]['points']))

        home_avg_goals_for.append(calculate_avg(team_stats_history[home_team]['goals_for']))
        away_avg_goals_for.append(calculate_avg(team_stats_history[away_team]['goals_for']))

        home_avg_goals_against.append(calculate_avg(team_stats_history[home_team]['goals_against']))
        away_avg_goals_against.append(calculate_avg(team_stats_history[away_team]['goals_against']))

        home_matches_played.append(len(team_stats_history[home_team]['points']))
        away_matches_played.append(len(team_stats_history[away_team]['points']))

        if len(team_stats_history[home_team]['points']) == 0:
            home_points_per_match.append(0)
        else:
            home_points_per_match.append(
                sum(team_stats_history[home_team]['points']) / len(team_stats_history[home_team]['points']))

        if len(team_stats_history[away_team]['points']) == 0:
            away_points_per_match.append(0)
        else:
            away_points_per_match.append(
                sum(team_stats_history[away_team]['points']) / len(team_stats_history[away_team]['points']))

        # Calculate form indicators (last 5 matches)
        home_form_points.append(calculate_avg(team_stats_history[home_team]['points'], 5))
        away_form_points.append(calculate_avg(team_stats_history[away_team]['points'], 5))

        home_form_goals_for.append(calculate_avg(team_stats_history[home_team]['goals_for'], 5))
        away_form_goals_for.append(calculate_avg(team_stats_history[away_team]['goals_for'], 5))

        home_form_goals_against.append(calculate_avg(team_stats_history[home_team]['goals_against'], 5))
        away_form_goals_against.append(calculate_avg(team_stats_history[away_team]['goals_against'], 5))

        # Calculate head-to-head statistics (last 5 matches)
        home_head_to_head_points.append(calculate_avg(head_to_head_stats[(home_team, away_team)]['points'], 5))
        away_head_to_head_points.append(calculate_avg(head_to_head_stats[(away_team, home_team)]['points'], 5))

        home_head_to_head_goals_for.append(calculate_avg(head_to_head_stats[(home_team, away_team)]['goals_for'], 5))
        away_head_to_head_goals_for.append(calculate_avg(head_to_head_stats[(away_team, home_team)]['goals_for'], 5))

        home_head_to_head_goals_against.append(
            calculate_avg(head_to_head_stats[(home_team, away_team)]['goals_against'], 5))
        away_head_to_head_goals_against.append(
            calculate_avg(head_to_head_stats[(away_team, home_team)]['goals_against'], 5))

        # Update stats history
        team_stats_history[home_team]['points'].append(row['Home Points'])
        team_stats_history[home_team]['goals_for'].append(row['G Home'])
        team_stats_history[home_team]['goals_against'].append(row['G Away'])

        team_stats_history[away_team]['points'].append(row['Away Points'])
        team_stats_history[away_team]['goals_for'].append(row['G Away'])
        team_stats_history[away_team]['goals_against'].append(row['G Home'])

        # Update head-to-head stats history
        head_to_head_stats[(home_team, away_team)]['points'].append(row['Home Points'])
        head_to_head_stats[(home_team, away_team)]['goals_for'].append(row['G Home'])
        head_to_head_stats[(home_team, away_team)]['goals_against'].append(row['G Away'])

        head_to_head_stats[(away_team, home_team)]['points'].append(row['Away Points'])
        head_to_head_stats[(away_team, home_team)]['goals_for'].append(row['G Away'])
        head_to_head_stats[(away_team, home_team)]['goals_against'].append(row['G Home'])

    # Add aggregated stats columns to the DataFrame
    df['Home Avg Points'] = home_avg_points
    df['Away Avg Points'] = away_avg_points
    df['Home Avg Goals For'] = home_avg_goals_for
    df['Away Avg Goals For'] = away_avg_goals_for
    df['Home Avg Goals Against'] = home_avg_goals_against
    df['Away Avg Goals Against'] = away_avg_goals_against
    df['Home Matches Played'] = home_matches_played
    df['Away Matches Played'] = away_matches_played
    df['Home Points/Match'] = home_points_per_match
    df['Away Points/Match'] = away_points_per_match
    df['Home Form Points'] = home_form_points
    df['Away Form Points'] = away_form_points
    df['Home Form Goals For'] = home_form_goals_for
    df['Away Form Goals For'] = away_form_goals_for
    df['Home Form Goals Against'] = home_form_goals_against
    df['Away Form Goals Against'] = away_form_goals_against
    df['Home Head-to-Head Points'] = home_head_to_head_points
    df['Away Head-to-Head Points'] = away_head_to_head_points
    df['Home Head-to-Head Goals For'] = home_head_to_head_goals_for
    df['Away Head-to-Head Goals For'] = away_head_to_head_goals_for
    df['Home Head-to-Head Goals Against'] = home_head_to_head_goals_against
    df['Away Head-to-Head Goals Against'] = away_head_to_head_goals_against

    df = df.round(2)

    # Save the updated DataFrame to a new CSV file
    output_file_name = f'./resources/premier-league_matches_merged.csv'

    # Check if the file exists and remove it if it does
    if os.path.exists(output_file_name):
        os.remove(output_file_name)

    df.to_csv(output_file_name, index=False)
    print('merged and added aggregate')