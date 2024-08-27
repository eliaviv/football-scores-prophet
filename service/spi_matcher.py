import pandas as pd
from fuzzywuzzy import process


special_matches = {
    'Saint-Étienne': 'St Etienne',
    'Köln': "FC Cologne",
    'Hertha BSC': 'Hertha Berlin'
}

def match_team_name(team_name, candidates):
    if team_name in special_matches:
        return special_matches[team_name]
    match, score = process.extractOne(team_name, candidates)
    if score > 80:  # Adjust the threshold as needed
        return match
    return None


def __extend_df_with_spi(df1, df2):
    """
    Extends df1 with extra features from df2 by matching on date and fuzzy matching team names.

    Parameters:
        df1 (pd.DataFrame): The first DataFrame containing columns 'date', 'Home', 'Away'.
        df2 (pd.DataFrame): The second DataFrame containing columns 'date', 'team1', 'team2',
                            and extra columns to be added to df1.

    Returns:
        pd.DataFrame: The extended df1 with extra features from df2.
    """
    # Initialize columns to be added to df1
    extra_columns = ['importance1', 'importance2', 'proj_score1', 'proj_score2',
                     'spi1', 'spi2', 'prob1', 'prob2', 'probtie']

    # Add the new columns to df1 with NaN values
    for col in extra_columns:
        df1[col] = pd.NA

    # Get unique team names from df2
    df2_teams = list(df2['team1'].unique()) + list(df2['team2'].unique())

    # Iterate over df1 and match each game with a corresponding game in df2
    for idx, row in df1.iterrows():
        home_team = match_team_name(row['Home'], df2_teams)
        away_team = match_team_name(row['Away'], df2_teams)
        match_date = row['Date']

        # Perform the equivalent of SQL query:
        # SELECT * FROM df2 WHERE date = match_date AND team1 = home_team AND team2 = away_team
        match = df2.query("date == @match_date and (team1 == @home_team or team2 == @away_team)")

        print(f"{match_date} {row['Home']} vs {row['Away']}")
        if len(match) > 1:
            print("too many matches")
        if not match.empty:
            # Update df1 with values from df2 for the matched row
            print("matched")
            for col in extra_columns:
                df1.at[idx, col] = match[col].values[0]
        else:
            print("no match found")

    return df1

def add_fivethirtyeight_spi_data(matches_df):
    df2 = pd.read_csv(f'./resources/soccer-spi/spi_matches.csv')

    __extend_df_with_spi(matches_df, df2)
