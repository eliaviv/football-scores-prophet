import sqlite3
from sqlite3 import Error

import pandas as pd

from db.datamodels.match import Match
from db.datamodels.player import Player

DB_NAME = 'db/matches.db'


class SQLiteClient:
    def __init__(self):
        self.conn = self.create_connection()
        self.create_table_if_not_exists()

    def create_connection(self):
        """ create a database connection to the SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(DB_NAME)
            print(f"Connected to SQLite database '{DB_NAME}'")
        except Error as e:
            print(e)

        return conn

    def create_table_if_not_exists(self):
        create_matches_table_sql = """
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY,
            game_id TEXT NOT NULL,
            wk TEXT,
            day TEXT,
            date TEXT,
            time TEXT,
            home TEXT,
            xg_home REAL,
            g_home INTEGER,
            away TEXT,
            xg_away REAL,
            g_away INTEGER,
            league TEXT,
            season TEXT,
            score TEXT,
            match_link TEXT
        );
        """

        create_players_table_sql = """
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY,
            player TEXT,
            number INTEGER,
            nation TEXT,
            pos TEXT,
            age TEXT,
            minutes INTEGER,
            goals INTEGER,
            assists INTEGER,
            pk INTEGER,
            pk_att INTEGER,
            shots INTEGER,
            shots_on_target INTEGER,
            crd_y INTEGER,
            crd_r INTEGER,
            touches INTEGER,
            tackles INTEGER,
            interceptions INTEGER,
            blocks INTEGER,
            xg REAL,
            npxg REAL,
            xag REAL,
            sca INTEGER,
            gca INTEGER,
            cmp_x INTEGER,
            cmp_pct_x REAL,
            prgp INTEGER,
            carries INTEGER,
            prgc INTEGER,
            succ_dribbles INTEGER,
            match_id INTEGER,
            is_home INTEGER
        );
        """
        try:
            c = self.conn.cursor()
            c.execute(create_matches_table_sql)
            c.execute(create_players_table_sql)
        except Error as e:
            print(e)

    def persist_matches(self, matches):
        for match_row in matches.iterrows():
            self.persist_match(match_row[1])

    def persist_match(self, match_row):
        match = Match(
            match_row["Game ID"], match_row["Wk"], match_row["Day"], match_row["Date"], match_row["Time"],
            match_row["Home"],
            match_row["xG Home"], match_row["G Home"], match_row["Away"], match_row["xG Away"], match_row["G Away"],
            match_row["League"], match_row["Season"], match_row["Score"], match_row["Match Link"]
        )
        sql = '''INSERT INTO matches(game_id, wk, day, date, time, home, xg_home, g_home, away, xg_away, g_away, league, season, score, match_link)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
        cur = self.conn.cursor()
        cur.execute(sql, (
            match.game_id, match.wk, match.day, match.date, match.time, match.home, match.xg_home, match.g_home,
            match.away,
            match.xg_away, match.g_away, match.league, match.season, match.score, match.match_link))
        self.conn.commit()
        return cur.lastrowid

    def persist_players(self, players_df, match_id, is_home):
        for player_row in players_df.iterrows():
            self.persist_player(player_row[1], match_id, is_home)

    def persist_player(self, player_row, match_id, is_home):
        player = Player(
            player_row['Player'], player_row['#'], player_row['Nation'], player_row['Pos'], player_row['Age'],
            player_row['Min'], player_row['Gls'], player_row['Ast'], player_row['PK'], player_row['PKatt'],
            player_row['Sh'], player_row['SoT'], player_row['CrdY'], player_row['CrdR'], player_row['Touches'],
            player_row['Tkl'], player_row['Int'], player_row['Blocks'], player_row['xG'], player_row['npxG'],
            player_row['xAG'], player_row['SCA'], player_row['GCA'], player_row['Cmp_x'], player_row['Cmp%_x'],
            player_row['PrgP'], player_row['Carries'], player_row['PrgC'], player_row['Succ'], match_id, is_home
        )
        sql = '''INSERT INTO players(player, number, nation, pos, age, minutes, goals, assists, pk, pk_att, shots, shots_on_target, crd_y, crd_r, touches, tackles, interceptions, blocks, xg, npxg, xag, sca, gca, cmp_x, cmp_pct_x, prgp, carries, prgc, succ_dribbles, match_id, is_home)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
        cur = self.conn.cursor()
        cur.execute(sql, (
            player.player, player.number, player.nation, player.pos, player.age, player.minutes, player.goals,
            player.assists, player.pk, player.pk_att, player.shots,
            player.shots_on_target, player.crd_y, player.crd_r, player.touches, player.tackles, player.interceptions,
            player.blocks, player.xg, player.npxg, player.xag,
            player.sca, player.gca, player.cmp_x, player.cmp_pct_x, player.prgp, player.carries, player.prgc,
            player.succ_dribbles,
            player.match_id, player.is_home))
        self.conn.commit()
        return cur.lastrowid

    def find_players_by_match_id_and_is_home(self, match_id, is_home):
        sql = '''SELECT * FROM players WHERE match_id = ? AND is_home = ?'''
        players_df = pd.read_sql_query(sql, self.conn, params=(match_id, is_home))
        return self._rename_player_columns(players_df)

    def find_all_matches(self):
        sql = '''SELECT * FROM matches order by date, time'''
        matches_df = pd.read_sql_query(sql, self.conn)
        return self._rename_matche_columns(matches_df)

    def find_matches_by_date_time_home_away(self, date, time, home, away):
        sql = '''SELECT * FROM matches WHERE date = ? AND time = ? AND home = ? AND away = ?'''
        matches_df = pd.read_sql_query(sql, self.conn, params=(date, time, home, away))
        return self._rename_matche_columns(matches_df)

    def _rename_matche_columns(self, matches_df):
        return matches_df.rename(columns={'game_id': 'Game ID', 'wk': 'Wk', 'day': 'Day', 'date': 'Date',
                                          'time': 'Time', 'home': 'Home', 'xg_home': 'xG Home', 'g_home': 'G Home',
                                          'away': 'Away', 'xg_away': 'xG Away', 'g_away': 'G Away', 'league': 'League',
                                          'season': 'Season', 'score': 'Score', 'match_link': 'Match Link'})

    def _rename_player_columns(self, matches_df):
        return matches_df.rename(columns={'player': 'Player', 'number': 'Number', 'nation': 'Nation', 'pos': 'Position',
                                          'age': 'Age', 'minutes': 'Minutes Played', 'goals': 'Goals',
                                          'assists': 'Assists', 'pk': 'Penalty Kicks',
                                          'pk_att': 'Penalty Kicks Attempted', 'shots': 'Shots',
                                          'shots_on_target': 'Shots On Target', 'crd_y': 'Yellow Cards',
                                          'crd_r': 'Red Cards', 'touches': 'Ball Touches', 'tackles': 'Tackles',
                                          'interceptions': 'Interceptions', 'blocks': 'Blocks', 'xg': 'Expected Goals',
                                          'npxg': 'Non Penalty Expected Goals', 'xag': 'Expected Assists',
                                          'sca': 'Shot Creating Actions', 'gca': 'Goal Creating Actions',
                                          'cmp_x': 'Passes Completed', 'cmp_pct_x': 'Passes Completed Percentage',
                                          'prgp': 'Progressive Passes', 'carries': 'Carries',
                                          'prgc': 'Progressive Carries', 'succ_dribbles': 'Successful Dribbles',
                                          'match_id': 'Game ID', 'is_home': 'Is Home'})
