import datetime
from random import choices, randint

import pandas as pd
import psycopg2

username = 'arseniy'
password = 'pink_floyd'
database = 'db_lab3_shaparenko'
host = 'localhost'
port = '5432'

clear_query = '''
DELETE FROM games;
DELETE FROM players;
DELETE FROM openings;
'''


def create_queries():
    df = pd.read_csv('games_data.csv')

    df['game_date'] = pd.to_datetime(df['created_at'] / 1000, unit='s', origin='unix')
    df = df.drop(['rated', 'last_move_at', 'created_at', 'moves'], axis=1)
    df = df.drop_duplicates(subset='id', keep='first')

    players = pd.DataFrame(columns=['player_id', 'rating', 'reg_date', 'sex'])
    openings = pd.DataFrame(columns=['eco', 'opening_name', 'opening_ply'])
    games = pd.DataFrame(columns=['game_id', 'game_date', 'time_control', 'num_turns',
                                  'victory_status', 'winner', 'white_player', 'black_player', 'eco'])

    players['player_id'] = pd.concat([df['white_id'].set_axis(range(0, 2 * len(df), 2)),
                                      df['black_id'].set_axis(range(1, 2 * len(df), 2))],
                                     ignore_index=False)
    players['rating'] = pd.concat([df['white_rating'].set_axis(range(0, 2 * len(df), 2)),
                                   df['black_rating'].set_axis(range(1, 2 * len(df), 2))],
                                  ignore_index=False)
    players = players.sort_index()
    players = players.drop_duplicates(subset='player_id', keep='first')
    players.index = [i for i in range(len(players))]

    start_date = datetime.date(1990, 1, 1)
    random_dates = [start_date + datetime.timedelta(randint(0, 365 * 15)) for _ in range(len(players))]
    players['reg_date'] = pd.Series(random_dates)

    random_sex = [choices(['M', 'F', None], weights=[0.6, 0.2, 0.2])[0] for _ in range(len(players))]
    players['sex'] = pd.Series(random_sex)

    openings['eco'] = df['opening_eco']
    openings['opening_name'] = df['opening_name']
    openings['opening_ply'] = df['opening_ply']
    openings = openings.drop_duplicates(subset='eco', keep='first')

    games['game_id'] = df['id']
    games['game_date'] = df['game_date']
    games['time_control'] = df['increment_code']
    games['num_turns'] = df['turns']
    games['victory_status'] = df['victory_status']
    games['winner'] = df['winner']
    games['white_player'] = df['white_id']
    games['black_player'] = df['black_id']
    games['eco'] = df['opening_eco']

    command_1 = ''
    for i, row in enumerate(players.itertuples()):
        row = list(row)
        row[3] = str(row[3])
        row[4] = 'NULL' if row[4] is None else row[4]
        row = tuple(row[1:])
        command_1 += f"INSERT INTO players ({', '.join(players.columns)}) VALUES {row};\n"

        if i == 930:
            break
    command_1 = command_1.replace("'NULL'", "NULL")

    command_2 = ''
    for i, row in enumerate(openings.itertuples()):
        row = tuple(row[1:])
        opening_name = row[1].replace("'", "''")
        command_2 += f"INSERT INTO openings ({', '.join(openings.columns)}) VALUES ('{row[0]}', '{opening_name}', {row[2]});\n"

        if i == 180:
            break

    command_3 = ''
    for i, row in enumerate(games.itertuples()):
        row = list(row)
        row[2] = str(row[2])
        row = tuple(row[1:])
        command_3 += f"INSERT INTO games ({', '.join(games.columns)}) VALUES {row};\n"

        if i == 1000:
            break

    return command_1, command_2, command_3


def main():
    connection = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

    with connection.cursor() as cursor:
        cursor.execute(clear_query)
        for query in create_queries():
            cursor.execute(query)

    connection.commit()


if __name__ == '__main__':
    main()
