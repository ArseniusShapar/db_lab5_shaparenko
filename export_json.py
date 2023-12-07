import json

import psycopg2

username = 'arseniy'
password = 'pink_floyd'
database = 'db_lab3_shaparenko'
host = 'localhost'
port = '5432'


def main():
    connection = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

    data = {}
    with connection:
        cursor = connection.cursor()

        for table in ('games', 'players', 'openings'):
            cursor.execute(f'SELECT * FROM {table}')
            fields = [x[0] for x in cursor.description]
            rows = [dict(zip(fields, row)) for row in cursor]
            data[table] = rows

        with open('data.json', 'w') as file:
            json.dump(data, file, default=str)


if __name__ == '__main__':
    main()
