import psycopg2

username = 'arseniy'
password = 'pink_floyd'
database = 'db_lab3_shaparenko'
host = 'localhost'
port = '5432'


def main():
    connection = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

    with connection.cursor() as cursor:
        for table in ('games', 'players', 'openings'):
            query = f'COPY (SELECT * FROM {table}) TO STDOUT WITH CSV HEADER'
            with open(f'{table}.csv', 'w') as file:
                cursor.copy_expert(query, file)


if __name__ == '__main__':
    main()
