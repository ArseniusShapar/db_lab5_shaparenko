from itertools import pairwise

import matplotlib.pyplot as plt
import psycopg2

username = 'arseniy'
password = 'pink_floyd'
database = 'db_lab3_shaparenko'
host = 'localhost'
port = '5432'

query_1 = '''SELECT eco, opening_name, games_count FROM OpeningsGames;'''

query_2 = '''
SELECT (SELECT COUNT(*) FROM GamesInformation WHERE winner = 'white') AS white_wins, 
	   (SELECT COUNT(*) FROM GamesInformation WHERE winner = 'black') AS black_wins,
	   (SELECT COUNT(*) FROM GamesInformation WHERE winner = 'draw')  AS draws;'''

query_3 = '''SELECT rating, SUM(wins) FROM PlayersWins GROUP BY rating ORDER BY rating;'''

create_view_1 = '''
CREATE OR REPLACE VIEW OpeningsGames AS 
	SELECT eco, opening_name, opening_ply, COUNT(game_id) AS games_count 
	FROM games RIGHT JOIN openings USING(eco) GROUP BY eco;
'''

create_view_2 = '''
CREATE OR REPLACE VIEW GamesInformation AS 
    SELECT game_id, game_date, time_control, num_turns, victory_status, winner FROM games;
'''

create_view_3 = '''
CREATE OR REPLACE VIEW PlayersWins AS 
	SELECT player_id, 
		   rating, 
		   reg_date, 
		   sex, 
		   (SELECT COUNT(*) FROM games WHERE (white_player = player_id AND winner = 'white') OR (black_player = player_id AND winner = 'black')) AS wins
	FROM players;
'''


def main():
    connection = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

    with connection.cursor() as cursor:
        # Creating views
        cursor.execute(create_view_1)
        cursor.execute(create_view_2)
        cursor.execute(create_view_3)

        # Query 1
        cursor.execute(query_1)
        response = cursor.fetchall()
        response.sort(key=lambda x: x[2], reverse=True)
        eco, opening_name, games_count = [row[0] for row in response], [row[1] for row in response], [row[2] for row in
                                                                                                      response]

        # Query 2
        cursor.execute(query_2)
        response = cursor.fetchall()
        white_wins, black_wins, draws = [row[0] for row in response], [row[1] for row in response], [row[2] for row in
                                                                                                     response]

        # Query 3
        cursor.execute(query_3)
        response = cursor.fetchall()
        rating, wins = [row[0] for row in response], [row[1] for row in response]
        grouped_rating = [i for i in range(750, 2851, 100)]
        grouped_wins = [sum([w for r, w in zip(rating, wins) if r1 <= r <= r2]) for r1, r2 in pairwise(grouped_rating)]

        figure, (bar_ax, pie_ax, graph_ax) = plt.subplots(1, 3, dpi=100)

        bar_ax.bar(eco[:10], games_count[:10], label='Total')
        bar_ax.set_title('Кількість партій, в яких був розіграний кожен дебют')
        bar_ax.set_xlabel('Дебюти')
        bar_ax.set_ylabel('Кількість партій')

        pie_ax.pie(white_wins + black_wins + draws,
                   labels=['білі', 'чорні', 'нічия'],
                   autopct='%1.1f%%')
        pie_ax.set_title('Кількість перемог кожної сторони(або нічиїх)')

        graph_ax.plot(grouped_rating[:-1], grouped_wins, marker='o')
        graph_ax.set_xlabel('Рейтинг')
        graph_ax.set_ylabel('Кількість перемог')
        graph_ax.set_xticks(range(500, 2501, 500))
        graph_ax.set_title('Графік залежності кількості перемог від рейтингу')
        graph_ax.plot(grouped_rating[:-1], grouped_wins, color='C0')

        mng = plt.get_current_fig_manager()
        mng.resize(1400, 600)
        plt.show()

    connection.commit()


if __name__ == '__main__':
    main()
