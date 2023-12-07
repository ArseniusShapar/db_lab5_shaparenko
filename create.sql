CREATE TABLE players
(
	player_id VARCHAR(150) NOT NULL,
	rating INT NOT NULL,
	reg_date DATE NOT NULL,
	sex CHAR(1),
	PRIMARY KEY (player_id)
);

CREATE TABLE openings
(
	eco VARCHAR(10) NOT NULL,
	opening_name VARCHAR(150) NOT NULL,
	opening_ply INT NOT NULL,
	PRIMARY KEY (eco)
);

CREATE TABLE games
(
	game_id CHAR(8) NOT NULL,
	game_date DATE NOT NULL,
	time_control VARCHAR(5) NOT NULL,
	num_turns INT NOT NULL,
	victory_status VARCHAR(10) NOT NULL,
	winner CHAR(5) NOT NULL,
	white_player VARCHAR(150) NOT NULL,
	black_player VARCHAR(150) NOT NULL,
	eco VARCHAR(10) NOT NULL,
	PRIMARY KEY (game_id),
	FOREIGN KEY (white_player) REFERENCES players(player_id),
	FOREIGN KEY (black_player) REFERENCES players(player_id),
	FOREIGN KEY (eco) REFERENCES openings(eco)
);