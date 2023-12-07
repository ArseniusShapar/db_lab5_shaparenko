DO $$
DECLARE
	player_id players.player_id%TYPE;
BEGIN
	player_id := 'player_';
	FOR i in 1..20
		LOOP 
			INSERT INTO players (player_id, rating, reg_date, sex)
			VALUES (player_id || i, i * 100, CURRENT_DATE - i + 1, CASE WHEN i % 2 = 1 THEN 'M' ELSE 'F' END);
		END LOOP;
END;
$$