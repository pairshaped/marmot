-- UNION: combine results from two SELECTs, removing duplicates.
SELECT id, name FROM users WHERE active = 1
UNION
SELECT id, name FROM users WHERE created_at > @since
