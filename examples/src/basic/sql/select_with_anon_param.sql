-- Select with an anonymous ? parameter.
-- Generated function: select_with_anon_param(db:, name: String) -> Result(...)
SELECT id, name, email
FROM users
WHERE name = ?
