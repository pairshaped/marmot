-- Anonymous ? parameters: each ? becomes a positional function argument.
-- The parameter type is inferred from the surrounding SQL context
-- (column comparison, LIMIT/OFFSET clause, etc.).
-- Generated function: find_by_name(db:, name: String) -> Result(...)
SELECT
    id,
    name,
    email
FROM users
WHERE name = ?
