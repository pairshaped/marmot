-- WHERE with IN (literal list).
-- The parameter type is inferred from the column being compared.
SELECT id, name, email
FROM users
WHERE id IN (@user_ids)

-- Usage in generated Gleam:
-- list_users_in(db:, user_ids: List(Int)) -> Result(...)
