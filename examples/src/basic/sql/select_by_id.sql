-- Select a single row by primary key with a named parameter.
-- Generated function: select_by_id(db:, id: Int) -> Result(List(SelectByIdRow), ...)
SELECT id, name, email, created_at, active
FROM users
WHERE id = @id
