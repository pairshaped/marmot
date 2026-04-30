-- Delete a row by id.
-- Generated function: delete_user(db:, id: Int) -> Result(List(Nil), ...)
DELETE FROM users
WHERE id = @id
