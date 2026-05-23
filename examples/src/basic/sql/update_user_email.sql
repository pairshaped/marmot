-- Update a row with named parameters.
-- Generated function: update_user_email(db:, email: String, id: Int) -> Result(List(Nil), ...)
UPDATE users
SET email =
@email
WHERE id = @id
