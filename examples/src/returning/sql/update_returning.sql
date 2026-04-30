-- UPDATE with RETURNING: update and return the modified rows.
-- Generated function returns rows instead of Nil.
UPDATE users
SET active = @active
WHERE id = @id
RETURNING id, name, active
