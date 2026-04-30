-- INSERT with RETURNING: returns the inserted row(s).
-- The returned columns become the generated Row type.
INSERT INTO users (name, email, created_at)
VALUES (@name, @email, @created_at)
RETURNING id, name, email, created_at
