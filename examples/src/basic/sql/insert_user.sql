-- Insert a row with named parameters.
-- No RETURNING clause, so the generated function returns Nil rows.
-- Generated function: insert_user(db:, name: String, email: String, created_at: Int) -> Result(List(Nil), ...)
INSERT INTO users (name, email, created_at)
VALUES (@name, @email, @created_at)
