-- REPLACE INTO: insert or replace a row if the primary key conflicts.
-- Generated function: replace_user(db:, id: Int, name: String, email: String, created_at: Int) -> Result(List(Nil), ...)
REPLACE INTO users (id, name, email, created_at)
VALUES (@id, @name, @email, @created_at)
