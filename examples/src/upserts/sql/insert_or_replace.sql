-- INSERT OR REPLACE: insert a row, or replace (delete + insert) if it conflicts
-- with a UNIQUE or PRIMARY KEY constraint.
INSERT OR REPLACE INTO users (id, name, email, created_at)
VALUES (@id, @name, @email, @created_at)
