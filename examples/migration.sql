-- Schema and seed data for Marmot examples.
-- Run: sqlite3 demo.sqlite < migration.sql

CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  active INTEGER NOT NULL DEFAULT 1
);

INSERT INTO users (id, name, email, created_at, active) VALUES
  (1, 'Alice',   'alice@example.com',   1714400000, 1),
  (2, 'Bob',     'bob@example.com',     1714486400, 1),
  (3, 'Charlie', 'charlie@example.com', 1714572800, 0),
  (4, 'Diana',   'diana@example.com',   1714659200, 1),
  (5, 'Eve',     'eve@example.com',     1714745600, 0);

CREATE TABLE posts (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id),
  title TEXT NOT NULL,
  body TEXT,
  published_at INTEGER
);

INSERT INTO posts (id, user_id, title, body, published_at) VALUES
  (1,  1, 'Getting Started with Gleam',       'Gleam is a type-safe language...',   1714500000),
  (2,  1, 'Why SQLite is Underrated',          'SQLite is fast and reliable...',     1714600000),
  (3,  2, 'Building APIs with Marmot',         'Marmot generates type-safe SQL...',  NULL),
  (4,  2, 'A Guide to Functional Programming', 'Functional programming is...',       1714800000),
  (5,  3, 'Draft: Untitled',                   NULL,                                 NULL),
  (6,  4, '10 Tips for Better Queries',        'Writing good SQL...',                1715000000);

CREATE TABLE comments (
  id INTEGER PRIMARY KEY,
  post_id INTEGER NOT NULL REFERENCES posts(id),
  user_id INTEGER NOT NULL REFERENCES users(id),
  body TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

INSERT INTO comments (id, post_id, user_id, body, created_at) VALUES
  (1, 1, 2, 'Great intro!',                            1714550000),
  (2, 1, 4, 'Thanks for writing this.',                 1714560000),
  (3, 2, 5, 'Totally agree, SQLite is amazing.',        1714650000),
  (4, 4, 1, 'The section on pattern matching is great.', 1714850000),
  (5, 4, 3, 'Could you explain monads?',                1714900000);

CREATE TABLE tags (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);

INSERT INTO tags (id, name) VALUES
  (1, 'gleam'),
  (2, 'sqlite'),
  (3, 'tutorial'),
  (4, 'opinion');

CREATE TABLE post_tags (
  post_id INTEGER NOT NULL REFERENCES posts(id),
  tag_id INTEGER NOT NULL REFERENCES tags(id),
  PRIMARY KEY (post_id, tag_id)
);

INSERT INTO post_tags (post_id, tag_id) VALUES
  (1, 1), (1, 3),
  (2, 2), (2, 4),
  (3, 1), (3, 2), (3, 3),
  (4, 1), (4, 3);

CREATE TABLE orders (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id),
  price REAL NOT NULL,
  created_at INTEGER NOT NULL
);

INSERT INTO orders (id, user_id, price, created_at) VALUES
  (1, 1, 29.99, 1714600000),
  (2, 1, 12.50, 1714700000),
  (3, 2, 45.00, 1714800000),
  (4, 4, 8.75,  1714900000),
  (5, 4, 22.00, 1715000000);

CREATE TABLE archived_posts (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  user_id INTEGER NOT NULL
);
