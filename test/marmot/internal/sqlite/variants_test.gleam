import birdie
import gleam/list
import gleam/option
import gleam/string
import marmot/internal/sqlite
import sqlight

// ---- UPSERT / ON CONFLICT ----

pub fn introspect_upsert_do_nothing_returning_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "INSERT INTO t (id, val) VALUES (?, ?) ON CONFLICT(id) DO NOTHING RETURNING id, val",
    )
  result |> string.inspect |> birdie.snap(title: "upsert do nothing returning")
}

pub fn introspect_upsert_do_update_returning_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "INSERT INTO t (id, val, updated_at) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET val = ?, updated_at = ? RETURNING id, val, updated_at",
    )
  result |> string.inspect |> birdie.snap(title: "upsert do update returning")
}

pub fn introspect_upsert_do_update_with_named_params_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL, counter INTEGER NOT NULL DEFAULT 1)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "INSERT INTO t (id, val, counter) VALUES (@id, @val, @counter) ON CONFLICT(id) DO UPDATE SET val = @val, counter = @counter",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "upsert do update with named params")
}

pub fn introspect_upsert_do_update_set_with_where_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 0)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "INSERT INTO t (id, val, version) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET val = ?, version = ? WHERE version < ? RETURNING id, val, version",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "upsert do update set with where")
}

pub fn introspect_upsert_do_nothing_no_returning_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "INSERT INTO t (id, val) VALUES (?, ?) ON CONFLICT(id) DO NOTHING",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "upsert do nothing no returning")
}

pub fn introspect_upsert_no_conflict_target_test() {
  // ON CONFLICT with no conflict target — fires on any uniqueness violation
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "INSERT INTO t (id, val) VALUES (?, ?) ON CONFLICT DO NOTHING",
    )
  result |> string.inspect |> birdie.snap(title: "upsert no conflict target")
}

pub fn introspect_upsert_insert_select_test() {
  // INSERT ... SELECT with ON CONFLICT — requires WHERE true to disambiguate ON
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE src (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec("INSERT INTO src (id, val) VALUES (1, 'hello')", on: db)
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "INSERT INTO t (id, val) SELECT id, val FROM src WHERE true ON CONFLICT(id) DO UPDATE SET val = excluded.val RETURNING id, val",
    )
  result |> string.inspect |> birdie.snap(title: "upsert insert select")
}

// ---- LIKE operator ----

pub fn introspect_like_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "SELECT id, name FROM users WHERE name LIKE ?")
  result |> string.inspect |> birdie.snap(title: "like param")
}

pub fn introspect_like_named_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "SELECT id FROM users WHERE name LIKE @pattern")
  result |> string.inspect |> birdie.snap(title: "like named param")
}

// ---- IS NULL / IS NOT NULL ----

pub fn introspect_is_null_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        deleted_at INTEGER
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT id, name FROM users WHERE deleted_at IS NULL",
    )
  result |> string.inspect |> birdie.snap(title: "is null")
}

pub fn introspect_is_not_null_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT id, name FROM users WHERE email IS NOT NULL",
    )
  result |> string.inspect |> birdie.snap(title: "is not null")
}

// ---- IN (literal list) ----

pub fn introspect_in_literal_list_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT id, name FROM users WHERE id IN (1, 2, 3)",
    )
  result |> string.inspect |> birdie.snap(title: "in literal list")
}

pub fn introspect_in_literal_list_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        status TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT id FROM users WHERE status IN (?, ?, ?)",
    )
  result |> string.inspect |> birdie.snap(title: "in literal list with params")
}

// ---- RIGHT JOIN / CROSS JOIN ----

pub fn introspect_right_join_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        org_id INTEGER NOT NULL,
        name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orgs (
        id INTEGER NOT NULL PRIMARY KEY,
        org_name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT u.id, o.org_name FROM users u RIGHT JOIN orgs o ON u.org_id = o.id",
    )
  result |> string.inspect |> birdie.snap(title: "right join")
}

pub fn introspect_cross_join_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, val_a TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, val_b TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "SELECT a.id, b.id FROM a CROSS JOIN b")
  result |> string.inspect |> birdie.snap(title: "cross join")
}

// ---- INSERT OR REPLACE / INSERT OR IGNORE ----

pub fn introspect_insert_or_replace_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "INSERT OR REPLACE INTO users (id, name) VALUES (?, ?)",
    )
  result |> string.inspect |> birdie.snap(title: "insert or replace")
}

pub fn introspect_insert_or_ignore_returning_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "INSERT OR IGNORE INTO users (name) VALUES (?) RETURNING id, name",
    )
  result |> string.inspect |> birdie.snap(title: "insert or ignore returning")
}

// ---- REPLACE INTO ----

pub fn introspect_replace_into_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "REPLACE INTO users (id, name) VALUES (?, ?)")
  result |> string.inspect |> birdie.snap(title: "replace into")
}

// ---- NOT IN / NOT LIKE / NOT BETWEEN ----

pub fn introspect_not_in_subquery_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE deleted (user_id INTEGER NOT NULL)", on: db)
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT id, name FROM users WHERE id NOT IN (SELECT user_id FROM deleted) AND name = ?",
    )
  result |> string.inspect |> birdie.snap(title: "not in subquery with param")
}

pub fn introspect_not_like_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "SELECT id FROM users WHERE name NOT LIKE ?")
  result |> string.inspect |> birdie.snap(title: "not like param")
}

pub fn introspect_not_between_named_params_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE events (
        id INTEGER NOT NULL PRIMARY KEY,
        created_at INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT id FROM events WHERE created_at NOT BETWEEN @start AND @end",
    )
  result |> string.inspect |> birdie.snap(title: "not between named params")
}

// ---- HAVING text-based param inference ----

pub fn introspect_having_named_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orders (
        id INTEGER NOT NULL PRIMARY KEY,
        region TEXT NOT NULL,
        amount INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT region, SUM(amount) AS total FROM orders GROUP BY region HAVING SUM(amount) > @min_amount",
    )
  result |> string.inspect |> birdie.snap(title: "having named param")
}

// ---- ORDER BY param binding ----

pub fn introspect_order_by_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT id, name FROM users WHERE name = ? ORDER BY id LIMIT ?",
    )
  result |> string.inspect |> birdie.snap(title: "order by with limit param")
}

// ---- Error path tests ----

pub fn introspect_columns_nonexistent_table_test() {
  // SQLite PRAGMA table_info returns empty set for nonexistent tables
  use db <- sqlight.with_connection(":memory:")
  let assert Ok([]) = sqlite.introspect_columns(db, "nonexistent_table")
}

pub fn introspect_query_syntax_error_test() {
  use db <- sqlight.with_connection(":memory:")
  let result = sqlite.introspect_query(db, "test", "THIS IS NOT VALID SQL")
  let assert Error(_) = result
}

pub fn introspect_query_empty_sql_test() {
  use db <- sqlight.with_connection(":memory:")
  let result = sqlite.introspect_query(db, "test", "")
  let assert Error(_) = result
}

pub fn parse_returns_annotation_invalid_test() {
  let assert Error(_) =
    sqlite.parse_returns_annotation("-- returns: NotARowType\nSELECT 1")
}

pub fn parse_returns_annotation_valid_test() {
  let assert Ok(option.Some("OrgRow")) =
    sqlite.parse_returns_annotation("-- returns: OrgRow\nSELECT id FROM orgs")
}

pub fn parse_returns_annotation_missing_test() {
  let assert Ok(option.None) =
    sqlite.parse_returns_annotation("SELECT id FROM orgs")
}

// ---- INSERT / REPLACE table name extraction (marmot-b10o) ----
// SQLite requires INTO for INSERT/REPLACE statements. These tests verify
// statement.parse_insert_table_name correctly handles the INSERT [OR ...] [INTO] prefix.

pub fn introspect_insert_or_replace_into_table_name_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "INSERT OR REPLACE INTO users (id, name) VALUES (?, ?)",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "insert or replace into table name")
}

pub fn introspect_replace_into_table_name_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "REPLACE INTO users (id, name) VALUES (?, ?)")
  result |> string.inspect |> birdie.snap(title: "replace into table name")
}

// ---- COUNT(DISTINCT) and FILTER(WHERE) (marmot-8iyp) ----

pub fn introspect_count_distinct_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orders (
        id INTEGER NOT NULL PRIMARY KEY,
        customer_id INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT COUNT(DISTINCT customer_id) AS unique_customers FROM orders",
    )
  result |> string.inspect |> birdie.snap(title: "count distinct")
}

pub fn introspect_count_filter_where_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orders (
        id INTEGER NOT NULL PRIMARY KEY,
        status TEXT NOT NULL,
        amount REAL NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT COUNT(*) FILTER(WHERE status = 'active') AS active_count FROM orders",
    )
  result |> string.inspect |> birdie.snap(title: "count filter where")
}

pub fn introspect_sum_filter_where_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orders (
        id INTEGER NOT NULL PRIMARY KEY,
        status TEXT NOT NULL,
        amount REAL NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT SUM(amount) FILTER(WHERE status = 'paid') AS total_paid FROM orders",
    )
  result |> string.inspect |> birdie.snap(title: "sum filter where")
}

pub fn introspect_filter_where_with_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orders (
        id INTEGER NOT NULL PRIMARY KEY,
        status TEXT NOT NULL,
        amount REAL NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT SUM(amount) FILTER(WHERE status = @status) AS total FROM orders",
    )
  result |> string.inspect |> birdie.snap(title: "filter where with param")
}

// ---- NATURAL JOIN and USING (marmot-xwim) ----

pub fn introspect_natural_join_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, val_a TEXT NOT NULL);
       CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, val_b TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "SELECT a.id, a.val_a FROM a NATURAL JOIN b")
  result |> string.inspect |> birdie.snap(title: "natural join")
}

pub fn introspect_natural_left_join_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, val_a TEXT NOT NULL);
       CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, val_b TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT a.id, a.val_a FROM a NATURAL LEFT JOIN b",
    )
  result |> string.inspect |> birdie.snap(title: "natural left join")
}

pub fn introspect_join_using_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, org_id INTEGER NOT NULL, total INTEGER NOT NULL);
       CREATE TABLE line_items (id INTEGER NOT NULL PRIMARY KEY, org_id INTEGER NOT NULL, product TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT o.total, l.product FROM orders o JOIN line_items l USING (org_id)",
    )
  result |> string.inspect |> birdie.snap(title: "join using")
}

// ---- LIMIT/OFFSET params default to IntType (marmot-hm5t) ----

pub fn introspect_limit_param_type_is_int_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "SELECT id, name FROM users LIMIT ?")
  let assert Ok(param) = list.first(result.parameters)
  param |> string.inspect |> birdie.snap(title: "limit param type is int")
}

pub fn introspect_limit_offset_param_types_are_int_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "SELECT id, name FROM users LIMIT ? OFFSET ?")
  result.parameters
  |> string.inspect
  |> birdie.snap(title: "limit offset param types are int")
}

pub fn introspect_limit_named_param_type_is_int_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT id, name FROM users LIMIT @limit OFFSET @offset",
    )
  result.parameters
  |> string.inspect
  |> birdie.snap(title: "limit named param type is int")
}

// ---- GROUP BY / HAVING param text-based inference (marmot-gqwq) ----

pub fn introspect_having_named_param_infers_column_type_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE sales (
        id INTEGER NOT NULL PRIMARY KEY,
        region TEXT NOT NULL,
        amount INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db, "test",
      "SELECT region, SUM(amount) AS total FROM sales GROUP BY region HAVING SUM(amount) > @min_amount",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "having named param infers column type")
}
