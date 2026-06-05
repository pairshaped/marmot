import birdie
import gleam/dict
import gleam/list
import gleam/option
import gleam/string
import marmot/internal/error
import marmot/internal/query.{Column, IntType, Parameter, StringType}
import marmot/internal/sqlite
import marmot/internal/sqlite/schema
import sqlight

pub fn introspect_integer_column_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)", on: db)
  let assert Ok(columns) = sqlite.introspect_columns(db, "t")
  columns |> string.inspect |> birdie.snap(title: "integer column")
}

pub fn introspect_all_types_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (
        a INTEGER NOT NULL,
        b REAL NOT NULL,
        c TEXT NOT NULL,
        d BLOB NOT NULL,
        e BOOLEAN NOT NULL,
        f TIMESTAMP NOT NULL,
        g DATE NOT NULL
      )",
      on: db,
    )
  let assert Ok(columns) = sqlite.introspect_columns(db, "t")
  columns |> string.inspect |> birdie.snap(title: "all types")
}

pub fn introspect_nullable_column_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (
        a INTEGER NOT NULL,
        b TEXT
      )",
      on: db,
    )
  let assert Ok(columns) = sqlite.introspect_columns(db, "t")
  columns |> string.inspect |> birdie.snap(title: "nullable column")
}

pub fn introspect_query_select_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id, username FROM users WHERE email = ?",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "select id, username with email param")
}

pub fn introspect_query_select_with_index_on_non_first_column_test() {
  // Regression test: SeekGE/SeekGT/SeekLE/SeekLT on an index cursor
  // should resolve to the indexed column, not the table's first column.
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT NOT NULL,
        age INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec("CREATE INDEX users_email_idx ON users(email)", on: db)
  // INSERT a row so the plan is not degenerate
  let assert Ok(_) =
    sqlight.exec(
      "INSERT INTO users (id, username, email, age) VALUES (1, 'alice', 'alice@example.com', 30)",
      on: db,
    )
  // WHERE email = ? with an index should use a SeekGE on the index cursor
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id, username, email, age FROM users WHERE email = ?",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "select with index on non-first column")
}

pub fn introspect_query_insert_returning_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "INSERT INTO users (username, created_at) VALUES (?, ?) RETURNING id, created_at",
    )
  result |> string.inspect |> birdie.snap(title: "insert returning")
}

pub fn introspect_insert_with_nullable_column_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        bio TEXT,
        created_at INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "INSERT INTO users (username, bio, created_at) VALUES (@username, @bio, @created_at)",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "insert with nullable column param")
}

pub fn introspect_query_no_return_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "DELETE FROM users WHERE id = ?")
  result |> string.inspect |> birdie.snap(title: "query no return")
}

pub fn introspect_query_no_params_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "SELECT id, name FROM users")
  result |> string.inspect |> birdie.snap(title: "query no params")
}

pub fn introspect_query_multiple_params_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id FROM users WHERE name = ? AND age > ?",
    )
  result |> string.inspect |> birdie.snap(title: "query multiple params")
}

pub fn introspect_query_nullable_result_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        bio TEXT
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id, bio FROM users WHERE id = ?",
    )
  result |> string.inspect |> birdie.snap(title: "query nullable result")
}

pub fn introspect_query_update_no_return_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "UPDATE users SET name = ? WHERE id = ?",
    )
  result |> string.inspect |> birdie.snap(title: "query update no return")
}

// --- Demonstrates string-based SQL parsing limitation ---
// This test shows what ACTUALLY happens with a subquery in WHERE.
// The EXPLAIN-based approach infers the parameter correctly,
// but parse_where_columns would fail on more complex DELETE/UPDATE patterns.
pub fn introspect_subquery_in_where_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE bans (user_id INTEGER NOT NULL, reason TEXT NOT NULL)",
      on: db,
    )
  // SELECT with subquery: uses EXPLAIN opcode path, should work
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id, name FROM users WHERE id IN (SELECT user_id FROM bans WHERE reason = ?)",
    )
  result |> string.inspect |> birdie.snap(title: "subquery in where")
}

// This demonstrates the string-parsing limitation:
// An INSERT with a subquery instead of VALUES is parsed incorrectly
// because parse_insert_columns splits on the first "(" / ")"
pub fn introspect_insert_with_subquery_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE logs (user_id INTEGER NOT NULL, action TEXT NOT NULL)",
      on: db,
    )
  // INSERT ... SELECT with a parameter in the subquery WHERE
  // parse_insert_columns("INSERT INTO logs (user_id, action) SELECT id, 'login' FROM users WHERE name = ?")
  // will extract columns ["user_id", "action"] (correct!)
  // But it then maps the ? parameter to "user_id" (first column), which is wrong.
  // The actual parameter is "name" from the WHERE clause.
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "INSERT INTO logs (user_id, action) SELECT id, 'login' FROM users WHERE name = ?",
    )
  // String parser would produce 2 params (one per INSERT column), but
  // there's only 1 `?`. The mismatch triggers fallback to EXPLAIN opcodes,
  // which correctly identifies 1 parameter.
  let assert 1 = list.length(result.parameters)
}

pub fn introspect_query_mixed_case_where_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id FROM users WHERE name = ? And age > ?",
    )
  result |> string.inspect |> birdie.snap(title: "query mixed case where")
}

pub fn introspect_query_update_returning_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        updated_at TIMESTAMP NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "UPDATE users SET name = ?, updated_at = ? WHERE id = ? RETURNING id, updated_at",
    )
  result |> string.inspect |> birdie.snap(title: "query update returning")
}

// --- Tests for fixed issues ---

pub fn introspect_returning_star_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "INSERT INTO users (name, email) VALUES (?, ?) RETURNING *",
    )
  result |> string.inspect |> birdie.snap(title: "returning star")
}

pub fn introspect_duplicate_parameter_names_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        age INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id FROM users WHERE age > ? AND age < ?",
    )
  result |> string.inspect |> birdie.snap(title: "duplicate parameter names")
}

pub fn introspect_three_duplicate_parameter_names_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        age INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id FROM users WHERE age > ? AND age < ? AND age = ?",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "three duplicate parameter names")
}

pub fn introspect_delete_returning_test() {
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
      db,
      "test",
      "DELETE FROM users WHERE id = ? RETURNING id, name",
    )
  result |> string.inspect |> birdie.snap(title: "delete returning")
}

pub fn introspect_where_no_spaces_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "UPDATE users SET name=? WHERE id=?")
  result |> string.inspect |> birdie.snap(title: "where no spaces")
}

pub fn introspect_returning_alias_preserves_case_test() {
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
      db,
      "test",
      "INSERT INTO users (name) VALUES (?) RETURNING id AS userId, name AS userName",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "returning alias preserves case")
}

pub fn introspect_table_named_asset_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE asset (id INTEGER NOT NULL PRIMARY KEY, value REAL NOT NULL)",
      on: db,
    )
  // "asset" contains "SET" as a substring -- should not confuse the parser
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "UPDATE asset SET value = ? WHERE id = ?",
    )
  result |> string.inspect |> birdie.snap(title: "table named asset")
}

pub fn introspect_join_query_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE posts (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT users.name, posts.title FROM posts JOIN users ON users.id = posts.user_id WHERE posts.user_id = ?",
    )
  // Columns from joined tables should resolve correctly
  result |> string.inspect |> birdie.snap(title: "join query")
}

pub fn debug_exists_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE item_features (
        id INTEGER PRIMARY KEY,
        item_id INTEGER NOT NULL,
        field_key TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT EXISTS(SELECT 1 FROM item_features WHERE item_id = ? AND field_key = ?) AS has_feature",
    )
  result |> string.inspect |> birdie.snap(title: "debug exists")
}

pub fn introspect_left_join_marks_right_side_nullable_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      );
      CREATE TABLE profiles (
        id INTEGER NOT NULL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        bio TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT u.id, u.name, p.bio
       FROM users u
       LEFT JOIN profiles p ON p.user_id = u.id",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "left join marks right side nullable")
}

pub fn introspect_left_join_on_unindexed_column_marks_right_side_nullable_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      );
      CREATE TABLE profiles (
        id INTEGER NOT NULL PRIMARY KEY,
        user_name TEXT NOT NULL,
        bio TEXT NOT NULL
      )",
      on: db,
    )
  // LEFT JOIN on user_name (unindexed TEXT column) forces SQLite to build a
  // transient OpenAutoindex cursor. NullRow then targets that autoindex cursor
  // rather than the profiles table cursor directly. apply_cursor_nullability
  // must propagate the nullable-cursor flag through the autoindex path so
  // p.bio is still marked nullable. Verified via EXPLAIN: OpenAutoindex at
  // addr 5, NullRow targeting cursor 2 (the autoindex), not cursor 1.
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT u.name, p.bio
       FROM users u
       LEFT JOIN profiles p ON p.user_name = u.name",
    )
  result
  |> string.inspect
  |> birdie.snap(
    title: "left join on unindexed column marks right side nullable",
  )
}

pub fn introspect_inner_join_keeps_both_sides_non_nullable_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      );
      CREATE TABLE profiles (
        id INTEGER NOT NULL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        bio TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT u.name, p.bio
       FROM users u
       JOIN profiles p ON p.user_id = u.id",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "inner join keeps both sides non nullable")
}

pub fn introspect_chained_left_joins_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, a_val TEXT NOT NULL);
       CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, b_val TEXT NOT NULL);
       CREATE TABLE c (id INTEGER NOT NULL PRIMARY KEY, b_id INTEGER NOT NULL, c_val TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT a.a_val, b.b_val, c.c_val
       FROM a
       LEFT JOIN b ON b.a_id = a.id
       LEFT JOIN c ON c.b_id = b.id",
    )
  result |> string.inspect |> birdie.snap(title: "chained left joins")
}

pub fn introspect_left_join_strength_reduced_by_where_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL);
       CREATE TABLE profiles (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL, bio TEXT NOT NULL)",
      on: db,
    )
  // WHERE p.bio = 'x' makes bio non-nullable in the result: SQLite rewrites
  // this LEFT JOIN as an INNER JOIN, so no NullRow is emitted. Our inference
  // should correctly report bio as non-nullable.
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT p.bio
       FROM users u
       LEFT JOIN profiles p ON p.user_id = u.id
       WHERE p.bio = 'x'",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "left join strength reduced by where")
}

pub fn introspect_mixed_inner_and_left_joins_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, a_val TEXT NOT NULL);
       CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, b_val TEXT NOT NULL);
       CREATE TABLE c (id INTEGER NOT NULL PRIMARY KEY, b_id INTEGER NOT NULL, c_val TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT a.a_val, b.b_val, c.c_val
       FROM a
       JOIN b ON b.a_id = a.id
       LEFT JOIN c ON c.b_id = b.id",
    )
  result |> string.inspect |> birdie.snap(title: "mixed inner and left joins")
}

pub fn introspect_row_number_returns_int_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, created_at INTEGER NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) AS position FROM items",
    )
  result |> string.inspect |> birdie.snap(title: "row number returns int")
}

pub fn introspect_rank_returns_int_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, score INTEGER NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id, RANK() OVER (ORDER BY score DESC) AS rk FROM items",
    )
  result |> string.inspect |> birdie.snap(title: "rank returns int")
}

pub fn introspect_order_by_with_sorter_resolves_columns_correctly_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE items (
        id INTEGER NOT NULL PRIMARY KEY,
        name_en TEXT NOT NULL,
        name_fr TEXT NOT NULL
      )",
      on: db,
    )
  // ORDER BY on a non-indexed text column forces SQLite to use a sorter.
  // The sorter-fill phase writes to result registers, then the output phase
  // re-reads into the same/different registers. Before the fix,
  // find_column_for_register picked the wrong producer (the SorterInsert
  // write), causing column types and nullability to resolve incorrectly.
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id, name_en, name_fr FROM items ORDER BY name_fr",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "order by with sorter resolves columns correctly")
}

pub fn introspect_alias_with_bang_forces_not_null_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT)",
      on: db,
    )
  // `name` is nullable in schema; the `!` suffix on the alias overrides it
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "SELECT name AS name! FROM users")
  result
  |> string.inspect
  |> birdie.snap(title: "alias with bang forces not null")
}

pub fn introspect_alias_with_question_forces_nullable_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "test", "SELECT name AS name? FROM users")
  result
  |> string.inspect
  |> birdie.snap(title: "alias with question forces nullable")
}

pub fn introspect_update_with_coalesce_named_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE participants (
        id INTEGER NOT NULL PRIMARY KEY,
        gender TEXT,
        birthdate TEXT,
        updated_at INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "UPDATE participants SET gender = COALESCE(@gender, gender), birthdate = COALESCE(@birthdate, birthdate), updated_at = @updated_at WHERE id = @id",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "update with coalesce named param")
}

pub fn introspect_update_with_subquery_named_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE line_items (
        id INTEGER NOT NULL PRIMARY KEY,
        order_id INTEGER NOT NULL,
        discount_cents INTEGER NOT NULL
      );
      CREATE TABLE line_item_discounts (
        id INTEGER NOT NULL PRIMARY KEY,
        line_item_id INTEGER NOT NULL,
        amount_cents INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "UPDATE line_items SET discount_cents = COALESCE((SELECT SUM(amount_cents) FROM line_item_discounts WHERE line_item_id = line_items.id), 0) WHERE order_id = @order_id",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "update with subquery named param")
}

// Update with IN (subquery) WHERE condition and a @named param inside the subquery.
// The @org_id param is in the subquery's WHERE, not the outer WHERE.
// It should be inferred as Int from orders.org_id.
pub fn introspect_update_with_in_subquery_named_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE line_item_question_values (
        id INTEGER NOT NULL PRIMARY KEY,
        line_item_id INTEGER NOT NULL,
        question_key TEXT NOT NULL,
        question_name TEXT
      );
      CREATE TABLE line_items (
        id INTEGER NOT NULL PRIMARY KEY,
        order_id INTEGER NOT NULL
      );
      CREATE TABLE orders (
        id INTEGER NOT NULL PRIMARY KEY,
        org_id INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "UPDATE line_item_question_values SET question_name = @question_name WHERE question_key = @question_key AND line_item_id IN (SELECT li.id FROM line_items li JOIN orders o ON o.id = li.order_id WHERE o.org_id = @org_id)",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "update with in subquery named param")
}

pub fn introspect_insert_select_with_named_params_in_select_list_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE item_features (
        id INTEGER NOT NULL PRIMARY KEY,
        item_id INTEGER NOT NULL,
        field_key TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "INSERT INTO item_features (item_id, field_key, created_at)
       SELECT @item_id, lf.field_key, @created_at
       FROM item_features lf
       WHERE lf.item_id = @source_item_id",
    )
  result
  |> string.inspect
  |> birdie.snap(title: "insert select with named params in select list")
}

// Note: CTEs currently lose type information because SQLite's EXPLAIN output
// does not trace column types through CTE boundaries. Columns from CTEs are
// inferred as StringType/nullable. This is a known limitation.
pub fn introspect_cte_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL, total REAL NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "WITH big_orders AS (
        SELECT id, user_id, total FROM orders WHERE total > ?
      )
      SELECT id, user_id, total FROM big_orders WHERE user_id = ?",
    )
  result |> string.inspect |> birdie.snap(title: "cte")
}

pub fn introspect_recursive_cte_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE categories (id INTEGER NOT NULL PRIMARY KEY, parent_id INTEGER, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "WITH RECURSIVE tree AS (
        SELECT id, parent_id, name, 0 AS depth FROM categories WHERE id = ?
        UNION ALL
        SELECT c.id, c.parent_id, c.name, t.depth + 1
        FROM categories c JOIN tree t ON c.parent_id = t.id
      )
      SELECT id, name, depth FROM tree",
    )
  result |> string.inspect |> birdie.snap(title: "recursive cte")
}

// Correlated subqueries in the SELECT list lose type info for the subquery column
pub fn introspect_correlated_subquery_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL);
       CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL, total REAL NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT u.id, u.name,
        (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count
      FROM users u
      WHERE u.id = ?",
    )
  result |> string.inspect |> birdie.snap(title: "correlated subquery")
}

pub fn introspect_multiple_joins_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL);
       CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL, status TEXT NOT NULL);
       CREATE TABLE order_items (id INTEGER NOT NULL PRIMARY KEY, order_id INTEGER NOT NULL, product_name TEXT NOT NULL, qty INTEGER NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT u.name AS user_name, o.status, oi.product_name, oi.qty
      FROM users u
      INNER JOIN orders o ON o.user_id = u.id
      INNER JOIN order_items oi ON oi.order_id = o.id
      WHERE u.id = ?",
    )
  result |> string.inspect |> birdie.snap(title: "multiple joins")
}

pub fn parse_returns_no_annotation_test() {
  let assert Ok(option.None) = sqlite.parse_returns_annotation("SELECT 1")
}

pub fn parse_returns_simple_annotation_test() {
  let assert Ok(option.Some("OrgRow")) =
    sqlite.parse_returns_annotation("-- returns: OrgRow\nSELECT 1")
}

pub fn parse_returns_with_blank_lines_above_test() {
  let assert Ok(option.Some("OrgRow")) =
    sqlite.parse_returns_annotation("\n\n-- returns: OrgRow\nSELECT 1")
}

pub fn parse_returns_with_other_comments_above_test() {
  let assert Ok(option.Some("OrgRow")) =
    sqlite.parse_returns_annotation(
      "-- this is a comment\n-- returns: OrgRow\nSELECT 1",
    )
}

pub fn parse_returns_after_sql_ignored_test() {
  // Annotation below the first SQL statement must not be picked up.
  let assert Ok(option.None) =
    sqlite.parse_returns_annotation("SELECT 1\n-- returns: OrgRow\n")
}

pub fn parse_returns_extra_whitespace_test() {
  let assert Ok(option.Some("OrgRow")) =
    sqlite.parse_returns_annotation("--    returns:    OrgRow   \nSELECT 1")
}

pub fn parse_returns_missing_row_suffix_test() {
  let assert Error(_) =
    sqlite.parse_returns_annotation("-- returns: Org\nSELECT 1")
}

pub fn parse_returns_invalid_identifier_lowercase_test() {
  let assert Error(_) =
    sqlite.parse_returns_annotation("-- returns: orgRow\nSELECT 1")
}

pub fn parse_returns_invalid_identifier_special_char_test() {
  let assert Error(_) =
    sqlite.parse_returns_annotation("-- returns: Org-Row\nSELECT 1")
}

pub fn schema_loader_marks_rowid_alias_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE rowid_t (id INTEGER PRIMARY KEY, name TEXT);
       CREATE TABLE no_rowid_t (id INTEGER PRIMARY KEY, name TEXT) WITHOUT ROWID;
       CREATE TABLE composite_t (a INTEGER, b INTEGER, PRIMARY KEY (a, b));",
      conn,
    )
  let metadata = schema.get_table_metadata_v2(conn)

  let assert Ok(rowid_cols) = dict.get(metadata.columns, "rowid_t")
  let assert Ok(rowid_id) =
    list.find(rowid_cols, fn(m) { m.column.name == "id" })
  let assert True = rowid_id.is_rowid_alias
  let assert False = rowid_id.column.nullable

  let assert Ok(no_rowid_cols) = dict.get(metadata.columns, "no_rowid_t")
  let assert Ok(no_rowid_id) =
    list.find(no_rowid_cols, fn(m) { m.column.name == "id" })
  let assert False = no_rowid_id.is_rowid_alias
  // PK column on a WITHOUT ROWID table must be non-null: SQLite enforces it.
  let assert False = no_rowid_id.column.nullable

  let assert Ok(composite_cols) = dict.get(metadata.columns, "composite_t")
  let assert Ok(composite_a) =
    list.find(composite_cols, fn(m) { m.column.name == "a" })
  let assert False = composite_a.is_rowid_alias
  // Composite PK columns are NULLABLE on ordinary rowid tables per SQLite
  // (legacy quirk; only WITHOUT ROWID and explicit NOT NULL enforce non-null).
  let assert True = composite_a.column.nullable
}

pub fn schema_loader_marks_generated_columns_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER, c INTEGER GENERATED ALWAYS AS (b * 2) VIRTUAL);",
      conn,
    )
  let metadata = schema.get_table_metadata_v2(conn)
  let assert Ok(cols) = dict.get(metadata.columns, "t")
  let names = list.map(cols, fn(m) { m.column.name })
  let assert ["a", "b", "c"] = names
  let assert Ok(c_meta) = list.find(cols, fn(m) { m.column.name == "c" })
  let assert True = c_meta.is_generated
  let assert Ok(b_meta) = list.find(cols, fn(m) { m.column.name == "b" })
  let assert False = b_meta.is_generated
}

pub fn schema_loader_int_pk_is_not_rowid_alias_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE int_t (id INT PRIMARY KEY, name TEXT);
       CREATE TABLE bigint_t (id BIGINT PRIMARY KEY, name TEXT);",
      conn,
    )
  let metadata = schema.get_table_metadata_v2(conn)

  let assert Ok(int_cols) = dict.get(metadata.columns, "int_t")
  let assert Ok(int_id) = list.find(int_cols, fn(m) { m.column.name == "id" })
  let assert False = int_id.is_rowid_alias

  let assert Ok(bigint_cols) = dict.get(metadata.columns, "bigint_t")
  let assert Ok(bigint_id) =
    list.find(bigint_cols, fn(m) { m.column.name == "id" })
  let assert False = bigint_id.is_rowid_alias
}

// ---- Resolver-driven parameter extraction ----

pub fn introspect_disambiguates_via_alias_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);
       CREATE TABLE orders (id TEXT PRIMARY KEY, user_id INTEGER NOT NULL);",
      conn,
    )
  // Both tables have an `id` column. users.id is INTEGER; orders.id is TEXT.
  // Search-all-tables resolution returns whichever it finds first.
  // Alias-aware resolution must pick orders.id for `o.id`, giving StringType.
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/by_order.sql",
      "SELECT u.email FROM users u JOIN orders o ON o.user_id = u.id WHERE o.id = @order_id",
    )
  let assert [Parameter(name: "order_id", column_type: StringType, ..)] =
    query.parameters
}

pub fn introspect_ambiguous_bare_column_errors_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);
       CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER);",
      conn,
    )
  // Bare `id` is ambiguous across users and orders. SQLite catches this at
  // EXPLAIN time before the resolver runs, so the error surfaces as SqlError.
  let assert Error(error.SqlError(_, _)) =
    sqlite.introspect_query(
      conn,
      "/tmp/ambiguous.sql",
      "SELECT email FROM users JOIN orders ON users.id = orders.user_id WHERE id = ?",
    )
}

pub fn introspect_cte_bare_column_falls_back_test() {
  // Regression for the dispatcher fix: WITH queries must route through
  // statement_parser.parse and the resolver, NOT the legacy classifier
  // which silently dispatched WITH to opcode_fallback.
  //
  // The CTE `foo` has no entry in marmot's table_schemas (CTEs aren't
  // introspected). The resolver returns UnknownTableRef -> StringType.
  // The opcode fallback path would resolve `id` to users.id (IntType)
  // by tracing through the materialized CTE. So a StringType assertion
  // proves the resolver path fired.
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE users (id INTEGER PRIMARY KEY);", conn)
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/cte_fallback.sql",
      "WITH foo AS (SELECT id FROM users) SELECT * FROM foo WHERE id = ?",
    )
  let assert [Parameter(name: "id", column_type: StringType, nullable: False)] =
    query.parameters
}

pub fn read_param_against_nullable_column_is_non_nullable_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE tasks (
        id INTEGER PRIMARY KEY,
        account_id INTEGER
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/read_nullable_column.sql",
      "SELECT id FROM tasks WHERE account_id = @account_id",
    )
  let assert [
    Parameter(name: "account_id", column_type: IntType, nullable: False),
  ] = query.parameters
}

pub fn read_param_with_is_operator_is_non_nullable_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE standings (
        id INTEGER PRIMARY KEY,
        season TEXT
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/read_is_nullable.sql",
      "SELECT id FROM standings WHERE season IS @season",
    )
  let assert [
    Parameter(name: "season", column_type: StringType, nullable: False),
  ] = query.parameters
}

pub fn read_param_with_is_operator_on_not_null_column_is_non_nullable_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/read_is_not_null_column.sql",
      "SELECT id FROM users WHERE name IS @name",
    )
  let assert [Parameter(name: "name", column_type: StringType, nullable: False)] =
    query.parameters
}

pub fn read_param_with_is_not_operator_is_non_nullable_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE standings (
        id INTEGER PRIMARY KEY,
        season TEXT
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/read_is_not_nullable.sql",
      "SELECT id FROM standings WHERE season IS NOT @season",
    )
  let assert [
    Parameter(name: "season", column_type: StringType, nullable: False),
  ] = query.parameters
}

pub fn read_param_with_null_guard_is_non_nullable_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE tasks (
        id INTEGER PRIMARY KEY,
        account_id INTEGER NOT NULL
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/read_null_guard.sql",
      "SELECT id FROM tasks
       WHERE (@account_id IS NULL OR account_id = @account_id)",
    )
  let assert [
    Parameter(name: "account_id", column_type: IntType, nullable: False),
  ] = query.parameters
}

pub fn read_param_with_suffix_null_guard_is_non_nullable_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE tasks (
        id INTEGER PRIMARY KEY,
        account_id INTEGER NOT NULL
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/read_suffix_null_guard.sql",
      "SELECT id FROM tasks
       WHERE account_id = @account_id OR @account_id IS NULL",
    )
  let assert [
    Parameter(name: "account_id", column_type: IntType, nullable: False),
  ] = query.parameters
}

pub fn read_param_with_not_null_guard_is_non_nullable_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE tasks (
        id INTEGER PRIMARY KEY,
        account_id INTEGER NOT NULL
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/read_not_null_guard.sql",
      "SELECT id FROM tasks
       WHERE @account_id IS NOT NULL AND account_id = @account_id",
    )
  let assert [Parameter(name: "account_id", nullable: False, ..)] =
    query.parameters
}

pub fn read_param_with_range_null_guard_is_non_nullable_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE tasks (
        id INTEGER PRIMARY KEY,
        created_at INTEGER NOT NULL
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/read_range_null_guard.sql",
      "SELECT id FROM tasks
       WHERE @from_date IS NULL OR created_at >= @from_date",
    )
  let assert [Parameter(name: "from_date", nullable: False, ..)] =
    query.parameters
}

pub fn delete_where_read_param_against_nullable_column_is_non_nullable_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE tasks (
        id INTEGER PRIMARY KEY,
        account_id INTEGER
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/delete_read_nullable_column.sql",
      "DELETE FROM tasks WHERE account_id = @account_id",
    )
  let assert [
    Parameter(name: "account_id", column_type: IntType, nullable: False),
  ] = query.parameters
}

pub fn read_param_inside_select_subquery_resolves_column_type_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE memberships (
        id INTEGER PRIMARY KEY,
        org_id INTEGER NOT NULL
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/select_subquery_param.sql",
      "SELECT CAST((SELECT count(*) FROM memberships WHERE org_id = @org_id) AS INTEGER) AS member_count",
    )
  let assert [Parameter(name: "org_id", column_type: IntType, nullable: False)] =
    query.parameters
}

pub fn read_param_inside_correlated_select_subquery_resolves_column_type_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE feature_requests (
        id INTEGER PRIMARY KEY
      );
      CREATE TABLE feature_request_votes (
        id INTEGER PRIMARY KEY,
        feature_request_id INTEGER NOT NULL,
        org_id INTEGER NOT NULL
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/correlated_select_subquery_param.sql",
      "SELECT
         fr.id,
         CAST((
           SELECT count(*)
           FROM feature_request_votes frv
           WHERE frv.feature_request_id = fr.id
             AND frv.org_id = @org_id
         ) AS INTEGER) AS has_voted
       FROM feature_requests fr
       WHERE fr.id = @id",
    )
  let assert [
    Parameter(name: "org_id", column_type: IntType, nullable: False),
    Parameter(name: "id", column_type: IntType, nullable: False),
  ] = query.parameters
}

pub fn read_param_inside_select_list_subquery_without_outer_where_resolves_column_type_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE feature_requests (
        id INTEGER PRIMARY KEY
      );
      CREATE TABLE feature_request_votes (
        id INTEGER PRIMARY KEY,
        feature_request_id INTEGER NOT NULL,
        org_id INTEGER
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/select_list_subquery_param.sql",
      "SELECT (
         SELECT count(*)
         FROM feature_request_votes frv
         WHERE frv.org_id = @org_id
       ) AS c
       FROM feature_requests",
    )
  let assert [Parameter(name: "org_id", column_type: IntType, nullable: False)] =
    query.parameters
}

pub fn read_param_inside_derived_table_subquery_resolves_column_type_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE participants (
        id INTEGER PRIMARY KEY,
        org_id INTEGER NOT NULL
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/derived_table_subquery_param.sql",
      "SELECT COUNT(*) FROM (
        SELECT id FROM participants p WHERE p.org_id = @org_id
      )",
    )
  let assert [Parameter(name: "org_id", column_type: IntType, nullable: False)] =
    query.parameters
}

pub fn read_params_inside_derived_table_with_exists_resolve_names_and_types_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE participants (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
      );
      CREATE TABLE participant_orgs (
        participant_id INTEGER NOT NULL,
        org_id INTEGER NOT NULL
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/derived_table_exists_param.sql",
      "SELECT COUNT(*) FROM (
        SELECT DISTINCT p.id
        FROM participants p
        WHERE EXISTS (
          SELECT 1
          FROM participant_orgs po
          WHERE po.participant_id = p.id
            AND po.org_id = @org_id
        )
        AND CAST(@search AS TEXT) = ''
      )",
    )
  let assert [
    Parameter(name: "org_id", column_type: IntType, nullable: False),
    Parameter(name: "search", column_type: StringType, nullable: False),
  ] = query.parameters
}

pub fn read_params_inside_count_wrapper_preserve_real_world_param_names_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE participants (
        id INTEGER PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT NOT NULL
      );
      CREATE TABLE line_items (
        participant_id INTEGER NOT NULL,
        item_id INTEGER
      );
      CREATE TABLE items (
        id INTEGER PRIMARY KEY,
        org_id INTEGER NOT NULL,
        season INTEGER
      );
      CREATE TABLE waiver_responses (
        participant_id INTEGER,
        org_id INTEGER NOT NULL
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/count_participants_for_admin.sql",
      "SELECT CAST(COUNT(*) AS INTEGER) AS count FROM (
        SELECT DISTINCT p.id
        FROM participants p
        WHERE (
          EXISTS (
            SELECT 1 FROM line_items li2
            JOIN items pr2 ON li2.item_id = pr2.id
            WHERE li2.participant_id = p.id AND pr2.org_id = @org_id
          )
          OR EXISTS (
            SELECT 1 FROM waiver_responses wa
            WHERE wa.participant_id = p.id AND wa.org_id = @org_id
          )
        )
        AND (CAST(@search AS TEXT) = ''
          OR p.first_name LIKE CAST(@search AS TEXT) || '%'
          OR p.last_name LIKE CAST(@search AS TEXT) || '%'
          OR p.email LIKE CAST(@search AS TEXT) || '%')
        AND (CAST(@season AS INTEGER) = 0 OR EXISTS (
          SELECT 1 FROM line_items li3
          JOIN items pr3 ON li3.item_id = pr3.id
          WHERE li3.participant_id = p.id
            AND pr3.org_id = @org_id
            AND pr3.season = CAST(@season AS INTEGER)
        ))
      )",
    )
  let assert [
    Parameter(name: "org_id", column_type: IntType, nullable: False),
    Parameter(name: "search", column_type: StringType, nullable: False),
    Parameter(name: "season", column_type: IntType, nullable: False),
  ] = query.parameters
}

pub fn update_set_write_nullable_but_where_read_non_nullable_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE tasks (
        id INTEGER PRIMARY KEY,
        account_id INTEGER,
        deleted_at INTEGER
      );",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/update_read_write_nullability.sql",
      "UPDATE tasks
       SET deleted_at = @deleted_at
       WHERE account_id = @account_id",
    )
  let assert [
    Parameter(name: "deleted_at", column_type: IntType, nullable: True),
    Parameter(name: "account_id", column_type: IntType, nullable: False),
  ] = query.parameters
}

// ---- sqlite.gleam derives Statement structure from parser ----

pub fn introspect_insert_returning_via_typed_statement_test() {
  // RETURNING presence and target table come from the typed Statement, not
  // from global keyword probes.
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/insert_returning.sql",
      "INSERT INTO events (name) VALUES (?) RETURNING id, name",
    )
  let assert [
    Column(name: "id", column_type: IntType, ..),
    Column(name: "name", column_type: StringType, ..),
  ] = query.columns
}

pub fn introspect_insert_no_returning_test() {
  // INSERT without RETURNING returns no columns. This exists to prove the
  // RETURNING detection didn't break the negative case.
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/insert_no_returning.sql",
      "INSERT INTO events (name) VALUES (?)",
    )
  let assert [] = query.columns
}

// ---- results.gleam uses typed Statement ----

pub fn results_extract_columns_via_typed_statement_test() {
  // Regression: extract_result_columns must derive select-list and from-table
  // data from the typed Statement (statement_parser.parse), not from the legacy
  // whole-statement keyword scanners. The typed parser gives a proper AST slice
  // for the SELECT list and a structured FromItem list.
  //
  // This test locks in that the column names and types are resolved correctly
  // through the new code path. The schema-lookup path (table_schemas ->
  // resolve_select_item) is exercised because "id" and "label" are bare columns.
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE events (id INTEGER PRIMARY KEY, label TEXT NOT NULL);",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/select_events.sql",
      "SELECT id, label FROM events",
    )
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "label", column_type: StringType, nullable: False),
  ] = query.columns
}

pub fn insert_values_no_column_list_uses_schema_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER);",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/insert_t.sql",
      "INSERT INTO t VALUES (?, ?, ?)",
    )
  // 3 parameters: id (rowid alias INTEGER PK on rowid table: nullable on
  // write because SQLite auto-assigns), name (NOT NULL: non-null), age
  // (no NOT NULL: nullable).
  let assert [
    Parameter(name: "id", column_type: IntType, nullable: True),
    Parameter(name: "name", column_type: StringType, nullable: False),
    Parameter(name: "age", column_type: IntType, nullable: True),
  ] = query.parameters
}

pub fn insert_values_skips_generated_columns_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER NOT NULL, c INTEGER GENERATED ALWAYS AS (b * 2) VIRTUAL);",
      conn,
    )
  // VALUES (?, ?) provides 2 expressions; bindable columns are {a, b}.
  // Generated column c is skipped.
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/insert_t.sql",
      "INSERT INTO t VALUES (?, ?)",
    )
  let assert [
    Parameter(name: "a", nullable: True, ..),
    Parameter(name: "b", nullable: False, ..),
  ] = query.parameters
}

pub fn insert_values_count_mismatch_errors_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER);", conn)
  let assert Error(error.InsertValuesCountMismatch(_, _, _, _)) =
    sqlite.introspect_query(
      conn,
      "/tmp/insert_t.sql",
      "INSERT INTO t VALUES (?)",
    )
}

pub fn insert_values_without_rowid_pk_is_not_nullable_on_write_test() {
  // Regression: WITHOUT ROWID INTEGER PRIMARY KEY is NOT a rowid alias,
  // so write-nullability follows the read-side calculation. SQLite
  // already enforces NOT NULL on PK columns of WITHOUT ROWID tables.
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT) WITHOUT ROWID;",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/insert_t.sql",
      "INSERT INTO t VALUES (?, ?)",
    )
  let assert [
    Parameter(name: "id", column_type: IntType, nullable: False),
    Parameter(name: "name", column_type: StringType, nullable: True),
  ] = query.parameters
}

pub fn insert_values_composite_pk_is_not_rowid_alias_on_write_test() {
  // Regression: composite PK columns are NOT rowid aliases. Write
  // nullability follows the read-side calculation: composite PK columns are
  // nullable on ordinary rowid tables per SQLite's legacy quirk, unless
  // explicitly NOT NULL.
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b));",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/insert_t.sql",
      "INSERT INTO t VALUES (?, ?)",
    )
  let assert [
    Parameter(name: "a", column_type: IntType, nullable: True),
    Parameter(name: "b", column_type: IntType, nullable: True),
  ] = query.parameters
}

pub fn insert_values_explicit_column_list_count_mismatch_test() {
  // Explicit column list must also be validated against VALUES row count.
  // Don't only validate the no-column-list case.
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER);", conn)
  let assert Error(error.InsertValuesCountMismatch(_, _, _, _)) =
    sqlite.introspect_query(
      conn,
      "/tmp/insert_t.sql",
      "INSERT INTO t (a, b) VALUES (?, ?, ?)",
    )
}

pub fn insert_values_multi_row_count_mismatch_identifies_row_test() {
  // Multi-row VALUES: first divergent row should be reported with 1-based index.
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE t (a INTEGER, b INTEGER);", conn)
  let assert Error(error.InsertValuesCountMismatch(
    _,
    expected: 2,
    got: 1,
    row: 2,
  )) =
    sqlite.introspect_query(
      conn,
      "/tmp/insert_t.sql",
      "INSERT INTO t VALUES (?, ?), (?)",
    )
}

pub fn insert_into_missing_table_falls_back_to_sql_error_test() {
  // Pre-flight validation must NOT mis-report missing tables as count
  // mismatches. Let SQLite produce its real "no such table" error.
  let assert Ok(conn) = sqlight.open(":memory:")
  // No CREATE TABLE: `missing` does not exist.
  let assert Error(error.SqlError(_, _)) =
    sqlite.introspect_query(
      conn,
      "/tmp/missing.sql",
      "INSERT INTO missing VALUES (?)",
    )
}

pub fn insert_values_placeholders_across_multiple_rows_test() {
  // Multi-row VALUES with placeholders in different positions per row.
  // SQLite enforces equal expression count, not equal placeholder positions.
  // Both placeholders must surface as parameters with their respective column
  // types. Row 1 has placeholder at column b; row 2 at column a.
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE t (a INTEGER NOT NULL, b TEXT NOT NULL);", conn)
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "/tmp/multirow_placeholders.sql",
      "INSERT INTO t VALUES (1, ?), (?, 'two')",
    )
  // Two parameters: one StringType (for column b in row 1), one IntType
  // (for column a in row 2). Names default to column names.
  let assert [
    Parameter(name: "b", column_type: StringType, nullable: False),
    Parameter(name: "a", column_type: IntType, nullable: False),
  ] = query.parameters
}

// ---- Real-world coverage regression guards ----
//
// Targeted tests distilled from a coverage audit against a production SQL
// corpus. Each one exercises a syntactic shape that was used in real code
// but had no dedicated marmot test. Direct type/nullability assertions so
// silent fallbacks to StringType become visible.

pub fn introspect_cast_conditional_bypass_param_test() {
  // Same named param appears twice each: once as a zero-guard
  // (`CAST(@p AS INTEGER) = 0`) and once as a column comparison
  // (`created_at >= CAST(@p AS INTEGER)`). Dedup must keep one entry per
  // name while still using the later column comparisons as type evidence.
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orders (
        id INTEGER NOT NULL PRIMARY KEY,
        org_id INTEGER NOT NULL,
        created_at INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id FROM orders
       WHERE org_id = @org_id
         AND (CAST(@start AS INTEGER) = 0 OR created_at >= CAST(@start AS INTEGER))
         AND (CAST(@end AS INTEGER) = 0 OR created_at <= CAST(@end AS INTEGER))",
    )
  let assert [
    Parameter(name: "org_id", column_type: IntType, nullable: False),
    Parameter(name: "start", column_type: IntType, nullable: False),
    Parameter(name: "end", column_type: IntType, nullable: False),
  ] = result.parameters
}

pub fn introspect_not_exists_subquery_with_param_test() {
  // NOT EXISTS uses a different SQLite opcode path than EXISTS. The named
  // param inside the subquery's WHERE must still be discovered and typed
  // from its column comparison.
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE participants (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE line_items (
        participant_id INTEGER NOT NULL,
        status TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT p.id, p.name FROM participants p
       WHERE NOT EXISTS (
         SELECT 1 FROM line_items li
         WHERE li.participant_id = p.id AND li.status = @status
       )",
    )
  let assert [
    Parameter(name: "status", column_type: StringType, nullable: False),
  ] = result.parameters
}

pub fn introspect_order_by_case_with_named_param_test() {
  // @by_date is reused across WHERE (twice) and ORDER BY CASE (once).
  // @from_ts is reused across WHERE (twice). Dedup must keep one entry per
  // name. Type for @by_date comes from string-literal comparison context;
  // @from_ts comes from `created_at` (INTEGER).
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE payments (
        id INTEGER NOT NULL PRIMARY KEY,
        created_at INTEGER NOT NULL,
        deposited_on INTEGER
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id, created_at, deposited_on FROM payments
       WHERE (@by_date = 'created_at' AND created_at >= @from_ts)
          OR (@by_date = 'deposited_on' AND deposited_on IS NOT NULL AND deposited_on >= @from_ts)
       ORDER BY CASE WHEN @by_date = 'deposited_on' THEN deposited_on END DESC, created_at DESC",
    )
  let assert [
    Parameter(name: "by_date", column_type: StringType, nullable: False),
    Parameter(name: "from_ts", column_type: IntType, nullable: False),
  ] = result.parameters
}

pub fn introspect_update_with_column_arithmetic_param_test() {
  // Named params live inside arithmetic expressions in both SET and WHERE:
  //   SET balance = balance + @delta
  //   WHERE balance + @min_delta >= 0
  // SET uses write-position inference, so @delta resolves from balance.
  // WHERE uses arithmetic expression context to resolve @min_delta from
  // balance as a read-position Int param.
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE credits (
        id INTEGER NOT NULL PRIMARY KEY,
        balance INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "UPDATE credits
       SET balance = balance + @delta
       WHERE id = @id AND balance + @min_delta >= 0
       RETURNING id, balance",
    )
  let assert [
    Parameter(name: "delta", column_type: IntType, nullable: False),
    Parameter(name: "id", column_type: IntType, nullable: False),
    Parameter(name: "min_delta", column_type: IntType, nullable: False),
  ] = result.parameters
}

pub fn introspect_select_with_multiplication_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE line_items (
        id INTEGER NOT NULL PRIMARY KEY,
        quantity INTEGER NOT NULL,
        unit_price INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id FROM line_items WHERE quantity * unit_price >= quantity * @threshold",
    )
  let assert [
    Parameter(name: "threshold", column_type: IntType, nullable: False),
  ] = result.parameters
}

// ---- Adversarial SQL regression probes ----
//
// These cases are small versions of SQL shapes that tend to expose parser
// boundary or binder-scope bugs. They are direct assertions so wrong fallback
// parameters are visible as test failures.

pub fn introspect_param_inside_cte_body_resolves_column_type_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orders (
        id INTEGER NOT NULL PRIMARY KEY,
        org_id INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "WITH filtered AS (
         SELECT id FROM orders WHERE org_id = @org_id
       )
       SELECT COUNT(*) FROM filtered",
    )
  let assert [Parameter(name: "org_id", column_type: IntType, nullable: False)] =
    result.parameters
}

pub fn introspect_param_in_second_union_arm_resolves_column_type_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        org_id INTEGER NOT NULL
      );
      CREATE TABLE archived_users (
        id INTEGER NOT NULL PRIMARY KEY,
        org_id INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id FROM users
       UNION ALL
       SELECT id FROM archived_users WHERE org_id = @org_id",
    )
  let assert [Parameter(name: "org_id", column_type: IntType, nullable: False)] =
    result.parameters
}

pub fn introspect_param_in_join_on_resolves_column_type_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE participants (
        id INTEGER NOT NULL PRIMARY KEY
      );
      CREATE TABLE line_items (
        id INTEGER NOT NULL PRIMARY KEY,
        participant_id INTEGER NOT NULL,
        org_id INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT p.id
       FROM participants p
       JOIN line_items li
         ON li.participant_id = p.id
        AND li.org_id = @org_id",
    )
  let assert [Parameter(name: "org_id", column_type: IntType, nullable: False)] =
    result.parameters
}

pub fn introspect_update_from_where_param_resolves_column_type_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE participants (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL
      );
      CREATE TABLE line_items (
        id INTEGER NOT NULL PRIMARY KEY,
        participant_id INTEGER NOT NULL,
        org_id INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "UPDATE participants
       SET name = @name
       FROM line_items li
       WHERE li.participant_id = participants.id
         AND li.org_id = @org_id",
    )
  let assert [
    Parameter(name: "name", column_type: StringType, nullable: False),
    Parameter(name: "org_id", column_type: IntType, nullable: False),
  ] = result.parameters
}

pub fn introspect_inner_alias_shadowing_resolves_inner_scope_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY
      );
      CREATE TABLE audit_logs (
        id INTEGER NOT NULL PRIMARY KEY,
        actor_id INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT *
       FROM users u
       WHERE EXISTS (
         SELECT 1 FROM audit_logs u WHERE u.actor_id = @actor_id
       )",
    )
  let assert [
    Parameter(name: "actor_id", column_type: IntType, nullable: False),
  ] = result.parameters
}

pub fn introspect_case_result_param_compared_to_column_resolves_type_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orders (
        id INTEGER NOT NULL PRIMARY KEY,
        status_rank INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "test",
      "SELECT id
       FROM orders
       WHERE CASE
         WHEN @rank = 0 THEN status_rank
         ELSE @rank
       END = status_rank",
    )
  let assert [Parameter(name: "rank", column_type: IntType, nullable: False)] =
    result.parameters
}
