import birdie
import gleam/list
import gleam/option
import gleam/string
import marmot/internal/sqlite
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
      "SELECT id, username FROM users WHERE email = ?",
    )
  result |> string.inspect |> birdie.snap(title: "select id, username with email param")
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
      "INSERT INTO users (username, bio, created_at) VALUES (@username, @bio, @created_at)",
    )
  result |> string.inspect |> birdie.snap(title: "insert with nullable column param")
}

pub fn introspect_query_no_return_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "DELETE FROM users WHERE id = ?")
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
    sqlite.introspect_query(db, "SELECT id, name FROM users")
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
    sqlite.introspect_query(db, "SELECT id, bio FROM users WHERE id = ?")
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
    sqlite.introspect_query(db, "UPDATE users SET name = ? WHERE id = ?")
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
  // SELECT with subquery — uses EXPLAIN opcode path, should work
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
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
  // will extract columns ["user_id", "action"] — correct!
  // But it then maps the ? parameter to "user_id" (first column), which is wrong.
  // The actual parameter is "name" from the WHERE clause.
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
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
      "SELECT id FROM users WHERE age > ? AND age < ?",
    )
  result |> string.inspect |> birdie.snap(title: "duplicate parameter names")
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
    sqlite.introspect_query(db, "UPDATE users SET name=? WHERE id=?")
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
      "INSERT INTO users (name) VALUES (?) RETURNING id AS userId, name AS userName",
    )
  result |> string.inspect |> birdie.snap(title: "returning alias preserves case")
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
    sqlite.introspect_query(db, "UPDATE asset SET value = ? WHERE id = ?")
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
      "SELECT u.id, u.name, p.bio
       FROM users u
       LEFT JOIN profiles p ON p.user_id = u.id",
    )
  result |> string.inspect |> birdie.snap(title: "left join marks right side nullable")
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
      "SELECT u.name, p.bio
       FROM users u
       LEFT JOIN profiles p ON p.user_name = u.name",
    )
  result |> string.inspect |> birdie.snap(title: "left join on unindexed column marks right side nullable")
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
      "SELECT u.name, p.bio
       FROM users u
       JOIN profiles p ON p.user_id = u.id",
    )
  result |> string.inspect |> birdie.snap(title: "inner join keeps both sides non nullable")
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
      "SELECT p.bio
       FROM users u
       LEFT JOIN profiles p ON p.user_id = u.id
       WHERE p.bio = 'x'",
    )
  result |> string.inspect |> birdie.snap(title: "left join strength reduced by where")
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
      "SELECT id, name_en, name_fr FROM items ORDER BY name_fr",
    )
  result |> string.inspect |> birdie.snap(title: "order by with sorter resolves columns correctly")
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
    sqlite.introspect_query(db, "SELECT name AS name! FROM users")
  result |> string.inspect |> birdie.snap(title: "alias with bang forces not null")
}

pub fn introspect_alias_with_question_forces_nullable_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT name AS name? FROM users")
  result |> string.inspect |> birdie.snap(title: "alias with question forces nullable")
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
      "UPDATE participants SET gender = COALESCE(@gender, gender), birthdate = COALESCE(@birthdate, birthdate), updated_at = @updated_at WHERE id = @id",
    )
  result |> string.inspect |> birdie.snap(title: "update with coalesce named param")
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
      "UPDATE line_items SET discount_cents = COALESCE((SELECT SUM(amount_cents) FROM line_item_discounts WHERE line_item_id = line_items.id), 0) WHERE order_id = @order_id",
    )
  result |> string.inspect |> birdie.snap(title: "update with subquery named param")
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
      "UPDATE line_item_question_values SET question_name = @question_name WHERE question_key = @question_key AND line_item_id IN (SELECT li.id FROM line_items li JOIN orders o ON o.id = li.order_id WHERE o.org_id = @org_id)",
    )
  result |> string.inspect |> birdie.snap(title: "update with in subquery named param")
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
      "INSERT INTO item_features (item_id, field_key, created_at)
       SELECT @item_id, lf.field_key, @created_at
       FROM item_features lf
       WHERE lf.item_id = @source_item_id",
    )
  result |> string.inspect |> birdie.snap(title: "insert select with named params in select list")
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

pub fn introspect_mixed_left_inner_joins_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL);
       CREATE TABLE profiles (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL, bio TEXT NOT NULL);
       CREATE TABLE avatars (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL, url TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT u.name, p.bio, a.url AS avatar_url
      FROM users u
      INNER JOIN profiles p ON p.user_id = u.id
      LEFT JOIN avatars a ON a.user_id = u.id
      WHERE u.id = ?",
    )
  result |> string.inspect |> birdie.snap(title: "mixed left inner joins")
}

// --- CASE expression inference tests ---

pub fn introspect_case_int_literals_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CASE WHEN active THEN 1 ELSE 0 END AS registered FROM t",
    )
  result |> string.inspect |> birdie.snap(title: "case int literals")
}

pub fn introspect_case_string_literals_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CASE WHEN active THEN 'yes' ELSE 'no' END AS label FROM t",
    )
  result |> string.inspect |> birdie.snap(title: "case string literals")
}

pub fn introspect_case_no_else_nullable_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CASE WHEN active THEN 1 END AS maybe_val FROM t",
    )
  result |> string.inspect |> birdie.snap(title: "case no else nullable")
}

pub fn introspect_case_null_branch_nullable_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CASE WHEN active THEN 1 ELSE NULL END AS maybe_val FROM t",
    )
  result |> string.inspect |> birdie.snap(title: "case null branch nullable")
}

pub fn introspect_case_mixed_types_fallback_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CASE WHEN active THEN 1 ELSE 'a' END AS mixed FROM t",
    )
  result |> string.inspect |> birdie.snap(title: "case mixed types fallback")
}

pub fn introspect_case_nested_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a BOOLEAN NOT NULL, b BOOLEAN NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CASE WHEN a THEN CASE WHEN b THEN 1 ELSE 0 END ELSE 2 END AS val FROM t",
    )
  result |> string.inspect |> birdie.snap(title: "case nested")
}

pub fn introspect_case_simple_form_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, status INT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END AS label FROM t",
    )
  result |> string.inspect |> birdie.snap(title: "case simple form")
}

pub fn introspect_case_column_ref_fallback_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a TEXT NOT NULL, b TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CASE WHEN id > 0 THEN a ELSE b END AS val FROM t",
    )
  result |> string.inspect |> birdie.snap(title: "case column ref fallback")
}

// Regression: UPDATE with `WHERE col = (subquery)` where the subquery
// contains multiple @named params. Previously only the first subquery
// param was extracted, causing the rest to fall through to the opcode
// fallback and produce generic `param`/`param_N: String` labels.
pub fn introspect_update_with_eq_subquery_multiple_named_params_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE waitlist_registrations (
        id INTEGER NOT NULL PRIMARY KEY,
        item_id INTEGER NOT NULL,
        account_id INTEGER NOT NULL,
        org_id INTEGER NOT NULL,
        claimed_at INTEGER,
        updated_at INTEGER NOT NULL,
        approved_at INTEGER,
        cancelled_at INTEGER
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "UPDATE waitlist_registrations
       SET claimed_at = @claimed_at, updated_at = @updated_at
       WHERE id = (
         SELECT wr.id FROM waitlist_registrations wr
         WHERE wr.item_id = @item_id AND wr.account_id = @account_id AND wr.org_id = @org_id
           AND wr.approved_at IS NOT NULL AND wr.claimed_at IS NULL AND wr.cancelled_at IS NULL
         ORDER BY wr.approved_at ASC
         LIMIT 1
       )",
    )
  result |> string.inspect |> birdie.snap(title: "update with eq subquery multiple named params")
}

pub fn introspect_case_with_exists_subquery_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE events (id INTEGER NOT NULL PRIMARY KEY, season_id INT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE seasons (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CASE WHEN EXISTS(SELECT 1 FROM events WHERE events.season_id = seasons.id) THEN 1 ELSE 0 END AS registered FROM seasons",
    )
  result |> string.inspect |> birdie.snap(title: "case with exists subquery")
}

// --- String literal awareness in comma/condition splitting ---

pub fn insert_values_with_string_containing_comma_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE notes (id INTEGER NOT NULL PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "INSERT INTO notes (id, title, body) VALUES (?, 'hello, world', ?)",
    )
  result |> string.inspect |> birdie.snap(title: "insert values with string containing comma")
}

pub fn insert_values_with_escaped_quote_in_string_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE notes (id INTEGER NOT NULL PRIMARY KEY, title TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "INSERT INTO notes (id, title) VALUES (?, 'it''s, complicated')",
    )
  result |> string.inspect |> birdie.snap(title: "insert values with escaped quote in string")
}

pub fn where_with_string_containing_and_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL)",
      on: db,
    )
  // The AND inside the string literal must not split the WHERE condition
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id FROM users WHERE name != 'foo AND bar' AND email = ?",
    )
  result |> string.inspect |> birdie.snap(title: "where with string containing and")
}

pub fn where_with_string_containing_or_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id FROM users WHERE name != 'yes or no' AND status = ?",
    )
  result |> string.inspect |> birdie.snap(title: "where with string containing or")
}

pub fn select_with_string_literal_containing_comma_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  // The comma inside the string literal in COALESCE must not split SELECT items
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT COALESCE(name, 'unknown, unnamed') AS display_name FROM users WHERE id = ?",
    )
  result |> string.inspect |> birdie.snap(title: "select with string literal containing comma")
}

// --- Tests for previously untested fixes ---

pub fn introspect_dollar_name_parameter_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id, name FROM users WHERE id = $id AND name = $name",
    )
  result |> string.inspect |> birdie.snap(title: "dollar name parameter")
}

pub fn introspect_sum_returns_float_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, amount REAL NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT SUM(amount) AS total FROM orders")
  result |> string.inspect |> birdie.snap(title: "sum returns float")
}

pub fn introspect_returning_cast_as_alias_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "INSERT INTO t (val) VALUES (?) RETURNING CAST(id AS TEXT) AS id_text",
    )
  result |> string.inspect |> birdie.snap(title: "returning cast as alias")
}

pub fn introspect_nested_cast_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val REAL NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CAST(CAST(val AS INT) AS TEXT) AS converted FROM t",
    )
  result |> string.inspect |> birdie.snap(title: "nested cast")
}

pub fn introspect_double_quoted_identifier_with_escaped_quotes_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (\"my\"\"col\" INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT \"my\"\"col\", name FROM t")
  let assert 2 = list.length(result.columns)
}

pub fn introspect_keyword_inside_string_literal_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "UPDATE t SET name = 'hello WHERE world' WHERE id = ?",
    )
  result |> string.inspect |> birdie.snap(title: "keyword inside string literal")
}

pub fn introspect_double_quoted_identifier_containing_placeholder_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (\"what?\" INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT \"what?\", val FROM t WHERE val = ?")
  result |> string.inspect |> birdie.snap(title: "double quoted identifier containing placeholder")
}

pub fn introspect_nested_function_unwrap_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "UPDATE t SET name = LOWER(TRIM(name)) WHERE id = ?",
    )
  result |> string.inspect |> birdie.snap(title: "nested function unwrap")
}

pub fn introspect_case_with_string_containing_end_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CASE WHEN active THEN 'THE END' ELSE 'no END here' END AS label FROM t",
    )
  result |> string.inspect |> birdie.snap(title: "case with string containing end")
}

pub fn introspect_placeholder_after_escaped_quote_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id FROM t WHERE name != 'it''s' AND id = ?",
    )
  result |> string.inspect |> birdie.snap(title: "placeholder after escaped quote")
}

pub fn introspect_string_literal_containing_close_paren_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT COALESCE(name, 'default)value') AS display FROM t WHERE id = ?",
    )
  result |> string.inspect |> birdie.snap(title: "string literal containing close paren")
}

pub fn introspect_keyword_in_subquery_not_matched_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL, total REAL NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id, name FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > ?)",
    )
  result |> string.inspect |> birdie.snap(title: "keyword in subquery not matched")
}

pub fn introspect_double_quoted_keyword_identifier_in_where_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, \"AND\" TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT id FROM t WHERE \"AND\" = ?")
  result |> string.inspect |> birdie.snap(title: "double quoted keyword identifier in where")
}

// Regression: CAST(COUNT(*) AS INTEGER) should infer IntType, not
// fall through to the StringType/nullable=True default. The outer CAST
// declares the target type and should be trusted when opcode tracing
// can't reach through the aggregate+cast chain.
pub fn introspect_cast_count_as_integer_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)", on: db)
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CAST(COUNT(*) AS INTEGER) AS count FROM t",
    )
  result |> string.inspect |> birdie.snap(title: "cast count as integer")
}

// Regression: CAST(COALESCE(SUM(...), 0) AS INTEGER) should infer IntType.
// The outer CAST target is explicit; use it when opcode tracing can't
// resolve the chained aggregate/coalesce through to a source column.
pub fn introspect_cast_coalesce_sum_as_integer_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, amount_cents INTEGER NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CAST(COALESCE(SUM(amount_cents), 0) AS INTEGER) AS total FROM t",
    )
  result |> string.inspect |> birdie.snap(title: "cast coalesce sum as integer")
}

// Regression: outer CAST around a subquery-aliased column should honor
// the CAST target type.
pub fn introspect_cast_subquery_column_as_integer_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT CAST(sub.val AS INTEGER) AS v
       FROM (SELECT val FROM t) sub",
    )
  result |> string.inspect |> birdie.snap(title: "cast subquery column as integer")
}

pub fn between_named_params_infer_column_type_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE events (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id, name FROM events WHERE created_at BETWEEN @from_ts AND @to_ts",
    )
  result |> string.inspect |> birdie.snap(title: "between named params infer column type")
}

pub fn coalesce_max_plus_literal_returns_int_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE items (
        id INTEGER NOT NULL PRIMARY KEY,
        org_id INTEGER NOT NULL,
        item_type TEXT NOT NULL,
        season INTEGER,
        position INTEGER NOT NULL DEFAULT 0
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT COALESCE(MAX(position), 0) + 1 AS next_position FROM items WHERE org_id = @org_id AND item_type = @item_type AND season IS @season",
    )
  result |> string.inspect |> birdie.snap(title: "coalesce max plus literal returns int")
}

// ---- SELECT DISTINCT ----

pub fn introspect_select_distinct_single_column_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, status TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec("INSERT INTO t (id, status) VALUES (1, 'active'), (2, 'active')", on: db)
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT DISTINCT status FROM t")
  result |> string.inspect |> birdie.snap(title: "select distinct single column")
}

pub fn introspect_select_distinct_multiple_columns_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, status TEXT NOT NULL, priority INTEGER NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT DISTINCT status, priority FROM t")
  result |> string.inspect |> birdie.snap(title: "select distinct multiple columns")
}

pub fn introspect_select_distinct_with_where_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, status TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT DISTINCT status FROM t WHERE id > ?",
    )
  result |> string.inspect |> birdie.snap(title: "select distinct with where param")
}

pub fn introspect_select_distinct_with_order_by_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT DISTINCT name FROM t ORDER BY name",
    )
  result |> string.inspect |> birdie.snap(title: "select distinct with order by")
}

// ---- GROUP BY ----

pub fn introspect_group_by_with_count_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, status TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT status, COUNT(*) AS cnt FROM t GROUP BY status",
    )
  result |> string.inspect |> birdie.snap(title: "group by with count")
}

pub fn introspect_group_by_multiple_columns_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, region TEXT NOT NULL, category TEXT NOT NULL, amount INTEGER NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT region, category, SUM(amount) AS total FROM t GROUP BY region, category",
    )
  result |> string.inspect |> birdie.snap(title: "group by multiple columns")
}

pub fn introspect_group_by_with_where_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, status TEXT NOT NULL, count INTEGER NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT status, SUM(count) AS total FROM t WHERE status = ? GROUP BY status",
    )
  result |> string.inspect |> birdie.snap(title: "group by with where param")
}

pub fn introspect_group_by_with_having_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, status TEXT NOT NULL, count INTEGER NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT status, SUM(count) AS total FROM t GROUP BY status HAVING SUM(count) > ?",
    )
  result |> string.inspect |> birdie.snap(title: "group by with having")
}

pub fn introspect_group_by_with_multiple_aggregates_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, grp TEXT NOT NULL, val INTEGER NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT grp, COUNT(*) AS cnt, AVG(val) AS avg_val, MAX(val) AS max_val FROM t GROUP BY grp",
    )
  result |> string.inspect |> birdie.snap(title: "group by with multiple aggregates")
}

// ---- UNION / INTERSECT / EXCEPT ----

pub fn introspect_union_select_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE active (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE archived (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id, name FROM active UNION SELECT id, name FROM archived",
    )
  result |> string.inspect |> birdie.snap(title: "union select")
}

pub fn introspect_union_all_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id, val FROM t1 UNION ALL SELECT id, val FROM t2",
    )
  result |> string.inspect |> birdie.snap(title: "union all")
}

pub fn introspect_union_with_where_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id, name FROM t1 WHERE name = ? UNION SELECT id, name FROM t2 WHERE name = ?",
    )
  result |> string.inspect |> birdie.snap(title: "union with where param")
}

pub fn introspect_intersect_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id FROM t1 INTERSECT SELECT id FROM t2",
    )
  result |> string.inspect |> birdie.snap(title: "intersect")
}

pub fn introspect_except_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id FROM t1 EXCEPT SELECT id FROM t2",
    )
  result |> string.inspect |> birdie.snap(title: "except")
}

pub fn introspect_compound_query_as_subquery_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id, name FROM (SELECT id, name FROM t1 UNION SELECT id, name FROM t2) WHERE name = ?",
    )
  result |> string.inspect |> birdie.snap(title: "compound query as subquery")
}

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
      db,
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
      db,
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
      db,
      "INSERT INTO t (id, val, counter) VALUES (@id, @val, @counter) ON CONFLICT(id) DO UPDATE SET val = @val, counter = @counter",
    )
  result |> string.inspect |> birdie.snap(title: "upsert do update with named params")
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
      db,
      "INSERT INTO t (id, val, version) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET val = ?, version = ? WHERE version < ? RETURNING id, val, version",
    )
  result |> string.inspect |> birdie.snap(title: "upsert do update set with where")
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
      db,
      "INSERT INTO t (id, val) VALUES (?, ?) ON CONFLICT(id) DO NOTHING",
    )
  result |> string.inspect |> birdie.snap(title: "upsert do nothing no returning")
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
      db,
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
      db,
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
    sqlite.introspect_query(
      db,
      "SELECT id, name FROM users WHERE name LIKE ?",
    )
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
    sqlite.introspect_query(
      db,
      "SELECT id FROM users WHERE name LIKE @pattern",
    )
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
    sqlite.introspect_query(db, "SELECT id, name FROM users WHERE deleted_at IS NULL")
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
    sqlite.introspect_query(db, "SELECT id, name FROM users WHERE email IS NOT NULL")
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
      db,
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
      db,
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
      db,
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
    sqlite.introspect_query(db, "SELECT a.id, b.id FROM a CROSS JOIN b")
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
      db,
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
      db,
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
    sqlite.introspect_query(
      db,
      "REPLACE INTO users (id, name) VALUES (?, ?)",
    )
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
    sqlight.exec(
      "CREATE TABLE deleted (user_id INTEGER NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
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
    sqlite.introspect_query(
      db,
      "SELECT id FROM users WHERE name NOT LIKE ?",
    )
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
      db,
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
      db,
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
      db,
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
  let result = sqlite.introspect_query(db, "THIS IS NOT VALID SQL")
  let assert Error(_) = result
}

pub fn introspect_query_empty_sql_test() {
  use db <- sqlight.with_connection(":memory:")
  let result = sqlite.introspect_query(db, "")
  let assert Error(_) = result
}

pub fn parse_returns_annotation_invalid_test() {
  let assert Error(_) = sqlite.parse_returns_annotation(
    "-- returns: NotARowType\nSELECT 1",
  )
}

pub fn parse_returns_annotation_valid_test() {
  let assert Ok(option.Some("OrgRow")) = sqlite.parse_returns_annotation(
    "-- returns: OrgRow\nSELECT id FROM orgs",
  )
}

pub fn parse_returns_annotation_missing_test() {
  let assert Ok(option.None) =
    sqlite.parse_returns_annotation("SELECT id FROM orgs")
}

// ---- INSERT / REPLACE table name extraction (marmot-b10o) ----
// SQLite requires INTO for INSERT/REPLACE statements. These tests verify
// parse_insert_table_name correctly handles the INSERT [OR ...] [INTO] prefix.

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
      db,
      "INSERT OR REPLACE INTO users (id, name) VALUES (?, ?)",
    )
  result |> string.inspect |> birdie.snap(title: "insert or replace into table name")
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
    sqlite.introspect_query(
      db,
      "REPLACE INTO users (id, name) VALUES (?, ?)",
    )
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
      db,
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
      db,
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
      db,
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
      db,
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
    sqlite.introspect_query(
      db,
      "SELECT a.id, a.val_a FROM a NATURAL JOIN b",
    )
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
      db,
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
      db,
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
    sqlite.introspect_query(
      db,
      "SELECT id, name FROM users LIMIT ?",
    )
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
    sqlite.introspect_query(
      db,
      "SELECT id, name FROM users LIMIT ? OFFSET ?",
    )
  result.parameters |> string.inspect |> birdie.snap(title: "limit offset param types are int")
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
      db,
      "SELECT id, name FROM users LIMIT @limit OFFSET @offset",
    )
  result.parameters |> string.inspect |> birdie.snap(title: "limit named param type is int")
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
      db,
      "SELECT region, SUM(amount) AS total FROM sales GROUP BY region HAVING SUM(amount) > @min_amount",
    )
  result |> string.inspect |> birdie.snap(title: "having named param infers column type")
}
