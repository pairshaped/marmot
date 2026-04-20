import gleam/list
import marmot/internal/query.{
  BitArrayType, BoolType, Column, DateType, FloatType, IntType, Parameter,
  StringType, TimestampType,
}
import marmot/internal/sqlite
import sqlight

pub fn introspect_integer_column_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)", on: db)
  let assert Ok(columns) = sqlite.introspect_columns(db, "t")
  let assert [Column(name: "id", column_type: IntType, nullable: False)] =
    columns
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
  let assert 7 = list.length(columns)
  let assert [
    Column(name: "a", column_type: IntType, nullable: False),
    Column(name: "b", column_type: FloatType, nullable: False),
    Column(name: "c", column_type: StringType, nullable: False),
    Column(name: "d", column_type: BitArrayType, nullable: False),
    Column(name: "e", column_type: BoolType, nullable: False),
    Column(name: "f", column_type: TimestampType, nullable: False),
    Column(name: "g", column_type: DateType, nullable: False),
  ] = columns
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
  let assert [
    Column(name: "a", column_type: IntType, nullable: False),
    Column(name: "b", column_type: StringType, nullable: True),
  ] = columns
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
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "username", column_type: StringType, nullable: False),
  ] = result.columns
  let assert [
    Parameter(name: "email", column_type: StringType, nullable: False),
  ] = result.parameters
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
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "created_at", column_type: TimestampType, nullable: False),
  ] = result.columns
  let assert [
    Parameter(name: "username", column_type: StringType, nullable: False),
    Parameter(name: "created_at", column_type: TimestampType, nullable: False),
  ] = result.parameters
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
  let assert [] = result.columns
  let assert [Parameter(name: "id", column_type: IntType, nullable: False)] =
    result.parameters
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
  let assert [] = result.parameters
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "name", column_type: StringType, nullable: False),
  ] = result.columns
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
  let assert [
    Parameter(name: "name", column_type: StringType, nullable: False),
    Parameter(name: "age", column_type: IntType, nullable: False),
  ] = result.parameters
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
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "bio", column_type: StringType, nullable: True),
  ] = result.columns
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
  let assert [] = result.columns
  let assert [
    Parameter(name: "name", column_type: StringType, nullable: False),
    Parameter(name: "id", column_type: IntType, nullable: False),
  ] = result.parameters
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
  // EXPLAIN-based inference works: finds the comparison context
  let assert [Column(name: "id", ..), Column(name: "name", ..)] = result.columns
  let assert [
    Parameter(name: "reason", column_type: StringType, nullable: False),
  ] = result.parameters
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
  let assert 2 = list.length(result.parameters)
  let assert [
    Parameter(name: "name", column_type: StringType, nullable: False),
    Parameter(name: "age", column_type: IntType, nullable: False),
  ] = result.parameters
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
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "updated_at", column_type: TimestampType, nullable: False),
  ] = result.columns
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
  // RETURNING * should expand to all table columns
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "name", column_type: StringType, nullable: False),
    Column(name: "email", column_type: StringType, nullable: False),
  ] = result.columns
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
  // Should deduplicate: age, age_2 instead of age, age
  let assert [Parameter(name: "age", ..), Parameter(name: "age_2", ..)] =
    result.parameters
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
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "name", column_type: StringType, nullable: False),
  ] = result.columns
  let assert 1 = list.length(result.parameters)
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
  let assert [
    Parameter(name: "name", column_type: StringType, nullable: False),
    Parameter(name: "id", column_type: IntType, nullable: False),
  ] = result.parameters
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
  // Aliases should preserve original case, not be uppercased
  let assert [Column(name: "userId", ..), Column(name: "userName", ..)] =
    result.columns
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
  let assert [
    Parameter(name: "value", column_type: FloatType, nullable: False),
    Parameter(name: "id", column_type: IntType, nullable: False),
  ] = result.parameters
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
  let assert [
    Column(name: "name", column_type: StringType, nullable: False),
    Column(name: "title", column_type: StringType, nullable: False),
  ] = result.columns
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
  let assert [
    Column(name: "has_feature", column_type: IntType, nullable: False),
  ] = result.columns
  let assert [
    Parameter(name: "item_id", column_type: IntType, nullable: False),
    Parameter(name: "field_key", column_type: StringType, nullable: False),
  ] = result.parameters
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
  // u.id and u.name come from the primary (non-nullable) side
  // p.bio comes from the LEFT JOINed (nullable) side
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "name", column_type: StringType, nullable: False),
    Column(name: "bio", column_type: StringType, nullable: True),
  ] = result.columns
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
  let assert [
    Column(name: "name", column_type: StringType, nullable: False),
    Column(name: "bio", column_type: StringType, nullable: True),
  ] = result.columns
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
  // INNER JOIN: both sides are non-nullable (NOT NULL in schema)
  let assert [
    Column(name: "name", column_type: StringType, nullable: False),
    Column(name: "bio", column_type: StringType, nullable: False),
  ] = result.columns
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
  // a is primary (non-nullable); b and c are LEFT JOINed (both nullable)
  let assert [
    Column(name: "a_val", column_type: StringType, nullable: False),
    Column(name: "b_val", column_type: StringType, nullable: True),
    Column(name: "c_val", column_type: StringType, nullable: True),
  ] = result.columns
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
  let assert [Column(name: "bio", column_type: StringType, nullable: False)] =
    result.columns
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
  // a and b are INNER JOINed (non-nullable); c is LEFT JOINed (nullable)
  let assert [
    Column(name: "a_val", column_type: StringType, nullable: False),
    Column(name: "b_val", column_type: StringType, nullable: False),
    Column(name: "c_val", column_type: StringType, nullable: True),
  ] = result.columns
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
  // The window function column is always IntType, non-nullable.
  // Note: the opcode tracer marks `id` as nullable when window function opcodes
  // are present — this is a known limitation of opcode tracing with OVER clauses.
  let assert [
    Column(name: "id", column_type: IntType, nullable: True),
    Column(name: "position", column_type: IntType, nullable: False),
  ] = result.columns
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
  // The window function column is always IntType, non-nullable.
  // Note: the opcode tracer marks `id` as nullable when window function opcodes
  // are present — this is a known limitation of opcode tracing with OVER clauses.
  let assert [
    Column(name: "id", column_type: IntType, nullable: True),
    Column(name: "rk", column_type: IntType, nullable: False),
  ] = result.columns
}
