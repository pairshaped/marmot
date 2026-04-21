import gleam/list
import gleam/option
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
  let assert [
    Parameter(name: "username", column_type: StringType, nullable: False),
    Parameter(name: "bio", column_type: StringType, nullable: True),
    Parameter(name: "created_at", column_type: IntType, nullable: False),
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
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
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
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "rk", column_type: IntType, nullable: False),
  ] = result.columns
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
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "name_en", column_type: StringType, nullable: False),
    Column(name: "name_fr", column_type: StringType, nullable: False),
  ] = result.columns
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
  let assert [Column(name: "name", column_type: StringType, nullable: False)] =
    result.columns
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
  let assert [Column(name: "name", column_type: StringType, nullable: True)] =
    result.columns
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
  let assert [
    Parameter(name: "gender", column_type: StringType, nullable: True),
    Parameter(name: "birthdate", column_type: StringType, nullable: True),
    Parameter(name: "updated_at", column_type: IntType, nullable: False),
    Parameter(name: "id", column_type: IntType, nullable: False),
  ] = result.parameters
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
  let assert [
    Parameter(name: "order_id", column_type: IntType, nullable: False),
  ] = result.parameters
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
  let assert [
    Parameter(name: "question_name", column_type: StringType, nullable: True),
    Parameter(name: "question_key", column_type: StringType, nullable: False),
    Parameter(name: "org_id", column_type: IntType, nullable: False),
  ] = result.parameters
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
  let assert [
    Parameter(name: "item_id", column_type: IntType, nullable: False),
    Parameter(name: "created_at", column_type: IntType, nullable: False),
    Parameter(name: "source_item_id", column_type: IntType, nullable: False),
  ] = result.parameters
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
  // Types fall back to String/nullable because CTE columns can't be traced
  let assert [
    Column(name: "id", column_type: StringType, nullable: True),
    Column(name: "user_id", column_type: StringType, nullable: True),
    Column(name: "total", column_type: StringType, nullable: True),
  ] = result.columns
  let assert 2 = list.length(result.parameters)
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
  // Recursive CTEs also lose type info
  let assert [
    Column(name: "id", column_type: StringType, nullable: True),
    Column(name: "name", column_type: StringType, nullable: True),
    Column(name: "depth", column_type: StringType, nullable: True),
  ] = result.columns
  let assert 1 = list.length(result.parameters)
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
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "name", column_type: StringType, nullable: False),
    // Subquery result type can't be traced through EXPLAIN
    Column(name: "order_count", column_type: StringType, nullable: True),
  ] = result.columns
  let assert 1 = list.length(result.parameters)
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
  let assert [
    Column(name: "user_name", column_type: StringType, nullable: False),
    Column(name: "status", column_type: StringType, nullable: False),
    Column(name: "product_name", column_type: StringType, nullable: False),
    Column(name: "qty", column_type: IntType, nullable: False),
  ] = result.columns
  let assert 1 = list.length(result.parameters)
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
  let assert [
    Column(name: "name", column_type: StringType, nullable: False),
    Column(name: "bio", column_type: StringType, nullable: False),
    Column(name: "avatar_url", column_type: StringType, nullable: True),
  ] = result.columns
  let assert 1 = list.length(result.parameters)
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
  let assert [Column(name: "registered", column_type: IntType, nullable: False)] =
    result.columns
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
  let assert [Column(name: "label", column_type: StringType, nullable: False)] =
    result.columns
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
  let assert [Column(name: "maybe_val", column_type: IntType, nullable: True)] =
    result.columns
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
  let assert [Column(name: "maybe_val", column_type: IntType, nullable: True)] =
    result.columns
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
  let assert [Column(name: "mixed", column_type: StringType, nullable: True)] =
    result.columns
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
  // The inner CASE is a non-literal expression, but "2" is a literal.
  // Since the inner CASE can't be resolved by infer_literal_type, it falls back.
  let assert [Column(name: "val", column_type: StringType, nullable: True)] =
    result.columns
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
  let assert [Column(name: "label", column_type: StringType, nullable: False)] =
    result.columns
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
  // Column refs fall through infer_expression_type, but the opcode path
  // traces the result back to the source column, resolving correctly.
  let assert [Column(name: "val", column_type: StringType, nullable: False)] =
    result.columns
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
  let assert [
    Parameter(name: "claimed_at", column_type: IntType, nullable: True),
    Parameter(name: "updated_at", column_type: IntType, nullable: False),
    Parameter(name: "item_id", column_type: IntType, nullable: False),
    Parameter(name: "account_id", column_type: IntType, nullable: False),
    Parameter(name: "org_id", column_type: IntType, nullable: False),
  ] = result.parameters
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
  let assert [Column(name: "registered", column_type: IntType, nullable: False)] =
    result.columns
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
  // Only 2 params: the string literal 'hello, world' is not a placeholder
  let assert [
    Parameter(name: "id", column_type: IntType, nullable: False),
    Parameter(name: "body", column_type: StringType, nullable: False),
  ] = result.parameters
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
  let assert [
    Parameter(name: "id", column_type: IntType, nullable: False),
  ] = result.parameters
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
  let assert [
    Parameter(name: "email", column_type: StringType, nullable: False),
  ] = result.parameters
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
  let assert [
    Parameter(name: "status", column_type: StringType, nullable: False),
  ] = result.parameters
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
  let assert [
    Column(name: "display_name", column_type: StringType, ..),
  ] = result.columns
  let assert [
    Parameter(name: "id", column_type: IntType, nullable: False),
  ] = result.parameters
}
