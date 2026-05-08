import birdie
import gleam/list
import gleam/string
import marmot/internal/sqlite
import sqlight

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
  result
  |> string.inspect
  |> birdie.snap(title: "update with eq subquery multiple named params")
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
  result
  |> string.inspect
  |> birdie.snap(title: "insert values with string containing comma")
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
  result
  |> string.inspect
  |> birdie.snap(title: "insert values with escaped quote in string")
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
  result
  |> string.inspect
  |> birdie.snap(title: "where with string containing and")
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
  result
  |> string.inspect
  |> birdie.snap(title: "where with string containing or")
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
  result
  |> string.inspect
  |> birdie.snap(title: "select with string literal containing comma")
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
  result
  |> string.inspect
  |> birdie.snap(title: "keyword inside string literal")
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
  result
  |> string.inspect
  |> birdie.snap(title: "double quoted identifier containing placeholder")
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
  result
  |> string.inspect
  |> birdie.snap(title: "case with string containing end")
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
  result
  |> string.inspect
  |> birdie.snap(title: "placeholder after escaped quote")
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
  result
  |> string.inspect
  |> birdie.snap(title: "string literal containing close paren")
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
  result
  |> string.inspect
  |> birdie.snap(title: "keyword in subquery not matched")
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
  result
  |> string.inspect
  |> birdie.snap(title: "double quoted keyword identifier in where")
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
  result
  |> string.inspect
  |> birdie.snap(title: "cast subquery column as integer")
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
  result
  |> string.inspect
  |> birdie.snap(title: "between named params infer column type")
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
  result
  |> string.inspect
  |> birdie.snap(title: "coalesce max plus literal returns int")
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
    sqlight.exec(
      "INSERT INTO t (id, status) VALUES (1, 'active'), (2, 'active')",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT DISTINCT status FROM t")
  result
  |> string.inspect
  |> birdie.snap(title: "select distinct single column")
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
  result
  |> string.inspect
  |> birdie.snap(title: "select distinct multiple columns")
}

pub fn introspect_select_distinct_with_where_param_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, status TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT DISTINCT status FROM t WHERE id > ?")
  result
  |> string.inspect
  |> birdie.snap(title: "select distinct with where param")
}

pub fn introspect_select_distinct_with_order_by_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT DISTINCT name FROM t ORDER BY name")
  result
  |> string.inspect
  |> birdie.snap(title: "select distinct with order by")
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
  result
  |> string.inspect
  |> birdie.snap(title: "group by with multiple aggregates")
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
    sqlight.exec("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY)", on: db)
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY)", on: db)
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT id FROM t1 INTERSECT SELECT id FROM t2")
  result |> string.inspect |> birdie.snap(title: "intersect")
}

pub fn introspect_except_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY)", on: db)
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY)", on: db)
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT id FROM t1 EXCEPT SELECT id FROM t2")
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
