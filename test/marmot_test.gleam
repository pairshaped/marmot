import birdie
import gleam/int
import gleam/list
import gleam/option
import gleam/string
import gleeunit
import marmot
import marmot/internal/codegen
import marmot/internal/query
import marmot/internal/sqlite
import sqlight

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn end_to_end_shared_row_types_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orgs (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )

  let sql_1 = "-- returns: OrgRow\nSELECT id, name FROM orgs WHERE id = @id"
  let sql_2 = "-- returns: OrgRow\nSELECT id, name FROM orgs"
  let sql_3 = "SELECT id FROM orgs"

  let assert Ok(info_1) = sqlite.introspect_query(db, sql_1)
  let assert Ok(info_2) = sqlite.introspect_query(db, sql_2)
  let assert Ok(info_3) = sqlite.introspect_query(db, sql_3)

  let assert Ok(option.Some("OrgRow")) = sqlite.parse_returns_annotation(sql_1)
  let assert Ok(option.Some("OrgRow")) = sqlite.parse_returns_annotation(sql_2)
  let assert Ok(option.None) = sqlite.parse_returns_annotation(sql_3)

  let queries = [
    query.Query(
      name: "get_org",
      sql: sql_1,
      path: "a.sql",
      parameters: info_1.parameters,
      columns: info_1.columns,
      custom_type_name: option.Some("OrgRow"),
    ),
    query.Query(
      name: "list_orgs",
      sql: sql_2,
      path: "b.sql",
      parameters: info_2.parameters,
      columns: info_2.columns,
      custom_type_name: option.Some("OrgRow"),
    ),
    query.Query(
      name: "count_orgs",
      sql: sql_3,
      path: "c.sql",
      parameters: info_3.parameters,
      columns: info_3.columns,
      custom_type_name: option.None,
    ),
  ]

  let assert Ok(output) =
    codegen.generate_module_with_config(queries, option.None)

  // Shared type emitted exactly once.
  let assert 1 = count_substring(output, "pub type OrgRow {")
  // Shared decoder emitted exactly once.
  let assert 1 = count_substring(output, "fn org_row_decoder()")
  // Unannotated query keeps its per-query type.
  let assert 1 = count_substring(output, "pub type CountOrgsRow {")
  // Annotated queries return the shared type.
  let assert True =
    string.contains(output, "Result(List(OrgRow), sqlight.Error)")
  // Annotated query functions reference the shared decoder.
  let assert True = string.contains(output, "expecting: org_row_decoder()")
}

fn count_substring(haystack: String, needle: String) -> Int {
  string.split(haystack, needle)
  |> list.length
  |> int.subtract(1)
}

pub fn snapshot_shared_row_types_output_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orgs (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        bio TEXT
      )",
      on: db,
    )

  let sql_1 =
    "-- returns: OrgRow\nSELECT id, name, bio FROM orgs WHERE id = @id"
  let sql_2 = "-- returns: OrgRow\nSELECT id, name, bio FROM orgs"

  let assert Ok(info_1) = sqlite.introspect_query(db, sql_1)
  let assert Ok(info_2) = sqlite.introspect_query(db, sql_2)

  let queries = [
    query.Query(
      name: "get_org",
      sql: sql_1,
      path: "a.sql",
      parameters: info_1.parameters,
      columns: info_1.columns,
      custom_type_name: option.Some("OrgRow"),
    ),
    query.Query(
      name: "list_orgs",
      sql: sql_2,
      path: "b.sql",
      parameters: info_2.parameters,
      columns: info_2.columns,
      custom_type_name: option.Some("OrgRow"),
    ),
  ]

  let assert Ok(output) =
    codegen.generate_module_with_config(queries, option.None)
  output
  |> birdie.snap(title: "shared_row_types_two_queries")
}

// ---- marmot module unit tests ----

pub fn validate_sql_empty_test() {
  let assert Error(Nil) = marmot.validate_sql("", "test.sql")
}

pub fn validate_sql_valid_test() {
  let assert Ok("SELECT 1") = marmot.validate_sql("SELECT 1", "test.sql")
}

pub fn validate_sql_trailing_semicolon_test() {
  let assert Ok("SELECT 1") = marmot.validate_sql("SELECT 1;", "test.sql")
}

pub fn validate_sql_multiple_statements_test() {
  let assert Error(Nil) =
    marmot.validate_sql("SELECT 1; SELECT 2", "test.sql")
}

pub fn validate_sql_semicolon_in_string_test() {
  // Semicolons inside string literals should not trigger the multiple-queries check
  let assert Ok("SELECT 'hello;world'") =
    marmot.validate_sql("SELECT 'hello;world'", "test.sql")
}

pub fn validate_sql_semicolon_in_comment_test() {
  let assert Ok("SELECT 1 -- comment; still a comment") =
    marmot.validate_sql("SELECT 1 -- comment; still a comment", "test.sql")
}

pub fn contains_semicolon_outside_strings_none_test() {
  let assert False = marmot.contains_semicolon_outside_strings("SELECT 1")
}

pub fn contains_semicolon_outside_strings_simple_test() {
  let assert True = marmot.contains_semicolon_outside_strings("SELECT 1; SELECT 2")
}

pub fn contains_semicolon_outside_strings_in_string_test() {
  let assert False =
    marmot.contains_semicolon_outside_strings("SELECT 'hello;world'")
}

pub fn contains_semicolon_outside_strings_in_double_quoted_test() {
  let assert False =
    marmot.contains_semicolon_outside_strings("SELECT \"hello;world\"")
}

pub fn contains_semicolon_outside_strings_in_line_comment_test() {
  let assert False =
    marmot.contains_semicolon_outside_strings("SELECT 1 -- a; comment")
}

pub fn contains_semicolon_outside_strings_in_block_comment_test() {
  let assert False =
    marmot.contains_semicolon_outside_strings("SELECT /* a; comment */ 1")
}

pub fn check_duplicate_columns_no_duplicates_test() {
  let cols = [
    query.Column(name: "id", column_type: query.IntType, nullable: False),
    query.Column(name: "name", column_type: query.StringType, nullable: True),
  ]
  let assert Ok(Nil) = marmot.check_duplicate_columns(cols, "test.sql")
}

pub fn check_duplicate_columns_has_duplicates_test() {
  let cols = [
    query.Column(name: "id", column_type: query.IntType, nullable: False),
    query.Column(name: "name", column_type: query.StringType, nullable: True),
    query.Column(name: "id", column_type: query.IntType, nullable: False),
  ]
  let assert Error(Nil) = marmot.check_duplicate_columns(cols, "test.sql")
}

pub fn check_duplicate_columns_empty_test() {
  let assert Ok(Nil) = marmot.check_duplicate_columns([], "test.sql")
}

pub fn check_duplicate_columns_single_test() {
  let cols = [
    query.Column(name: "id", column_type: query.IntType, nullable: False),
  ]
  let assert Ok(Nil) = marmot.check_duplicate_columns(cols, "test.sql")
}
