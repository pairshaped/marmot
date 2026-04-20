import birdie
import gleam/int
import gleam/list
import gleam/option
import gleam/string
import gleeunit
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

  let output = codegen.generate_module_with_config(queries, option.None)

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

  codegen.generate_module_with_config(queries, option.None)
  |> birdie.snap(title: "shared_row_types_two_queries")
}
