import birdie
import gleam/option
import gleam/string
import marmot/internal/error.{SharedTypeMismatch}
import marmot/internal/query.{Column, IntType, StringType}

pub fn database_not_configured_error_test() {
  error.DatabaseNotConfigured
  |> error.to_string
  |> birdie.snap(title: "database not configured error")
}

pub fn file_read_error_test() {
  error.FileReadError(
    path: "src/app/sql/find_user.sql",
    message: "No such file",
  )
  |> error.to_string
  |> birdie.snap(title: "file read error")
}

pub fn sql_error_test() {
  error.SqlError(
    path: "src/app/sql/find_user.sql",
    message: "near \"SELEC\": syntax error",
  )
  |> error.to_string
  |> birdie.snap(title: "sql syntax error")
}

pub fn unknown_column_error_test() {
  error.UnknownColumn(
    path: "src/app/sql/find_user.sql",
    column: "naem",
    table: "users",
    suggestion: option.Some("name"),
  )
  |> error.to_string
  |> birdie.snap(title: "unknown column error with suggestion")
}

pub fn unknown_column_no_suggestion_error_test() {
  error.UnknownColumn(
    path: "src/app/sql/find_user.sql",
    column: "xyz",
    table: "users",
    suggestion: option.None,
  )
  |> error.to_string
  |> birdie.snap(title: "unknown column error without suggestion")
}

pub fn type_inference_error_test() {
  error.TypeInferenceError(
    path: "src/app/sql/complex.sql",
    expression: "coalesce(a.foo, b.bar)",
  )
  |> error.to_string
  |> birdie.snap(title: "type inference error")
}

pub fn empty_sql_file_error_test() {
  error.EmptySqlFile(path: "src/app/sql/empty.sql")
  |> error.to_string
  |> birdie.snap(title: "empty sql file error")
}

pub fn multiple_queries_error_test() {
  error.MultipleQueries(path: "src/app/sql/multi.sql")
  |> error.to_string
  |> birdie.snap(title: "multiple queries error")
}

pub fn stale_generated_code_error_test() {
  error.StaleGeneratedCode(files: ["src/app/sql.gleam", "src/other/sql.gleam"])
  |> error.to_string
  |> birdie.snap(title: "stale generated code error")
}

pub fn database_open_error_test() {
  error.DatabaseOpenError(
    path: "/tmp/missing.sqlite",
    message: "unable to open database file",
  )
  |> error.to_string
  |> birdie.snap(title: "database open error")
}

pub fn duplicate_columns_error_test() {
  error.DuplicateColumns(path: "src/app/sql/find_user.sql", columns: [
    "id",
    "name",
  ])
  |> error.to_string
  |> birdie.snap(title: "duplicate columns error")
}

pub fn sql_error_row_value_hint_test() {
  error.SqlError(
    path: "src/app/sql/find_user.sql",
    message: "row value misused",
  )
  |> error.to_string
  |> birdie.snap(title: "sql error with row value hint")
}

pub fn sql_error_no_such_table_hint_test() {
  error.SqlError(
    path: "src/app/sql/find_user.sql",
    message: "no such table: users",
  )
  |> error.to_string
  |> birdie.snap(title: "sql error with no such table hint")
}

pub fn unknown_table_error_test() {
  error.UnknownTable(
    path: "src/app/sql/find_user.sql",
    table: "usres",
    suggestion: option.Some("users"),
  )
  |> error.to_string
  |> birdie.snap(title: "unknown table error with suggestion")
}

pub fn unknown_table_no_suggestion_error_test() {
  error.UnknownTable(
    path: "src/app/sql/find_user.sql",
    table: "xyz",
    suggestion: option.None,
  )
  |> error.to_string
  |> birdie.snap(title: "unknown table error without suggestion")
}

pub fn invalid_returns_annotation_error_test() {
  error.InvalidReturnsAnnotation(
    path: "src/server/orgs/sql/get_org.sql",
    name: "Org",
    reason: "type name must end with `Row` (e.g., `OrgRow`)",
  )
  |> error.to_string
  |> birdie.snap(title: "invalid returns annotation error")
}

pub fn shared_type_mismatch_message_test() {
  let conflict_a = #("src/server/orgs/sql/get_org_by_id.sql", [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "name", column_type: StringType, nullable: False),
  ])
  let conflict_b = #("src/server/orgs/sql/list_orgs.sql", [
    Column(name: "id", column_type: IntType, nullable: False),
  ])
  let msg =
    error.to_string(
      SharedTypeMismatch(name: "OrgRow", conflicts: [conflict_a, conflict_b]),
    )
  let assert True = string.contains(msg, "OrgRow")
  let assert True = string.contains(msg, "get_org_by_id.sql")
  let assert True = string.contains(msg, "list_orgs.sql")
}
