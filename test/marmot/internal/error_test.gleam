import birdie
import gleam/option
import marmot/internal/error

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
  error.DuplicateColumns(
    path: "src/app/sql/find_user.sql",
    columns: ["id", "name"],
  )
  |> error.to_string
  |> birdie.snap(title: "duplicate columns error")
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
