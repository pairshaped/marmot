import gleam/bool
import gleam/list
import gleam/string
import marmot/internal/query.{type Column}

pub type MarmotError {
  DatabaseNotConfigured
  DatabaseOpenError(path: String, message: String)
  FileReadError(path: String, message: String)
  SqlError(path: String, message: String)
  EmptySqlFile(path: String)
  InvalidFilename(path: String)
  MultipleQueries(path: String)
  DuplicateColumns(path: String, columns: List(String))
  OutputNotUnderSrc(output: String)
  InvalidReturnsAnnotation(path: String, name: String, reason: String)
  SharedTypeMismatch(name: String, conflicts: List(#(String, List(Column))))
}

pub fn to_string(error: MarmotError) -> String {
  case error {
    DatabaseNotConfigured ->
      "error: Could not connect to SQLite database
  No database configured. Set one of:
    \u{2022} DATABASE_URL environment variable
    \u{2022} database field in [tools.marmot] section of gleam.toml
    \u{2022} --database flag"

    DatabaseOpenError(path:, message:) -> "error: Could not open SQLite database
  Path: " <> path <> "
  " <> message

    FileReadError(path:, message:) -> "error: Could not read file
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message

    SqlError(path:, message:) -> {
      let hint = sql_error_hint(message)
      "error: SQL error
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message <> hint
    }

    InvalidFilename(path:) -> "error: Invalid filename
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Filename produces no valid Gleam identifier
  \u{2502}
  hint: Rename the file to use letters, digits, or underscores"

    EmptySqlFile(path:) -> "error: Empty SQL file
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} This file contains no SQL query"

    MultipleQueries(path:) -> "error: Multiple queries in one file
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Each .sql file must contain exactly one query
  \u{2502}
  hint: Split this file into separate .sql files, one query per file"

    DuplicateColumns(path:, columns:) -> {
      let col_list =
        columns
        |> list.map(fn(c) { "\"" <> c <> "\"" })
        |> string.join(", ")
      "error: Duplicate column names
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Query returns duplicate column names: " <> col_list <> "
  \u{2502}
  hint: Use column aliases to give each column a unique name:
        SELECT a.id AS a_id, b.id AS b_id ..."
    }

    OutputNotUnderSrc(output:) -> "error: Output directory must be under src/
  \u{250c}\u{2500} gleam.toml
  \u{2502}
  \u{2502} output = \"" <> output <> "\"
  \u{2502}
  hint: Gleam compiles modules from src/, so generated code must live there.
        For example: output = \"src/generated/sql\""

    InvalidReturnsAnnotation(path:, name:, reason:) ->
      "error: Invalid -- returns: annotation
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} `-- returns: " <> name <> "` is invalid: " <> reason

    SharedTypeMismatch(name:, conflicts:) -> {
      let header =
        "error: Shared return type mismatch for `" <> name <> "`\n  \u{2502}\n"
      let body =
        list.map(conflicts, fn(pair) {
          let #(path, cols) = pair
          let shape =
            list.map(cols, fn(c) {
              c.name
              <> ": "
              <> query.gleam_type(c.column_type)
              <> case c.nullable {
                True -> "?"
                False -> ""
              }
            })
            |> string.join(", ")
          "  \u{2502} " <> path <> "\n  \u{2502}   returns: [" <> shape <> "]"
        })
        |> string.join("\n  \u{2502}\n")
      header
      <> body
      <> "\n  \u{2502}\n  \u{2502} Align SELECT columns across these queries, or remove the annotation from one."
    }
  }
}

fn sql_error_hint(message: String) -> String {
  use <- bool.guard(
    string.contains(message, "row value misused"),
    "
  \u{2502}
  hint: Did you accidentally parenthesize your SELECT columns?
        Write: SELECT id, name FROM ...
        Not:   SELECT (id, name) FROM ...",
  )
  use <- bool.guard(
    string.contains(message, "no such table"),
    "
  \u{2502}
  hint: Make sure the database file contains your schema.
        Marmot needs the tables to exist so it can infer types.",
  )
  use <- bool.guard(
    string.contains(message, "no such column"),
    "
  \u{2502}
  hint: Check that the column name matches your schema exactly.
        Column names are case-sensitive in some contexts.",
  )
  ""
}
