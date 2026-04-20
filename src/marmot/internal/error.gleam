import gleam/list
import gleam/option.{type Option}
import gleam/string

pub type MarmotError {
  DatabaseNotConfigured
  DatabaseOpenError(path: String, message: String)
  FileReadError(path: String, message: String)
  SqlError(path: String, message: String)
  UnknownColumn(
    path: String,
    column: String,
    table: String,
    suggestion: Option(String),
  )
  UnknownTable(path: String, table: String, suggestion: Option(String))
  TypeInferenceError(path: String, expression: String)
  EmptySqlFile(path: String)
  InvalidFilename(path: String)
  MultipleQueries(path: String)
  DuplicateColumns(path: String, columns: List(String))
  StaleGeneratedCode(files: List(String))
  OutputNotUnderSrc(output: String)
}

pub fn to_string(error: MarmotError) -> String {
  case error {
    DatabaseNotConfigured ->
      "error: Could not connect to SQLite database
  No database configured. Set one of:
    \u{2022} DATABASE_URL environment variable
    \u{2022} database field in [marmot] section of gleam.toml
    \u{2022} --database flag"

    DatabaseOpenError(path:, message:) -> "error: Could not open SQLite database
  Path: " <> path <> "
  " <> message

    FileReadError(path:, message:) -> "error: Could not read file
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message

    SqlError(path:, message:) -> "error: SQL error
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message

    UnknownColumn(path:, column:, table:, suggestion:) -> {
      let hint = case suggestion {
        option.Some(s) -> "
  hint: Did you mean \"" <> s <> "\"?"
        option.None -> ""
      }
      "error: Unknown column
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Column \"" <> column <> "\" does not exist on table \"" <> table <> "\"" <> hint
    }

    UnknownTable(path:, table:, suggestion:) -> {
      let hint = case suggestion {
        option.Some(s) -> "
  hint: Did you mean \"" <> s <> "\"?"
        option.None -> ""
      }
      "error: Unknown table
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Table \"" <> table <> "\" does not exist" <> hint
    }

    TypeInferenceError(path:, expression:) -> "error: Could not infer type
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Could not determine the type of: " <> expression <> "
  \u{2502}
  hint: Consider using a CAST expression: CAST(... AS TEXT)"

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

    StaleGeneratedCode(files:) -> {
      let file_list =
        files
        |> list.map(fn(f) { "    \u{2022} " <> f })
        |> string.join("\n")
      "error: Generated code is out of date

  These files need regeneration:
" <> file_list <> "

  Run `gleam run -m marmot` to update."
    }

    OutputNotUnderSrc(output:) -> "error: Output directory must be under src/
  \u{250c}\u{2500} gleam.toml
  \u{2502}
  \u{2502} output = \"" <> output <> "\"
  \u{2502}
  hint: Gleam compiles modules from src/, so generated code must live there.
        For example: output = \"src/generated/sql\""
  }
}
