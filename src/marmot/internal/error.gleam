//// User-facing Marmot errors and message rendering.
////
//// Error variants carry enough context for `to_string` to print the path,
//// failing value, and recovery hint without reaching back into pipeline state.

import gleam/bool
import gleam/int
import gleam/list
import gleam/string
import marmot/internal/query.{type Column}

/// Errors that stop code generation. Each variant carries enough context for
/// `to_string` to produce a pretty-printed message with hints.
pub type MarmotError {
  DatabaseNotConfigured
  /// Failed to open the SQLite database file.
  DatabaseOpenError(path: String, message: String)
  /// Could not read a .sql file from disk.
  FileReadError(path: String, message: String)
  /// SQLite rejected the query (syntax error, missing table, etc.).
  SqlError(path: String, message: String)
  /// .sql file contains only whitespace and/or comments.
  EmptySqlFile(path: String)
  /// Filename produces no valid Gleam identifier (e.g., all special characters).
  InvalidFilename(path: String)
  /// .sql file contains more than one SQL statement.
  MultipleQueries(path: String)
  /// Two or more result columns have the same name.
  DuplicateColumns(path: String, columns: List(String))
  /// Configured output path is not under `src/`.
  OutputNotUnderSrc(output: String)
  /// `-- returns:` annotation has an invalid type name.
  InvalidReturnsAnnotation(path: String, name: String, reason: String)
  /// Queries sharing a `-- returns:` type have mismatched column shapes.
  SharedTypeMismatch(name: String, conflicts: List(#(String, List(Column))))
  /// Sanitized column or parameter names collide after kebab/snake normalization.
  GeneratedNameCollision(path: String, names: List(#(String, String)))
  /// Bare column reference matches more than one in-scope table.
  AmbiguousColumnReference(
    path: String,
    column: String,
    candidates: List(String),
  )
  /// Qualified column reference uses a table alias that is not in scope.
  UnknownColumnAlias(path: String, alias: String)
  /// Qualified column reference names a column that does not exist on the
  /// referenced (and known) table.
  UnknownColumnInTable(path: String, table: String, column: String)
  /// Bare column reference does not match any column in any in-scope known
  /// table (and no in-scope table is unknown / a CTE / a view).
  UnknownColumnReference(path: String, column: String)
  /// INSERT VALUES row has a different number of expressions than the number
  /// of bindable columns (from the explicit column list or the table schema).
  InsertValuesCountMismatch(path: String, expected: Int, got: Int, row: Int)
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
            list.map(cols, format_shared_column)
            |> string.join(", ")
          "  \u{2502} " <> path <> "\n  \u{2502}   returns: [" <> shape <> "]"
        })
        |> string.join("\n  \u{2502}\n")
      header
      <> body
      <> "\n  \u{2502}\n  \u{2502} Align SELECT columns across these queries, or remove the annotation from one."
    }

    GeneratedNameCollision(path:, names:) -> {
      let name_list =
        names
        |> list.map(fn(pair) {
          let #(raw, generated) = pair
          "\"" <> raw <> "\" -> `" <> generated <> "`"
        })
        |> string.join(", ")

      "error: Generated Gleam names collide
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} These SQL names produce the same generated Gleam name: " <> name_list <> "
  \u{2502}
  hint: Use SQL aliases so each generated name is different."
    }

    AmbiguousColumnReference(path:, column:, candidates:) -> {
      let candidate_list =
        candidates
        |> list.map(fn(c) { "`" <> c <> "`" })
        |> string.join(", ")
      "error: Ambiguous column reference
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} `" <> column <> "` matches more than one in-scope table: " <> candidate_list <> "
  \u{2502}
  hint: Qualify the column with a table alias (e.g. `users." <> column <> "`)."
    }

    UnknownColumnAlias(path:, alias:) -> "error: Unknown table alias
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} `" <> alias <> "` does not match any table in this query's FROM clause
  \u{2502}
  hint: Check the alias spelling, or add the table to FROM."

    UnknownColumnInTable(path:, table:, column:) ->
      "error: Unknown column on table
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Column `" <> column <> "` does not exist on table `" <> table <> "`
  \u{2502}
  hint: Check the column name against the table's schema."

    UnknownColumnReference(path:, column:) -> "error: Unknown column
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} `" <> column <> "` does not match any column in this query's tables
  \u{2502}
  hint: Check the column spelling, or qualify it with a table alias."

    InsertValuesCountMismatch(path:, expected:, got:, row:) ->
      "error: INSERT VALUES column count mismatch
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Row " <> int.to_string(row) <> " has " <> int.to_string(got) <> " expression(s) but " <> int.to_string(
        expected,
      ) <> " column(s) expected
  \u{2502}
  hint: Each VALUES row must have exactly one expression per bindable column."
  }
}

fn format_shared_column(column: Column) -> String {
  let suffix = case column.nullable {
    True -> "?"
    False -> ""
  }

  column.name <> ": " <> query.gleam_type(column.column_type) <> suffix
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
