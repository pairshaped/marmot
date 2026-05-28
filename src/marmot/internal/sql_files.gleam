//// Shared ordered SQL-file runner for migrations and seeds.

import gleam/dynamic/decode
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import simplifile
import sqlight

pub type SqlFilesError {
  DatabaseOpenError(path: String, message: String)
  MissingDirectory(path: String)
  PathIsNotDirectory(path: String)
  NoSqlFiles(path: String)
  DirectoryReadError(path: String, message: String)
  FileReadError(path: String, message: String)
  InvalidFilename(path: String)
  SqlError(path: String, message: String)
}

type SqlFile {
  SqlFile(path: String, version: String)
}

pub fn run(
  database_path: String,
  directory: String,
  tracking_table: Option(String),
) -> Result(List(String), SqlFilesError) {
  case sqlight.open(database_path) {
    Error(err) ->
      Error(DatabaseOpenError(path: database_path, message: err.message))

    Ok(db) -> {
      let result = run_connection(db, directory, tracking_table)
      let _close_result = sqlight.close(db)
      result
    }
  }
}

fn run_connection(
  db: sqlight.Connection,
  directory: String,
  tracking_table: Option(String),
) -> Result(List(String), SqlFilesError) {
  use files <- result.try(read_sql_files(directory))

  case tracking_table {
    option.None -> apply_files(files, db, tracking_table, [])
    option.Some(table_name) -> {
      use _ <- result.try(ensure_tracking_table(db, table_name))
      use applied <- result.try(read_applied_versions(db, table_name))

      files
      |> list.filter(fn(file) { !list.contains(applied, file.version) })
      |> apply_files(db, tracking_table, [])
    }
  }
}

fn read_sql_files(directory: String) -> Result(List(SqlFile), SqlFilesError) {
  use _ <- result.try(validate_directory(directory))

  case simplifile.read_directory(directory) {
    Error(err) ->
      Error(DirectoryReadError(path: directory, message: string.inspect(err)))

    Ok([]) -> Error(NoSqlFiles(path: directory))

    Ok(entries) -> {
      entries
      |> list.sort(string.compare)
      |> list.try_map(fn(filename) {
        sql_file_from_filename(directory, filename)
      })
    }
  }
}

fn validate_directory(directory: String) -> Result(Nil, SqlFilesError) {
  case simplifile.is_directory(directory) {
    Ok(True) -> Ok(Nil)
    Ok(False) -> path_not_directory_error(directory)
    Error(err) ->
      Error(DirectoryReadError(path: directory, message: string.inspect(err)))
  }
}

fn path_not_directory_error(directory: String) -> Result(Nil, SqlFilesError) {
  case simplifile.is_file(directory) {
    Ok(True) -> Error(PathIsNotDirectory(path: directory))
    Ok(False) -> Error(MissingDirectory(path: directory))
    Error(err) ->
      Error(DirectoryReadError(path: directory, message: string.inspect(err)))
  }
}

fn sql_file_from_filename(
  directory: String,
  filename: String,
) -> Result(SqlFile, SqlFilesError) {
  let path = directory <> "/" <> filename

  case valid_filename(filename) {
    False -> Error(InvalidFilename(path: path))
    True -> {
      let version = string.drop_end(filename, string.length(".sql"))
      Ok(SqlFile(path:, version:))
    }
  }
}

fn valid_filename(filename: String) -> Bool {
  use <- bool_guard(string.ends_with(filename, ".sql"), False)
  let stem = string.drop_end(filename, string.length(".sql"))
  use <- bool_guard(string.length(stem) > 4, False)
  use <- bool_guard(string.slice(stem, at_index: 3, length: 1) == "_", False)
  use <- bool_guard(stem |> string.drop_start(4) |> description_is_valid, False)

  stem
  |> string.slice(at_index: 0, length: 3)
  |> string.to_graphemes
  |> list.all(is_digit)
}

fn description_is_valid(description: String) -> Bool {
  description != ""
  && {
    description
    |> string.to_graphemes
    |> list.all(is_description_character)
  }
}

fn is_digit(char: String) -> Bool {
  list.contains(["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"], char)
}

fn is_description_character(char: String) -> Bool {
  list.contains(
    [
      "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o",
      "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "0", "1", "2", "3",
      "4", "5", "6", "7", "8", "9", "_",
    ],
    char,
  )
}

fn bool_guard(check: Bool, fallback: a, next: fn() -> a) -> a {
  case check {
    True -> next()
    False -> fallback
  }
}

fn ensure_tracking_table(
  db: sqlight.Connection,
  table_name: String,
) -> Result(Nil, SqlFilesError) {
  sqlight.exec("CREATE TABLE IF NOT EXISTS " <> table_name <> " (
      version TEXT PRIMARY KEY,
      applied_at TEXT NOT NULL
    )", on: db)
  |> result.map_error(fn(err) {
    SqlError(path: table_name, message: err.message)
  })
}

fn read_applied_versions(
  db: sqlight.Connection,
  table_name: String,
) -> Result(List(String), SqlFilesError) {
  sqlight.query(
    "SELECT version FROM " <> table_name <> " ORDER BY version",
    on: db,
    with: [],
    expecting: version_decoder(),
  )
  |> result.map_error(fn(err) {
    SqlError(path: table_name, message: err.message)
  })
}

fn version_decoder() -> decode.Decoder(String) {
  use version <- decode.field(0, decode.string)
  decode.success(version)
}

fn apply_files(
  files: List(SqlFile),
  db: sqlight.Connection,
  tracking_table: Option(String),
  applied: List(String),
) -> Result(List(String), SqlFilesError) {
  case files {
    [] -> Ok(list.reverse(applied))
    [file, ..rest] -> {
      use _ <- result.try(apply_file(db, tracking_table, file))
      apply_files(rest, db, tracking_table, [file.version, ..applied])
    }
  }
}

fn apply_file(
  db: sqlight.Connection,
  tracking_table: Option(String),
  file: SqlFile,
) -> Result(Nil, SqlFilesError) {
  use sql <- result.try(read_sql(file))
  use _ <- result.try(begin_transaction(db, file))

  case sqlight.exec(sql, on: db) {
    Error(err) -> {
      let _rollback = rollback_transaction(db)
      Error(SqlError(path: file.path, message: err.message))
    }

    Ok(_) ->
      case tracking_table {
        option.None -> commit_transaction(db, file)
        option.Some(table_name) -> {
          case record_version(db, table_name, file.version, file.path) {
            Error(err) -> {
              let _rollback = rollback_transaction(db)
              Error(err)
            }

            Ok(_) -> commit_transaction(db, file)
          }
        }
      }
  }
}

fn read_sql(file: SqlFile) -> Result(String, SqlFilesError) {
  simplifile.read(file.path)
  |> result.map_error(fn(err) {
    FileReadError(path: file.path, message: string.inspect(err))
  })
}

fn begin_transaction(
  db: sqlight.Connection,
  file: SqlFile,
) -> Result(Nil, SqlFilesError) {
  sqlight.exec("BEGIN", on: db)
  |> result.map_error(fn(err) {
    SqlError(path: file.path, message: err.message)
  })
}

fn commit_transaction(
  db: sqlight.Connection,
  file: SqlFile,
) -> Result(Nil, SqlFilesError) {
  sqlight.exec("COMMIT", on: db)
  |> result.map_error(fn(err) {
    SqlError(path: file.path, message: err.message)
  })
}

fn rollback_transaction(db: sqlight.Connection) -> Result(Nil, sqlight.Error) {
  sqlight.exec("ROLLBACK", on: db)
}

fn record_version(
  db: sqlight.Connection,
  table_name: String,
  version: String,
  path: String,
) -> Result(List(Nil), SqlFilesError) {
  sqlight.query("INSERT INTO " <> table_name <> " (version, applied_at)
    VALUES (?, datetime('now'))", on: db, with: [sqlight.text(version)], expecting: decode.success(
    Nil,
  ))
  |> result.map_error(fn(err) { SqlError(path: path, message: err.message) })
}
