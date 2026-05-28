//// Forward-only SQLite seed files for Marmot.

import gleam/option
import gleam/result
import marmot/internal/sql_files

const seed_dir = "db/seeds"

pub type SeedError {
  DatabaseOpenError(path: String, message: String)
  MissingSeedDirectory(path: String)
  SeedPathIsNotDirectory(path: String)
  NoSeedFiles(path: String)
  SeedDirectoryReadError(path: String, message: String)
  SeedFileReadError(path: String, message: String)
  InvalidSeedFilename(path: String)
  SeedSqlError(path: String, message: String)
}

pub fn seed(database_path: String) -> Result(List(String), SeedError) {
  seed_from(database_path, seed_dir)
}

@internal
pub fn seed_from(
  database_path: String,
  seeds_dir: String,
) -> Result(List(String), SeedError) {
  sql_files.run(database_path, seeds_dir, option.None)
  |> result.map_error(map_error)
}

fn map_error(error: sql_files.SqlFilesError) -> SeedError {
  case error {
    sql_files.DatabaseOpenError(path:, message:) ->
      DatabaseOpenError(path:, message:)
    sql_files.MissingDirectory(path:) -> MissingSeedDirectory(path:)
    sql_files.PathIsNotDirectory(path:) -> SeedPathIsNotDirectory(path:)
    sql_files.NoSqlFiles(path:) -> NoSeedFiles(path:)
    sql_files.DirectoryReadError(path:, message:) ->
      SeedDirectoryReadError(path:, message:)
    sql_files.FileReadError(path:, message:) ->
      SeedFileReadError(path:, message:)
    sql_files.InvalidFilename(path:) -> InvalidSeedFilename(path:)
    sql_files.SqlError(path:, message:) -> SeedSqlError(path:, message:)
  }
}

pub fn to_string(error: SeedError) -> String {
  case error {
    DatabaseOpenError(path:, message:) -> "error: Could not open SQLite database
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message

    MissingSeedDirectory(path:) -> "error: Missing seed directory
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Marmot looks for seed files in this directory.
  \u{2502}
  hint: Create db/seeds and add a file named like 001_development_users.sql"

    SeedPathIsNotDirectory(path:) -> "error: Seed path is not a directory
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Expected a directory containing seed files.
  \u{2502}
  hint: Replace this file with a directory and add files named like 001_development_users.sql"

    NoSeedFiles(path:) -> "error: No seed files found
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} This directory exists, but it does not contain any seed files.
  \u{2502}
  hint: Add at least one file named like 001_development_users.sql"

    SeedDirectoryReadError(path:, message:) ->
      "error: Could not read seed directory
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message

    SeedFileReadError(path:, message:) -> "error: Could not read seed file
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message

    InvalidSeedFilename(path:) -> "error: Invalid seed filename
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Seed files must match NNN_description.sql.
  \u{2502} NNN must be three digits. The description must use lowercase letters, digits, or underscores.
  \u{2502}
  hint: Rename this file to something like 001_development_users.sql"

    SeedSqlError(path:, message:) -> "error: Seed SQL failed
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message
  }
}
