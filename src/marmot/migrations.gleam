//// Forward-only SQLite migrations for Marmot.

import gleam/option
import gleam/result
import marmot/internal/sql_files

const migration_dir = "db/migrations"

const tracking_table = "schema_migrations"

pub type MigrationError {
  DatabaseOpenError(path: String, message: String)
  MissingMigrationDirectory(path: String)
  MigrationPathIsNotDirectory(path: String)
  NoMigrationFiles(path: String)
  MigrationDirectoryReadError(path: String, message: String)
  MigrationFileReadError(path: String, message: String)
  InvalidMigrationFilename(path: String)
  MigrationSqlError(path: String, message: String)
}

pub fn migrate(database_path: String) -> Result(List(String), MigrationError) {
  migrate_from(database_path, migration_dir)
}

@internal
pub fn migrate_from(
  database_path: String,
  migrations_dir: String,
) -> Result(List(String), MigrationError) {
  sql_files.run(database_path, migrations_dir, option.Some(tracking_table))
  |> result.map_error(map_error)
}

fn map_error(error: sql_files.SqlFilesError) -> MigrationError {
  case error {
    sql_files.DatabaseOpenError(path:, message:) ->
      DatabaseOpenError(path:, message:)
    sql_files.MissingDirectory(path:) -> MissingMigrationDirectory(path:)
    sql_files.PathIsNotDirectory(path:) -> MigrationPathIsNotDirectory(path:)
    sql_files.NoSqlFiles(path:) -> NoMigrationFiles(path:)
    sql_files.DirectoryReadError(path:, message:) ->
      MigrationDirectoryReadError(path:, message:)
    sql_files.FileReadError(path:, message:) ->
      MigrationFileReadError(path:, message:)
    sql_files.InvalidFilename(path:) -> InvalidMigrationFilename(path:)
    sql_files.SqlError(path:, message:) -> MigrationSqlError(path:, message:)
  }
}

pub fn to_string(error: MigrationError) -> String {
  case error {
    DatabaseOpenError(path:, message:) -> "error: Could not open SQLite database
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message

    MissingMigrationDirectory(path:) -> "error: Missing migration directory
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Marmot looks for migration files in this directory.
  \u{2502}
  hint: Create db/migrations and add a file named like 001_create_users.sql"

    MigrationPathIsNotDirectory(path:) ->
      "error: Migration path is not a directory
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Expected a directory containing migration files.
  \u{2502}
  hint: Replace this file with a directory and add files named like 001_create_users.sql"

    NoMigrationFiles(path:) -> "error: No migration files found
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} This directory exists, but it does not contain any migration files.
  \u{2502}
  hint: Add at least one file named like 001_create_users.sql"

    MigrationDirectoryReadError(path:, message:) ->
      "error: Could not read migration directory
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message

    MigrationFileReadError(path:, message:) ->
      "error: Could not read migration file
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message

    InvalidMigrationFilename(path:) -> "error: Invalid migration filename
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Migration files must match NNN_description.sql.
  \u{2502} NNN must be three digits. The description must use lowercase letters, digits, or underscores.
  \u{2502}
  hint: Rename this file to something like 001_create_users.sql"

    MigrationSqlError(path:, message:) -> "error: Migration SQL failed
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message
  }
}
