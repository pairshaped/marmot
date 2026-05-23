//// Forward-only SQLite migrations for Marmot.

import gleam/dynamic/decode
import gleam/list
import gleam/result
import gleam/string
import simplifile
import sqlight

const migration_dir = "db/migrations"

pub type MigrationError {
  DatabaseOpenError(path: String, message: String)
  MigrationDirectoryReadError(path: String, message: String)
  MigrationFileReadError(path: String, message: String)
  InvalidMigrationFilename(path: String)
  MigrationSqlError(path: String, message: String)
}

type Migration {
  Migration(path: String, version: String)
}

pub fn migrate(database_path: String) -> Result(List(String), MigrationError) {
  migrate_from(database_path, migration_dir)
}

@internal
pub fn migrate_from(
  database_path: String,
  migrations_dir: String,
) -> Result(List(String), MigrationError) {
  case sqlight.open(database_path) {
    Error(err) ->
      Error(DatabaseOpenError(path: database_path, message: err.message))

    Ok(db) -> {
      let result = migrate_connection(db, migrations_dir)
      let _close_result = sqlight.close(db)
      result
    }
  }
}

fn migrate_connection(
  db: sqlight.Connection,
  migrations_dir: String,
) -> Result(List(String), MigrationError) {
  use migrations <- result.try(read_migrations(migrations_dir))
  use _ <- result.try(ensure_schema_migrations(db))
  use applied <- result.try(read_applied_versions(db))

  migrations
  |> list.filter(fn(migration) { !list.contains(applied, migration.version) })
  |> apply_migrations(db, [])
}

fn read_migrations(
  migrations_dir: String,
) -> Result(List(Migration), MigrationError) {
  case simplifile.read_directory(migrations_dir) {
    Error(err) ->
      Error(MigrationDirectoryReadError(
        path: migrations_dir,
        message: string.inspect(err),
      ))

    Ok(entries) ->
      entries
      |> list.sort(string.compare)
      |> list.try_map(fn(filename) {
        migration_from_filename(migrations_dir, filename)
      })
  }
}

fn migration_from_filename(
  migrations_dir: String,
  filename: String,
) -> Result(Migration, MigrationError) {
  let path = migrations_dir <> "/" <> filename

  case valid_migration_filename(filename) {
    False -> Error(InvalidMigrationFilename(path: path))
    True -> {
      let version = string.drop_end(filename, string.length(".sql"))
      Ok(Migration(path:, version:))
    }
  }
}

fn valid_migration_filename(filename: String) -> Bool {
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

fn ensure_schema_migrations(
  db: sqlight.Connection,
) -> Result(Nil, MigrationError) {
  sqlight.exec(
    "CREATE TABLE IF NOT EXISTS schema_migrations (
      version TEXT PRIMARY KEY,
      applied_at TEXT NOT NULL
    )",
    on: db,
  )
  |> result.map_error(fn(err) {
    MigrationSqlError(path: "schema_migrations", message: err.message)
  })
}

fn read_applied_versions(
  db: sqlight.Connection,
) -> Result(List(String), MigrationError) {
  sqlight.query(
    "SELECT version FROM schema_migrations ORDER BY version",
    on: db,
    with: [],
    expecting: version_decoder(),
  )
  |> result.map_error(fn(err) {
    MigrationSqlError(path: "schema_migrations", message: err.message)
  })
}

fn version_decoder() -> decode.Decoder(String) {
  use version <- decode.field(0, decode.string)
  decode.success(version)
}

fn apply_migrations(
  migrations: List(Migration),
  db: sqlight.Connection,
  applied: List(String),
) -> Result(List(String), MigrationError) {
  case migrations {
    [] -> Ok(list.reverse(applied))
    [migration, ..rest] -> {
      use _ <- result.try(apply_migration(db, migration))
      apply_migrations(rest, db, [migration.version, ..applied])
    }
  }
}

fn apply_migration(
  db: sqlight.Connection,
  migration: Migration,
) -> Result(Nil, MigrationError) {
  use sql <- result.try(read_migration_sql(migration))
  use _ <- result.try(begin_transaction(db, migration))

  case sqlight.exec(sql, on: db) {
    Error(err) -> {
      let _rollback = rollback_transaction(db)
      Error(MigrationSqlError(path: migration.path, message: err.message))
    }

    Ok(_) ->
      case record_migration(db, migration.version, migration.path) {
        Error(err) -> {
          let _rollback = rollback_transaction(db)
          Error(err)
        }

        Ok(_) -> commit_transaction(db, migration)
      }
  }
}

fn read_migration_sql(migration: Migration) -> Result(String, MigrationError) {
  simplifile.read(migration.path)
  |> result.map_error(fn(err) {
    MigrationFileReadError(path: migration.path, message: string.inspect(err))
  })
}

fn begin_transaction(
  db: sqlight.Connection,
  migration: Migration,
) -> Result(Nil, MigrationError) {
  sqlight.exec("BEGIN", on: db)
  |> result.map_error(fn(err) {
    MigrationSqlError(path: migration.path, message: err.message)
  })
}

fn commit_transaction(
  db: sqlight.Connection,
  migration: Migration,
) -> Result(Nil, MigrationError) {
  sqlight.exec("COMMIT", on: db)
  |> result.map_error(fn(err) {
    MigrationSqlError(path: migration.path, message: err.message)
  })
}

fn rollback_transaction(db: sqlight.Connection) -> Result(Nil, sqlight.Error) {
  sqlight.exec("ROLLBACK", on: db)
}

fn record_migration(
  db: sqlight.Connection,
  version: String,
  path: String,
) -> Result(List(Nil), MigrationError) {
  sqlight.query(
    "INSERT INTO schema_migrations (version, applied_at)
    VALUES (?, datetime('now'))",
    on: db,
    with: [sqlight.text(version)],
    expecting: decode.success(Nil),
  )
  |> result.map_error(fn(err) {
    MigrationSqlError(path: path, message: err.message)
  })
}

pub fn to_string(error: MigrationError) -> String {
  case error {
    DatabaseOpenError(path:, message:) -> "error: Could not open SQLite database
  Path: " <> path <> "
  " <> message

    MigrationDirectoryReadError(path:, message:) ->
      "error: Could not read migration directory
  Path: " <> path <> "
  " <> message

    MigrationFileReadError(path:, message:) ->
      "error: Could not read migration file
  Path: " <> path <> "
  " <> message

    InvalidMigrationFilename(path:) -> "error: Invalid migration filename
  Path: " <> path <> "
  Expected db/migrations/NNN_description.sql, for example 001_create_users.sql"

    MigrationSqlError(path:, message:) -> "error: Migration SQL failed
  Path: " <> path <> "
  " <> message
  }
}
