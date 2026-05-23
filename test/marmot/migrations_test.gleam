import gleam/dynamic/decode
import marmot/migrations
import simplifile
import sqlight

pub fn applies_migrations_in_filename_order_test() {
  let base = prepare("test_tmp/migrations_order")
  let migrations_dir = base <> "/db/migrations"
  let db_path = base <> "/app.db"

  write_migration(
    migrations_dir,
    "002_add_email.sql",
    "ALTER TABLE users ADD COLUMN email TEXT",
  )
  write_migration(
    migrations_dir,
    "001_create_users.sql",
    "CREATE TABLE users (id INTEGER PRIMARY KEY)",
  )

  let assert Ok(["001_create_users", "002_add_email"]) =
    migrations.migrate_from(db_path, migrations_dir)

  use db <- sqlight.with_connection(db_path)
  let assert Ok(["001_create_users", "002_add_email"]) = applied_versions(db)
  let assert Ok(_) =
    sqlight.exec(
      "INSERT INTO users (id, email) VALUES (1, 'lucy@example.com')",
      on: db,
    )
}

pub fn skips_already_applied_migrations_test() {
  let base = prepare("test_tmp/migrations_idempotent")
  let migrations_dir = base <> "/db/migrations"
  let db_path = base <> "/app.db"

  write_migration(
    migrations_dir,
    "001_create_users.sql",
    "CREATE TABLE users (id INTEGER PRIMARY KEY)",
  )

  let assert Ok(["001_create_users"]) =
    migrations.migrate_from(db_path, migrations_dir)
  let assert Ok([]) = migrations.migrate_from(db_path, migrations_dir)

  use db <- sqlight.with_connection(db_path)
  let assert Ok(["001_create_users"]) = applied_versions(db)
}

pub fn failed_migration_is_not_recorded_test() {
  let base = prepare("test_tmp/migrations_failure")
  let migrations_dir = base <> "/db/migrations"
  let db_path = base <> "/app.db"

  write_migration(
    migrations_dir,
    "001_create_users.sql",
    "CREATE TABLE users (id INTEGER PRIMARY KEY)",
  )
  write_migration(
    migrations_dir,
    "002_insert_missing.sql",
    "INSERT INTO missing_table (id) VALUES (1)",
  )

  let assert Error(migrations.MigrationSqlError(path: _, message: _)) =
    migrations.migrate_from(db_path, migrations_dir)

  use db <- sqlight.with_connection(db_path)
  let assert Ok(["001_create_users"]) = applied_versions(db)
}

pub fn rejects_invalid_migration_filenames_test() {
  let base = prepare("test_tmp/migrations_invalid_filename")
  let migrations_dir = base <> "/db/migrations"
  let db_path = base <> "/app.db"

  write_migration(
    migrations_dir,
    "001_create_users.sql",
    "CREATE TABLE users (id INTEGER PRIMARY KEY)",
  )
  write_migration(migrations_dir, "002-add-email.sql", "SELECT 1")

  let assert Error(migrations.InvalidMigrationFilename(path: invalid_path)) =
    migrations.migrate_from(db_path, migrations_dir)
  assert invalid_path == migrations_dir <> "/002-add-email.sql"
}

fn prepare(base: String) -> String {
  let _ = simplifile.delete_all([base])
  let assert Ok(_) = simplifile.create_directory_all(base <> "/db/migrations")
  base
}

fn write_migration(dir: String, filename: String, sql: String) -> Nil {
  let assert Ok(_) = simplifile.write(dir <> "/" <> filename, sql)
  Nil
}

fn applied_versions(
  db: sqlight.Connection,
) -> Result(List(String), sqlight.Error) {
  sqlight.query(
    "SELECT version FROM schema_migrations ORDER BY version",
    on: db,
    with: [],
    expecting: version_decoder(),
  )
}

fn version_decoder() -> decode.Decoder(String) {
  use version <- decode.field(0, decode.string)
  decode.success(version)
}
