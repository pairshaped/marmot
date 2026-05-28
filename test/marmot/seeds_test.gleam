import gleam/dynamic/decode
import gleam/list
import gleam/result
import marmot/seeds
import simplifile
import sqlight

pub fn runs_seeds_in_filename_order_test() {
  let base = prepare("test_tmp/seeds_order")
  let seeds_dir = base <> "/db/seeds"
  let db_path = base <> "/app.db"

  write_seed(
    seeds_dir,
    "002_add_lucy.sql",
    "INSERT INTO users (id, name) VALUES (1, 'Lucy')",
  )
  write_seed(
    seeds_dir,
    "001_create_users.sql",
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
  )

  let assert Ok(["001_create_users", "002_add_lucy"]) =
    seeds.seed_from(db_path, seeds_dir)

  use db <- sqlight.with_connection(db_path)
  let assert Ok(1) = user_count(db)
}

pub fn runs_all_seeds_every_time_test() {
  let base = prepare("test_tmp/seeds_repeat")
  let seeds_dir = base <> "/db/seeds"
  let db_path = base <> "/app.db"

  write_seed(
    seeds_dir,
    "001_create_users.sql",
    "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
  )
  write_seed(
    seeds_dir,
    "002_add_user.sql",
    "INSERT INTO users (name) VALUES ('Lucy')",
  )

  let assert Ok(["001_create_users", "002_add_user"]) =
    seeds.seed_from(db_path, seeds_dir)
  let assert Ok(["001_create_users", "002_add_user"]) =
    seeds.seed_from(db_path, seeds_dir)

  use db <- sqlight.with_connection(db_path)
  let assert Ok(2) = user_count(db)
  let assert Ok(0) = table_count(db, "schema_seeds")
}

pub fn rejects_invalid_seed_filenames_test() {
  let base = prepare("test_tmp/seeds_invalid_filename")
  let seeds_dir = base <> "/db/seeds"
  let db_path = base <> "/app.db"

  write_seed(seeds_dir, "001-create-users.sql", "SELECT 1")

  let assert Error(seeds.InvalidSeedFilename(path: invalid_path)) =
    seeds.seed_from(db_path, seeds_dir)
  assert invalid_path == seeds_dir <> "/001-create-users.sql"
}

pub fn rejects_missing_seed_directory_test() {
  let base = prepare_base("test_tmp/seeds_missing_dir")
  let seeds_dir = base <> "/db/seeds"
  let db_path = base <> "/app.db"

  let assert Error(seeds.MissingSeedDirectory(path: missing_path)) =
    seeds.seed_from(db_path, seeds_dir)
  assert missing_path == seeds_dir
}

fn prepare_base(base: String) -> String {
  let _ = simplifile.delete_all([base])
  let assert Ok(_) = simplifile.create_directory_all(base)
  base
}

fn prepare(base: String) -> String {
  let _ = prepare_base(base)
  let assert Ok(_) = simplifile.create_directory_all(base <> "/db/seeds")
  base
}

fn write_seed(dir: String, filename: String, sql: String) -> Nil {
  let assert Ok(_) = simplifile.write(dir <> "/" <> filename, sql)
  Nil
}

fn user_count(db: sqlight.Connection) -> Result(Int, sqlight.Error) {
  sqlight.query(
    "SELECT COUNT(*) FROM users",
    on: db,
    with: [],
    expecting: int_decoder(),
  )
  |> result.map(fn(rows) {
    rows
    |> list.first
    |> result.unwrap(0)
  })
}

fn table_count(
  db: sqlight.Connection,
  table_name: String,
) -> Result(Int, sqlight.Error) {
  sqlight.query(
    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
    on: db,
    with: [sqlight.text(table_name)],
    expecting: int_decoder(),
  )
  |> result.map(fn(rows) {
    rows
    |> list.first
    |> result.unwrap(0)
  })
}

fn int_decoder() -> decode.Decoder(Int) {
  use value <- decode.field(0, decode.int)
  decode.success(value)
}
