import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/list
import gleam/option
import gleam/string
import marmot/internal/codegen
import marmot/internal/project
import marmot/internal/query.{type Query, Column, IntType, Query}
import marmot/internal/statement_cache as templates
import simplifile
import sqlight
import statement_cache_fixture/statement_cache

// The fixture under test/statement_cache_fixture is the generated statement
// cache for module path "statement_cache_fixture/statement_cache". These tests
// run the real generated code. To refresh the fixture after changing the
// templates, run the tests once with MARMOT_UPDATE_FIXTURES=1, then rerun them.

const fixture_module = "statement_cache_fixture/statement_cache"

const fixture_gleam = "test/statement_cache_fixture/statement_cache.gleam"

const fixture_erlang =
  "test/statement_cache_fixture/statement_cache_fixture@statement_cache_ffi.erl"

@external(erlang, "marmot_test_ffi", "rescue")
fn rescue(body: fn() -> a) -> Result(Nil, String)

@external(erlang, "marmot_test_ffi", "get_env")
fn get_env(name: String) -> Result(String, Nil)

@external(erlang, "marmot_test_ffi", "cached_statement")
fn cached_statement(db: sqlight.Connection, sql: String) -> Result(Dynamic, Nil)

@external(erlang, "marmot_test_ffi", "cached_statement_count")
fn cached_statement_count(db: sqlight.Connection) -> Int

@external(erlang, "marmot_test_ffi", "run_concurrently")
fn run_concurrently(funs: List(fn() -> a)) -> List(a)

@external(erlang, "marmot_test_ffi", "run_in_new_process")
fn run_in_new_process(fun: fn() -> a) -> a

// Generated source

pub fn fixture_matches_generated_source_test() {
  check_fixture(fixture_gleam, templates.gleam_source(fixture_module))
  check_fixture(fixture_erlang, templates.erlang_source(fixture_module))
}

fn check_fixture(path: String, expected: String) -> Nil {
  case get_env("MARMOT_UPDATE_FIXTURES") {
    Ok("1") -> {
      let assert Ok(_) = simplifile.write(path, expected)
      Nil
    }
    _ -> {
      let assert Ok(actual) = simplifile.read(path)
      case actual == expected {
        True -> Nil
        False ->
          panic as {
            path
            <> " is stale. Run once with MARMOT_UPDATE_FIXTURES=1 to refresh it, then rerun."
          }
      }
    }
  }
}

pub fn ffi_module_name_follows_module_path_test() {
  let assert "generated@sql@statement_cache_ffi" =
    templates.ffi_module_name("generated/sql/statement_cache")
  let assert "server@db@statement_cache_ffi" =
    templates.ffi_module_name("server/db/statement_cache")
}

pub fn statement_cache_paths_follow_output_directory_test() {
  let assert "src/generated/sql/statement_cache.gleam" =
    project.statement_cache_output_path(option.None)
  let assert "generated/sql/statement_cache" =
    project.statement_cache_module_path(option.None)
  let assert "src/generated/sql/generated@sql@statement_cache_ffi.erl" =
    project.statement_cache_ffi_output_path(option.None)

  let configured = option.Some("./src/server/generated/app/")
  let assert "src/server/generated/app/statement_cache.gleam" =
    project.statement_cache_output_path(option.Some("src/server/generated/app/"))
  let assert "server/generated/app/statement_cache" =
    project.statement_cache_module_path(configured)
  let assert "src/server/generated/app/server@generated@app@statement_cache_ffi.erl" =
    project.statement_cache_ffi_output_path(option.Some(
      "src/server/generated/app",
    ))
}

pub fn default_codegen_calls_statement_cache_test() {
  let assert Ok(output) = codegen.generate_module([count_query()])
  let assert True =
    string.contains(output, "import generated/sql/statement_cache")
  let assert True = string.contains(output, "statement_cache.query(")
  let assert False = string.contains(output, "sqlight.query(")
}

pub fn query_function_replaces_statement_cache_test() {
  let assert Ok(output) =
    codegen.generate_module_with_config(
      [count_query()],
      option.Some("server/db.query"),
      fixture_module,
    )
  let assert True = string.contains(output, "import server/db")
  let assert True = string.contains(output, "db.query(")
  let assert False = string.contains(output, "statement_cache")
}

fn count_query() -> Query {
  Query(
    name: "count_users",
    sql: "SELECT count(*) AS total FROM users",
    path: "src/app/sql/count_users.sql",
    parameters: [],
    columns: [Column(name: "total", column_type: IntType, nullable: False)],
    custom_type_name: option.None,
  )
}

// Statement reuse

const user_sql = "SELECT id, name FROM users WHERE id = ?1"

fn user_decoder() -> decode.Decoder(#(Int, String)) {
  use id <- decode.field(0, decode.int)
  use name <- decode.field(1, decode.string)
  decode.success(#(id, name))
}

fn find_user(db: sqlight.Connection, id: Int) {
  statement_cache.query(
    user_sql,
    on: db,
    with: [sqlight.int(id)],
    expecting: user_decoder(),
  )
}

fn users_db() -> sqlight.Connection {
  let assert Ok(db) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
       INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace'), (3, 'Edsger');",
      on: db,
    )
  db
}

pub fn query_matches_sqlight_query_test() {
  use db <- with_users_db
  let expected =
    sqlight.query(
      user_sql,
      on: db,
      with: [sqlight.int(2)],
      expecting: user_decoder(),
    )
  let assert Ok([#(2, "Grace")]) = expected
  let assert True = find_user(db, 2) == expected
  let assert True = find_user(db, 2) == expected
  let assert Ok([]) = find_user(db, 99)
}

pub fn decode_failure_matches_sqlight_query_test() {
  use db <- with_users_db
  // Decode the text column as an Int.
  let wrong = {
    use value <- decode.field(1, decode.int)
    decode.success(value)
  }
  let expected =
    sqlight.query(user_sql, on: db, with: [sqlight.int(1)], expecting: wrong)
  let assert Error(_) = expected
  let assert True =
    statement_cache.query(
      user_sql,
      on: db,
      with: [sqlight.int(1)],
      expecting: wrong,
    )
    == expected
  // The statement is still usable after the decoder rejected its rows.
  let assert Ok([#(1, "Ada")]) = find_user(db, 1)
}

pub fn reuses_the_same_statement_across_calls_test() {
  use db <- with_users_db
  let assert Error(Nil) = cached_statement(db, user_sql)

  let assert Ok([#(1, "Ada")]) = find_user(db, 1)
  let assert Ok(first) = cached_statement(db, user_sql)

  let assert Ok([#(2, "Grace")]) = find_user(db, 2)
  let assert Ok([#(3, "Edsger")]) = find_user(db, 3)
  let assert Ok(later) = cached_statement(db, user_sql)

  // Same compiled statement object, rebound each time, and still one entry.
  let assert True = first == later
  let assert 1 = cached_statement_count(db)
}

pub fn caches_one_statement_per_distinct_sql_test() {
  use db <- with_users_db
  let count = fn(sql) {
    statement_cache.query(
      sql,
      on: db,
      with: [],
      expecting: decode.field(0, decode.int, decode.success),
    )
  }
  let assert Ok([3]) = count("SELECT count(*) FROM users")
  let assert Ok([6]) = count("SELECT sum(id) FROM users")
  let assert Ok([3]) = count("SELECT count(*) FROM users")
  let assert Ok(_) = find_user(db, 1)
  let assert 3 = cached_statement_count(db)
}

pub fn statements_are_kept_per_connection_test() {
  use db <- with_users_db
  use other <- with_users_db
  let assert Ok(_) = find_user(db, 1)
  let assert Ok(_) = find_user(other, 1)
  let assert Ok(on_db) = cached_statement(db, user_sql)
  let assert Ok(on_other) = cached_statement(other, user_sql)
  let assert False = on_db == on_other
}

pub fn zero_parameter_statement_runs_repeatedly_test() {
  use db <- with_users_db
  let run = fn() {
    statement_cache.query(
      "SELECT id FROM users ORDER BY id",
      on: db,
      with: [],
      expecting: decode.field(0, decode.int, decode.success),
    )
  }
  let assert Ok([1, 2, 3]) = run()
  let assert Ok([1, 2, 3]) = run()
}

// Errors leave the statement reusable

const insert_sql = "INSERT INTO users (id, name) VALUES (?1, ?2) RETURNING id"

fn insert_user(db: sqlight.Connection, id: Int, name: String) {
  statement_cache.query(
    insert_sql,
    on: db,
    with: [sqlight.int(id), sqlight.text(name)],
    expecting: decode.field(0, decode.int, decode.success),
  )
}

pub fn step_error_leaves_the_statement_reusable_test() {
  use db <- with_users_db
  let assert Ok([4]) = insert_user(db, 4, "Barbara")
  let assert Ok(before) = cached_statement(db, insert_sql)

  // Primary key violation: the error comes from stepping the statement.
  let assert Error(sqlight.SqlightError(code: sqlight.ConstraintPrimarykey, ..)) =
    insert_user(db, 4, "Again")
  // NOT NULL violation: a second kind of step error on the same statement.
  let assert Error(sqlight.SqlightError(
    code: sqlight.ConstraintNotnull,
    message:,
    ..,
  )) =
    statement_cache.query(
      insert_sql,
      on: db,
      with: [sqlight.int(9), sqlight.null()],
      expecting: decode.field(0, decode.int, decode.success),
    )
  let assert True = string.contains(message, "users.name")

  // The same statement is still cached, reset, and works.
  let assert Ok([5]) = insert_user(db, 5, "Donald")
  let assert Ok(after) = cached_statement(db, insert_sql)
  let assert True = before == after
  let assert Ok([#(5, "Donald")]) = find_user(db, 5)
}

pub fn bind_error_leaves_the_statement_reusable_test() {
  use db <- with_users_db
  let assert Ok([4]) = insert_user(db, 4, "Barbara")
  let assert Ok(before) = cached_statement(db, insert_sql)

  // A third argument for a statement with two parameters is out of range.
  let assert Error(sqlight.SqlightError(code: sqlight.Range, ..)) =
    statement_cache.query(
      insert_sql,
      on: db,
      with: [sqlight.int(6), sqlight.text("Extra"), sqlight.int(1)],
      expecting: decode.field(0, decode.int, decode.success),
    )
  // Nothing was inserted, and the statement still works.
  let assert Ok([]) = find_user(db, 6)
  let assert Ok([5]) = insert_user(db, 5, "Donald")
  let assert Ok(after) = cached_statement(db, insert_sql)
  let assert True = before == after
}

pub fn missing_arguments_bind_null_not_the_previous_call_test() {
  use db <- with_users_db
  let pair = {
    use a <- decode.field(0, decode.optional(decode.int))
    use b <- decode.field(1, decode.optional(decode.int))
    decode.success(#(a, b))
  }
  let run = fn(arguments) {
    statement_cache.query(
      "SELECT ?1, ?2",
      on: db,
      with: arguments,
      expecting: pair,
    )
  }
  let assert Ok([#(option.Some(1), option.Some(2))]) =
    run([sqlight.int(1), sqlight.int(2)])
  // A fresh statement would have NULL for ?2. A reused one must too.
  let assert Ok([#(option.Some(3), option.None)]) = run([sqlight.int(3)])
}

pub fn exception_while_binding_discards_the_statement_test() {
  use db <- with_users_db
  let assert Ok(_) = find_user(db, 1)
  let assert Ok(before) = cached_statement(db, user_sql)

  // An integer wider than 64 bits makes esqlite raise instead of returning
  // an error. The call must still raise, as sqlight.query does.
  let too_big = 1_180_591_620_717_411_303_424
  let assert Error(_) = rescue(fn() { find_user(db, too_big) })

  // The statement was dropped rather than left in an unknown state, and the
  // next call compiles a fresh one that works.
  let assert Error(Nil) = cached_statement(db, user_sql)
  let assert Ok([#(2, "Grace")]) = find_user(db, 2)
  let assert Ok(after) = cached_statement(db, user_sql)
  let assert False = before == after
}

// Other processes

pub fn processes_never_share_statements_test() {
  use db <- with_users_db
  let assert Ok(_) = find_user(db, 1)
  let assert Ok(mine) = cached_statement(db, user_sql)

  let #(result, theirs) =
    run_in_new_process(fn() {
      let result = find_user(db, 2)
      #(result, cached_statement(db, user_sql))
    })
  let assert Ok([#(2, "Grace")]) = result
  let assert Ok(theirs) = theirs
  let assert False = mine == theirs
  let assert Ok(still_mine) = cached_statement(db, user_sql)
  let assert True = mine == still_mine
}

pub fn concurrent_processes_on_one_connection_get_correct_rows_test() {
  use db <- with_users_db
  let worker = fn(id) {
    fn() {
      list.repeat(Nil, 300)
      |> list.all(fn(_) {
        case find_user(db, id) {
          Ok([#(found, _)]) -> found == id
          _ -> False
        }
      })
    }
  }
  let results = run_concurrently([worker(1), worker(2), worker(3), worker(2)])
  let assert [True, True, True, True] = results
}

pub fn busy_database_returns_an_error_and_keeps_the_statement_test() {
  let path = temp_database("busy")
  let assert Ok(writer) = sqlight.open(path)
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE t (a INTEGER PRIMARY KEY)", on: writer)
  let assert Ok(other) = sqlight.open(path)
  let assert Ok(_) = sqlight.exec("PRAGMA busy_timeout = 0", on: other)
  let insert = fn(a) {
    statement_cache.query(
      "INSERT INTO t (a) VALUES (?1)",
      on: other,
      with: [sqlight.int(a)],
      expecting: decode.success(Nil),
    )
  }
  let assert Ok(_) = insert(1)
  let assert Ok(before) =
    cached_statement(other, "INSERT INTO t (a) VALUES (?1)")

  // Another connection holds the write lock. sqlight.query would crash inside
  // esqlite here; the cache returns the error and resets the statement.
  let assert Ok(_) = sqlight.exec("BEGIN IMMEDIATE", on: writer)
  let assert Error(sqlight.SqlightError(code: sqlight.Busy, ..)) = insert(2)
  let assert Ok(_) = sqlight.exec("ROLLBACK", on: writer)

  let assert Ok(_) = insert(2)
  let assert Ok(after) =
    cached_statement(other, "INSERT INTO t (a) VALUES (?1)")
  let assert True = before == after
  let assert Ok(Nil) = statement_cache.close(other)
  let assert Ok(Nil) = sqlight.close(writer)
}

pub fn exception_leaves_the_database_writable_from_another_connection_test() {
  let path = temp_database("exception")
  let assert Ok(db) = sqlight.open(path)
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
       INSERT INTO users VALUES (1, 'Ada');",
      on: db,
    )
  let assert Ok(_) = find_user(db, 1)
  let assert Error(_) =
    rescue(fn() { find_user(db, 1_180_591_620_717_411_303_424) })

  // No statement is left holding a read lock, so another connection can write.
  let assert Ok(other) = sqlight.open(path)
  let assert Ok(_) = sqlight.exec("PRAGMA busy_timeout = 0", on: other)
  let assert Ok(_) =
    sqlight.exec("INSERT INTO users VALUES (2, 'Grace')", on: other)
  let assert Ok([#(2, "Grace")]) = find_user(db, 2)
  let assert Ok(Nil) = sqlight.close(other)
  let assert Ok(Nil) = statement_cache.close(db)
}

// Closing connections

pub fn closed_connection_never_runs_cached_statements_test() {
  let db = users_db()
  let assert Ok(_) = find_user(db, 1)
  let assert Ok(_) = cached_statement(db, user_sql)
  let assert Ok(Nil) = sqlight.close(db)

  // SQLite would happily keep stepping the statement of a closed connection.
  // The cache must fail exactly as sqlight.query fails on a closed connection.
  let uncached =
    rescue(fn() {
      sqlight.query(
        user_sql,
        on: db,
        with: [sqlight.int(1)],
        expecting: user_decoder(),
      )
    })
  let cached = rescue(fn() { find_user(db, 1) })
  let assert Error(_) = uncached
  let assert True = cached == uncached
}

pub fn forget_releases_the_database_file_test() {
  let path = temp_database("forget")
  let assert Ok(db) = sqlight.open(path)
  prepare_wal_table(db)
  let assert Ok(_) = count_rows(db)

  // Closing with a cached statement alive leaves SQLite's connection open
  // behind the scenes, so the write-ahead log is not cleaned up.
  let assert Ok(Nil) = sqlight.close(db)
  let assert Ok(True) = simplifile.is_file(path <> "-wal")

  statement_cache.forget(db)
  let assert Ok(False) = simplifile.is_file(path <> "-wal")
  let assert 0 = cached_statement_count(db)
}

pub fn querying_a_closed_connection_releases_its_statements_test() {
  let path = temp_database("closed_query")
  let assert Ok(db) = sqlight.open(path)
  prepare_wal_table(db)
  let assert Ok(_) = count_rows(db)
  let assert Ok(Nil) = sqlight.close(db)
  let assert Ok(True) = simplifile.is_file(path <> "-wal")

  // The failing query itself drops the statements; no forget call is needed.
  let assert Error(_) = rescue(fn() { count_rows(db) })
  let assert Ok(False) = simplifile.is_file(path <> "-wal")
  let assert 0 = cached_statement_count(db)
}

pub fn close_helper_releases_the_database_file_test() {
  let path = temp_database("close")
  let assert Ok(db) = sqlight.open(path)
  prepare_wal_table(db)
  let assert Ok(_) = count_rows(db)
  let assert Ok(True) = simplifile.is_file(path <> "-wal")

  let assert Ok(Nil) = statement_cache.close(db)
  let assert Ok(False) = simplifile.is_file(path <> "-wal")
}

pub fn opening_a_new_connection_releases_closed_ones_test() {
  let path = temp_database("prune")
  let assert Ok(old) = sqlight.open(path)
  prepare_wal_table(old)
  let assert Ok(_) = count_rows(old)
  let assert Ok(Nil) = sqlight.close(old)
  let assert Ok(True) = simplifile.is_file(path <> "-wal")

  // The first query on another connection notices the closed one.
  use db <- with_users_db
  let assert Ok(_) = find_user(db, 1)
  let assert Ok(False) = simplifile.is_file(path <> "-wal")
  let assert 0 = cached_statement_count(old)
}

pub fn with_connection_closes_cleanly_with_cached_statements_test() {
  // sqlight.with_connection asserts that close succeeds.
  let rows = {
    use db <- sqlight.with_connection(":memory:")
    let assert Ok(_) = sqlight.exec("CREATE TABLE t (a INTEGER)", on: db)
    count_rows_in(db, "SELECT count(*) FROM t")
  }
  let assert Ok([0]) = rows
}

fn temp_database(name: String) -> String {
  let dir = "test_tmp/statement_cache_" <> name
  let _ = simplifile.delete(dir)
  let assert Ok(_) = simplifile.create_directory_all(dir)
  dir <> "/app.db"
}

fn prepare_wal_table(db: sqlight.Connection) -> Nil {
  let assert Ok(_) =
    sqlight.exec(
      "PRAGMA journal_mode = WAL;
       CREATE TABLE t (a INTEGER);
       INSERT INTO t VALUES (1);",
      on: db,
    )
  Nil
}

fn count_rows(db: sqlight.Connection) -> Result(List(Int), sqlight.Error) {
  count_rows_in(db, "SELECT count(*) FROM t")
}

fn count_rows_in(
  db: sqlight.Connection,
  sql: String,
) -> Result(List(Int), sqlight.Error) {
  statement_cache.query(
    sql,
    on: db,
    with: [],
    expecting: decode.field(0, decode.int, decode.success),
  )
}

// A query_function wrapper can keep the cache by delegating to it.

fn logging_query(
  sql: String,
  on connection: sqlight.Connection,
  with arguments: List(sqlight.Value),
  expecting decoder: decode.Decoder(a),
  log log: fn(String) -> Nil,
) -> Result(List(a), sqlight.Error) {
  log(sql)
  statement_cache.query(
    sql,
    on: connection,
    with: arguments,
    expecting: decoder,
  )
}

pub fn wrapper_delegating_to_the_cache_keeps_caching_test() {
  use db <- with_users_db
  let run = fn() {
    logging_query(
      user_sql,
      on: db,
      with: [sqlight.int(1)],
      expecting: user_decoder(),
      log: fn(_) { Nil },
    )
  }
  let assert Ok([#(1, "Ada")]) = run()
  let assert Ok(first) = cached_statement(db, user_sql)
  let assert Ok([#(1, "Ada")]) = run()
  let assert Ok(second) = cached_statement(db, user_sql)
  let assert True = first == second
}

// Helpers

fn with_users_db(body: fn(sqlight.Connection) -> a) -> a {
  let db = users_db()
  let value = body(db)
  let assert Ok(Nil) = statement_cache.close(db)
  value
}
