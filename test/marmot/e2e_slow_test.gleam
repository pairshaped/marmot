import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import marmot
import marmot/internal/codegen
import marmot/internal/project
import marmot/internal/query
import marmot/internal/sqlite
import simplifile
import sqlight

fn with_temp_dir(base: String, body: fn() -> Nil) -> Nil {
  let result = rescue(body)
  let _ = simplifile.delete(base)
  case result {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

@external(erlang, "marmot_test_ffi", "rescue")
fn rescue(body: fn() -> Nil) -> Result(Nil, String)

@external(erlang, "timer", "sleep")
fn sleep(ms: Int) -> Nil

const cli_build_cache = "build/e2e_cli_shared"

const cli_build_cache_lock = "build/e2e_cli_shared.lock"

const cli_build_cache_ready = "build/e2e_cli_shared/.ready"

pub fn compile_failure_message_includes_generated_source_test() {
  let message =
    compile_failure_message(
      1,
      "tmp/check_project/src/sql.gleam",
      "pub fn broken",
    )

  let assert True =
    string.contains(message, "gleam check failed with exit code 1")
  let assert True =
    string.contains(message, "Generated file: tmp/check_project/src/sql.gleam")
  let assert True = string.contains(message, "Generated file contents:")
  let assert True = string.contains(message, "pub fn broken")
  Nil
}

fn compile_failure_message(
  exit_code: Int,
  out_file: String,
  contents: String,
) -> String {
  "gleam check failed with exit code "
  <> int.to_string(exit_code)
  <> "\nGenerated file: "
  <> out_file
  <> "\n\nGenerated file contents:\n"
  <> contents
}

fn report_compile_failure(exit_code: Int, out_file: String) -> Nil {
  let contents = case simplifile.read(out_file) {
    Ok(contents) -> contents
    Error(_) -> "Could not read generated file."
  }
  io.println_error(compile_failure_message(exit_code, out_file, contents))
}

pub fn e2e_compile_generated_code_test() {
  let base = "test_e2e_compile_tmp"
  use <- with_temp_dir(base)

  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE edge_users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        avatar BLOB,
        score REAL,
        bio TEXT,
        archived INTEGER
      )",
      on: db,
    )

  let sql_dir = base <> "/src/app/sql"
  let assert Ok(_) = simplifile.create_directory_all(sql_dir)
  let assert Ok(_) =
    simplifile.write(
      sql_dir <> "/find_edge_user.sql",
      "-- returns: EdgeUserRow\nSELECT id, name, avatar, score, bio, archived FROM edge_users WHERE id = ?",
    )

  let sql_files = project.list_sql_files(sql_dir)
  let queries =
    list.filter_map(sql_files, fn(file_path) {
      case simplifile.read(file_path) {
        Ok(sql) ->
          case sqlite.introspect_query(db, string.trim(sql)) {
            Ok(info) -> {
              let filename =
                file_path
                |> string.split("/")
                |> list.last
                |> result.unwrap("query.sql")
              let assert Ok(name) = query.function_name(filename)
              let assert Ok(type_name) =
                sqlite.parse_returns_annotation(string.trim(sql))
              Ok(query.Query(
                name: name,
                sql: string.trim(sql),
                path: file_path,
                parameters: info.parameters,
                columns: info.columns,
                custom_type_name: type_name,
              ))
            }
            Error(_) -> Error(Nil)
          }
        Error(_) -> Error(Nil)
      }
    })

  let assert 1 = list.length(queries)
  let assert Ok(raw) = codegen.generate_module(queries)
  let generated = marmot.format_gleam(raw)

  let project_dir = base <> "/check_project"
  let assert Ok(_) = simplifile.create_directory_all(project_dir <> "/src")
  let assert Ok(_) =
    simplifile.write(
      project_dir <> "/gleam.toml",
      "name = \"check\"
version = \"1.0.0\"
target = \"erlang\"

[dependencies]
gleam_stdlib = \">= 0.34.0 and < 2.0.0\"
sqlight = \">= 1.0.0 and < 2.0.0\"
",
    )

  let out_file = project_dir <> "/src/sql.gleam"
  let assert Ok(_) = simplifile.write(out_file, generated)

  let exit_code =
    marmot.run_executable_in_timeout("gleam", ["check"], project_dir, 120_000)

  case exit_code {
    0 -> Nil
    other -> report_compile_failure(other, out_file)
  }
  let assert 0 = exit_code
  Nil
}

pub fn e2e_shared_row_types_compiles_test() {
  let base = "test_e2e_shared_rows_tmp"
  use <- with_temp_dir(base)

  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE dogs (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        breed TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE cats (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT,
        color TEXT NOT NULL
      )",
      on: db,
    )

  let sql_dir = base <> "/src/app/sql"
  let assert Ok(_) = simplifile.create_directory_all(sql_dir)
  let assert Ok(_) =
    simplifile.write(
      sql_dir <> "/find_dog.sql",
      "SELECT id, name, breed FROM dogs WHERE id = ?",
    )
  let assert Ok(_) =
    simplifile.write(
      sql_dir <> "/find_cat.sql",
      "SELECT id, name, color FROM cats WHERE id = ?",
    )

  let sql_files = project.list_sql_files(sql_dir)
  let queries =
    list.filter_map(sql_files, fn(file_path) {
      case simplifile.read(file_path) {
        Ok(sql) ->
          case sqlite.introspect_query(db, string.trim(sql)) {
            Ok(info) -> {
              let filename =
                file_path
                |> string.split("/")
                |> list.last
                |> result.unwrap("query.sql")
              let assert Ok(name) = query.function_name(filename)
              Ok(query.Query(
                name: name,
                sql: string.trim(sql),
                path: file_path,
                parameters: info.parameters,
                columns: info.columns,
                custom_type_name: option.None,
              ))
            }
            Error(_) -> Error(Nil)
          }
        Error(_) -> Error(Nil)
      }
    })

  let assert 2 = list.length(queries)
  let assert Ok(raw) = codegen.generate_module(queries)
  let generated = marmot.format_gleam(raw)

  let project_dir = base <> "/check_project"
  let assert Ok(_) = simplifile.create_directory_all(project_dir <> "/src")
  let assert Ok(_) =
    simplifile.write(
      project_dir <> "/gleam.toml",
      "name = \"check\"
version = \"1.0.0\"
target = \"erlang\"

[dependencies]
gleam_stdlib = \">= 0.34.0 and < 2.0.0\"
sqlight = \">= 1.0.0 and < 2.0.0\"
",
    )

  let out_file = project_dir <> "/src/sql.gleam"
  let assert Ok(_) = simplifile.write(out_file, generated)

  let exit_code =
    marmot.run_executable_in_timeout("gleam", ["check"], project_dir, 120_000)

  case exit_code {
    0 -> Nil
    other -> report_compile_failure(other, out_file)
  }
  let assert 0 = exit_code
  Nil
}

fn write_cli_project(name: String, base: String) -> Nil {
  let assert Ok(_) = simplifile.create_directory_all(base <> "/src")
  link_cli_project_build(base)
  let assert Ok(_) =
    simplifile.write(
      base <> "/gleam.toml",
      "name = \""
        <> name
        <> "\"\nversion = \"1.0.0\"\ntarget = \"erlang\"\n"
        <> "\n[dependencies]\n"
        <> "gleam_stdlib = \">= 0.34.0 and < 2.0.0\"\n"
        <> "marmot = { path = \"..\" }\n",
    )
  Nil
}

fn write_cli_project_with_config(
  name: String,
  base: String,
  config: String,
) -> Nil {
  let assert Ok(_) = simplifile.create_directory_all(base <> "/src")
  link_cli_project_build(base)
  let assert Ok(_) =
    simplifile.write(
      base <> "/gleam.toml",
      "name = \""
        <> name
        <> "\"\nversion = \"1.0.0\"\ntarget = \"erlang\"\n"
        <> "\n[dependencies]\n"
        <> "gleam_stdlib = \">= 0.34.0 and < 2.0.0\"\n"
        <> "marmot = { path = \"..\" }\n"
        <> config,
    )
  Nil
}

fn link_cli_project_build(base: String) -> Nil {
  ensure_cli_build_cache()
  let assert Ok(_) =
    simplifile.create_symlink("../" <> cli_build_cache, base <> "/build")
  Nil
}

fn ensure_cli_build_cache() -> Nil {
  case simplifile.is_file(cli_build_cache_ready) {
    Ok(True) -> Nil
    _ -> prepare_cli_build_cache()
  }
}

fn prepare_cli_build_cache() -> Nil {
  case simplifile.create_directory(cli_build_cache_lock) {
    Ok(_) -> {
      let _ = simplifile.delete(cli_build_cache)
      let assert Ok(_) = simplifile.create_directory_all(cli_build_cache)
      let assert Ok(_) =
        simplifile.copy_directory(
          at: "build/packages",
          to: cli_build_cache <> "/packages",
        )
      let assert Ok(_) =
        simplifile.copy_directory(
          at: "build/dev",
          to: cli_build_cache <> "/dev",
        )
      let assert Ok(_) = simplifile.write(cli_build_cache_ready, "ready")
      let _ = simplifile.delete(cli_build_cache_lock)
      Nil
    }
    Error(_) -> wait_for_cli_build_cache(100)
  }
}

fn wait_for_cli_build_cache(attempts: Int) -> Nil {
  case simplifile.is_file(cli_build_cache_ready) {
    Ok(True) -> Nil
    _ if attempts > 0 -> {
      sleep(100)
      wait_for_cli_build_cache(attempts - 1)
    }
    _ -> panic as "Timed out waiting for shared CLI build cache"
  }
}

pub fn e2e_cli_missing_database_test() {
  let base = "test_e2e_cli_missing_db"
  let result =
    rescue(fn() {
      write_cli_project("cli_missing_db", base)
      let exit_code =
        marmot.run_executable_in_timeout(
          "gleam",
          ["run", "-m", "marmot"],
          base,
          120_000,
        )
      let assert 1 = exit_code
      Nil
    })
  let _ = simplifile.delete(base)
  case result {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

pub fn e2e_cli_invalid_output_test() {
  let base = "test_e2e_cli_bad_output"
  let result =
    rescue(fn() {
      write_cli_project("cli_bad_output", base)
      let exit_code =
        marmot.run_executable_in_timeout(
          "gleam",
          ["run", "-m", "marmot", "--", "--output", "/etc/foo"],
          base,
          120_000,
        )
      let assert 1 = exit_code
      Nil
    })
  let _ = simplifile.delete(base)
  case result {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

pub fn e2e_cli_malformed_query_function_test() {
  let base = "test_e2e_cli_bad_query_fn"
  let result =
    rescue(fn() {
      write_cli_project_with_config(
        "cli_bad_query_fn",
        base,
        "\n[tools.marmot]\ndatabase = \"test.db\"\nquery_function = \"not_valid\"\n",
      )
      let exit_code =
        marmot.run_executable_in_timeout(
          "gleam",
          ["run", "-m", "marmot"],
          base,
          120_000,
        )
      let assert 1 = exit_code
      Nil
    })
  let _ = simplifile.delete(base)
  case result {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

pub fn e2e_cli_successful_generation_test() {
  let base = "test_e2e_cli_success"
  let result =
    rescue(fn() {
      write_cli_project_with_config(
        "cli_success",
        base,
        "\n[tools.marmot]\ndatabase = \"test.db\"\n",
      )

      let assert Ok(db) = sqlight.open(base <> "/test.db")
      let assert Ok(_) =
        sqlight.exec(
          "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
          on: db,
        )
      let _ = sqlight.close(db)

      let sql_dir = base <> "/src/sql"
      let assert Ok(_) = simplifile.create_directory(sql_dir)
      let assert Ok(_) =
        simplifile.write(
          sql_dir <> "/find_user.sql",
          "SELECT id, name FROM users WHERE id = ?",
        )

      let exit_code =
        marmot.run_executable_in_timeout(
          "gleam",
          ["run", "-m", "marmot"],
          base,
          120_000,
        )

      let assert Ok(True) =
        simplifile.is_file(base <> "/src/generated/sql/sql.gleam")
      let assert 0 = exit_code
      Nil
    })
  let _ = simplifile.delete(base)
  case result {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

pub fn e2e_cli_no_sql_directories_test() {
  let base = "test_e2e_cli_no_sql"
  let result =
    rescue(fn() {
      write_cli_project_with_config(
        "cli_no_sql",
        base,
        "\n[tools.marmot]\ndatabase = \"test.db\"\n",
      )

      use db <- sqlight.with_connection(base <> "/test.db")
      let assert Ok(_) =
        sqlight.exec(
          "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY)",
          on: db,
        )

      let exit_code =
        marmot.run_executable_in_timeout(
          "gleam",
          ["run", "-m", "marmot"],
          base,
          120_000,
        )
      let assert 0 = exit_code
      Nil
    })
  let _ = simplifile.delete(base)
  case result {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

pub fn e2e_cli_configured_sql_dir_missing_test() {
  let base = "test_e2e_cli_missing_sql_dir"
  let result =
    rescue(fn() {
      write_cli_project_with_config(
        "cli_missing_sql_dir",
        base,
        "\n[tools.marmot]\ndatabase = \"test.db\"\nsql_dir = \"src/sql\"\n",
      )

      use db <- sqlight.with_connection(base <> "/test.db")
      let assert Ok(_) =
        sqlight.exec(
          "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY)",
          on: db,
        )

      let exit_code =
        marmot.run_executable_in_timeout(
          "gleam",
          ["run", "-m", "marmot"],
          base,
          120_000,
        )
      let assert 1 = exit_code
      Nil
    })
  let _ = simplifile.delete(base)
  case result {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

pub fn e2e_cli_database_open_error_test() {
  let base = "test_e2e_cli_bad_db"
  let result =
    rescue(fn() {
      write_cli_project("cli_bad_db", base)
      let exit_code =
        marmot.run_executable_in_timeout(
          "gleam",
          [
            "run",
            "-m",
            "marmot",
            "--",
            "--database",
            "/nonexistent_dir_xyz/db.sqlite",
          ],
          base,
          120_000,
        )
      let assert 1 = exit_code
      Nil
    })
  let _ = simplifile.delete(base)
  case result {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}
