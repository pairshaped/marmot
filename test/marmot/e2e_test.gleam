import birdie
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

/// Run a test body with guaranteed cleanup of a temp directory, even if
/// the body panics on a failed assertion.
fn with_temp_dir(base: String, body: fn() -> Nil) -> Nil {
  let result = rescue(body)
  // Always clean up, regardless of pass/fail
  let _ = simplifile.delete(base)
  case result {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

@external(erlang, "marmot_test_ffi", "rescue")
fn rescue(body: fn() -> Nil) -> Result(Nil, String)

pub fn e2e_generate_module_test() {
  let base = "test_e2e_tmp"
  use <- with_temp_dir(base)

  // Setup: create temp directory structure with SQL files
  let sql_dir = base <> "/src/app/sql"
  let assert Ok(_) = simplifile.create_directory_all(sql_dir)
  let assert Ok(_) =
    simplifile.write(
      sql_dir <> "/find_user.sql",
      "SELECT id, username FROM users WHERE username = ?",
    )
  let assert Ok(_) =
    simplifile.write(
      sql_dir <> "/delete_user.sql",
      "DELETE FROM users WHERE id = ?",
    )

  // Create a test database with the schema
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        username TEXT NOT NULL
      )",
      on: db,
    )

  // Run the pipeline: scan -> introspect -> generate
  let sql_files = project.list_sql_files(sql_dir)
  let assert 2 = list.length(sql_files)

  let queries =
    list.filter_map(sql_files, fn(file_path) {
      case simplifile.read(file_path) {
        Ok(sql) -> {
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
        }
        Error(_) -> Error(Nil)
      }
    })

  let assert 2 = list.length(queries)

  // Generate module
  let assert Ok(output) = codegen.generate_module(queries)
  output
  |> birdie.snap(title: "e2e generate module two queries")
  let assert True = string.contains(output, "pub fn delete_user")
  let assert True = string.contains(output, "pub fn find_user")
  let assert True = string.contains(output, "FindUserRow")
  let assert True = string.contains(output, "import sqlight")

  // Write to output path
  let output_dir = option.Some(base <> "/src/generated/sql")
  let output_path = project.output_path(sql_dir, output_dir)
  let assert True = string.ends_with(output_path, "_sql.gleam")
  Nil
}

pub fn e2e_multiple_sql_directories_test() {
  let base = "test_e2e_multi"
  use <- with_temp_dir(base)

  let sql_dir1 = base <> "/src/app/sql"
  let sql_dir2 = base <> "/src/other/sql"
  let assert Ok(_) = simplifile.create_directory_all(sql_dir1)
  let assert Ok(_) = simplifile.create_directory_all(sql_dir2)
  let assert Ok(_) =
    simplifile.write(
      sql_dir1 <> "/find_user.sql",
      "SELECT id, username FROM users WHERE id = ?",
    )
  let assert Ok(_) =
    simplifile.write(
      sql_dir2 <> "/list_items.sql",
      "SELECT id, name FROM items",
    )

  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, username TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )

  // Scan both directories
  let dirs = project.find_sql_directories(base <> "/src", option.None)
  let assert 2 = list.length(dirs)

  // Generate modules for each directory
  let modules =
    list.map(dirs, fn(dir) {
      let sql_files = project.list_sql_files(dir)
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
                  let assert Ok(module) =
                    codegen.generate_module([
                      query.Query(
                        name: name,
                        sql: string.trim(sql),
                        path: file_path,
                        parameters: info.parameters,
                        columns: info.columns,
                        custom_type_name: option.None,
                      ),
                    ])
                  Ok(#(dir, module))
                }
                Error(_) -> Error(Nil)
              }
            Error(_) -> Error(Nil)
          }
        })
      queries
    })
    |> list.flatten

  // Should have 2 separate modules
  let assert 2 = list.length(modules)
  modules
  |> list.map(fn(m) { m.1 })
  |> string.join("\n---\n")
  |> birdie.snap(title: "e2e multiple sql directories combined")
  let assert True =
    list.any(modules, fn(m) { string.contains(m.1, "find_user") })
  let assert True =
    list.any(modules, fn(m) { string.contains(m.1, "list_items") })
  Nil
}

pub fn e2e_check_stale_detection_test() {
  let base = "test_e2e_check"
  use <- with_temp_dir(base)

  let sql_dir = base <> "/src/app/sql"
  let assert Ok(_) = simplifile.create_directory_all(sql_dir)
  let assert Ok(_) =
    simplifile.write(
      sql_dir <> "/find_user.sql",
      "SELECT id, username FROM users WHERE id = ?",
    )

  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, username TEXT NOT NULL)",
      on: db,
    )

  // Generate the module
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

  let output_dir = option.Some(base <> "/src/generated/sql")
  let output_path = project.output_path(sql_dir, output_dir)
  let assert Ok(expected) = codegen.generate_module(queries)

  // Before writing: output file doesn't exist, should be stale
  let current_before =
    simplifile.read(output_path)
    |> result.unwrap("")
  let assert True = expected != current_before

  // Write the generated output
  let parent =
    output_path
    |> string.split("/")
    |> list.reverse
    |> list.rest
    |> result.unwrap([])
    |> list.reverse
    |> string.join("/")
  let assert Ok(_) = simplifile.create_directory_all(parent)
  let assert Ok(_) = simplifile.write(output_path, expected)

  // After writing: should be up to date
  let current_after =
    simplifile.read(output_path)
    |> result.unwrap("")
  let assert True = expected == current_after

  // Modify a SQL file to make it stale
  let assert Ok(_) =
    simplifile.write(
      sql_dir <> "/find_user.sql",
      "SELECT id FROM users WHERE id = ?",
    )
  let queries2 =
    list.filter_map(project.list_sql_files(sql_dir), fn(file_path) {
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
  let assert Ok(expected2) = codegen.generate_module(queries2)
  let assert True = expected2 != current_after
  Nil
}

pub fn e2e_configured_output_dir_test() {
  let base = "test_e2e_tmp2"
  use <- with_temp_dir(base)

  let sql_dir = base <> "/src/app/sql"
  let output_dir = base <> "/src/generated"
  let assert Ok(_) = simplifile.create_directory_all(sql_dir)
  let assert Ok(_) = simplifile.create_directory_all(output_dir)
  let assert Ok(_) =
    simplifile.write(
      sql_dir <> "/find_user.sql",
      "SELECT id FROM users WHERE id = ?",
    )

  let output_path = project.output_path(sql_dir, option.Some(output_dir))
  // Common prefix is "test_e2e_tmp2/src/", relative is "app/sql",
  // strip trailing /sql -> "app"
  let expected = output_dir <> "/" <> "app_sql.gleam"
  let assert True = output_path == expected
  Nil
}

// ---- generated code compilation tests ----

pub fn e2e_compile_generated_code_test() {
  let base = "test_e2e_compile_tmp"
  use <- with_temp_dir(base)

  // Schema with nullable types including BLOB, which exercises edge-case imports
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

  // Write generated code into the examples project where deps are resolved,
  // then run gleam check to verify the code compiles and imports resolve.
  let out_file = "examples/src/generated/compile_edge_sql.gleam"
  let assert Ok(_) = simplifile.write(out_file, generated)

  let exit_code = marmot.run_executable_in("gleam", ["check"], "examples")
  let _ = simplifile.delete(out_file)

  let assert 0 = exit_code
  Nil
}

pub fn e2e_shared_row_types_compiles_test() {
  let base = "test_e2e_shared_rows_tmp"
  use <- with_temp_dir(base)

  // Two tables sharing a column name "id" and "name" — exercises shared row
  // type generation where column types differ (cats.name is nullable)
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

  let out_file = "examples/src/generated/compile_shared_rows_sql.gleam"
  let assert Ok(_) = simplifile.write(out_file, generated)

  let exit_code = marmot.run_executable_in("gleam", ["check"], "examples")
  let _ = simplifile.delete(out_file)

  let assert 0 = exit_code
  Nil
}

// ---- real CLI tests (gleam run -m marmot) ----

fn write_cli_project(name: String, base: String) -> Nil {
  let assert Ok(_) = simplifile.create_directory_all(base <> "/src")
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

      // Create test database and SQL file via explicit open/close
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

      // Verify output file was written
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
      // No sql/ dirs: reports message and exits 0, not an error
      let assert 0 = exit_code
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
