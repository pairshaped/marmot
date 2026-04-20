import gleam/list
import gleam/option
import gleam/result
import gleam/string
import marmot/internal/codegen
import marmot/internal/project
import marmot/internal/query
import marmot/internal/sqlite
import simplifile
import sqlight

pub fn e2e_generate_module_test() {
  // Setup: create temp directory structure with SQL files
  let base = "test_e2e_tmp"
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
  let output = codegen.generate_module(queries)
  let assert True = string.contains(output, "pub fn delete_user")
  let assert True = string.contains(output, "pub fn find_user")
  let assert True = string.contains(output, "FindUserRow")
  let assert True = string.contains(output, "import sqlight")

  // Write to output path
  let output_path = project.output_path(sql_dir, option.None, option.None)
  let assert True = string.ends_with(output_path, "sql.gleam")

  // Cleanup
  let assert Ok(_) = simplifile.delete(base)
}

pub fn e2e_multiple_sql_directories_test() {
  let base = "test_e2e_multi"
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
  let dirs = project.find_sql_directories(base <> "/src")
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
                  Ok(#(
                    dir,
                    codegen.generate_module([
                      query.Query(
                        name: name,
                        sql: string.trim(sql),
                        path: file_path,
                        parameters: info.parameters,
                        columns: info.columns,
                      ),
                    ]),
                  ))
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
  let assert True =
    list.any(modules, fn(m) { string.contains(m.1, "find_user") })
  let assert True =
    list.any(modules, fn(m) { string.contains(m.1, "list_items") })

  // Cleanup
  let assert Ok(_) = simplifile.delete(base)
}

pub fn e2e_check_stale_detection_test() {
  let base = "test_e2e_check"
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
              ))
            }
            Error(_) -> Error(Nil)
          }
        Error(_) -> Error(Nil)
      }
    })

  let output_path = project.output_path(sql_dir, option.None, option.None)
  let expected = codegen.generate_module(queries)

  // Before writing: output file doesn't exist, should be stale
  let current_before =
    simplifile.read(output_path)
    |> result.unwrap("")
  let assert True = expected != current_before

  // Write the generated output
  let parent = base <> "/src/app"
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
              ))
            }
            Error(_) -> Error(Nil)
          }
        Error(_) -> Error(Nil)
      }
    })
  let expected2 = codegen.generate_module(queries2)
  let assert True = expected2 != current_after

  // Cleanup
  let assert Ok(_) = simplifile.delete(base)
}

pub fn e2e_configured_output_dir_test() {
  let base = "test_e2e_tmp2"
  let sql_dir = base <> "/src/app/sql"
  let output_dir = base <> "/src/generated"
  let assert Ok(_) = simplifile.create_directory_all(sql_dir)
  let assert Ok(_) = simplifile.create_directory_all(output_dir)
  let assert Ok(_) =
    simplifile.write(
      sql_dir <> "/find_user.sql",
      "SELECT id FROM users WHERE id = ?",
    )

  let output_path =
    project.output_path(sql_dir, option.Some(output_dir), option.None)
  // Output filename is derived from the sql_dir path to avoid collisions
  let expected = output_dir <> "/" <> "test_e2e_tmp2_src_app_sql.gleam"
  let assert True = output_path == expected

  // Cleanup
  let assert Ok(_) = simplifile.delete(base)
}
