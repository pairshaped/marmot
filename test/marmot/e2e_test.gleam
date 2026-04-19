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
              let name = query.function_name(filename)
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
  let output_path = project.output_path(sql_dir, option.None)
  let assert True = string.ends_with(output_path, "sql.gleam")

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

  let output_path = project.output_path(sql_dir, option.Some(output_dir))
  let assert True = output_path == output_dir <> "/sql.gleam"

  // Cleanup
  let assert Ok(_) = simplifile.delete(base)
}
