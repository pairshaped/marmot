import argv
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import marmot/internal/codegen
import marmot/internal/error
import marmot/internal/project
import marmot/internal/query
import marmot/internal/sqlite
import simplifile
import sqlight

pub fn main() -> Nil {
  let args = argv.load().arguments

  case args {
    ["check"] -> run_check()
    _ -> run_generate()
  }
}

fn run_generate() -> Nil {
  let args = argv.load().arguments
  let env_database = get_env("DATABASE_URL")

  let toml_content =
    simplifile.read("gleam.toml")
    |> result.unwrap("")

  let config = project.parse_config(toml_content, args, env_database)

  let db_path = case config.database {
    option.Some(path) -> path
    option.None -> {
      io.println_error(error.to_string(error.DatabaseNotConfigured))
      halt(1)
      panic as "unreachable"
    }
  }

  case sqlight.open(db_path) {
    Ok(db) -> {
      generate_all(db, config)
      // Connection cleanup — error is non-actionable
      let _close_result = sqlight.close(db)
      Nil
    }
    Error(err) -> {
      io.println_error(
        error.to_string(error.DatabaseOpenError(
          path: db_path,
          message: err.message,
        )),
      )
      halt(1)
    }
  }
}

fn generate_all(db: sqlight.Connection, config: project.Config) -> Nil {
  let sql_dirs = project.find_sql_directories("src")
  case sql_dirs {
    [] -> io.println("No sql/ directories found under src/")
    dirs -> {
      list.each(dirs, fn(dir) { generate_for_directory(db, dir, config) })
      io.println(
        "Generated " <> int.to_string(list.length(dirs)) <> " module(s)",
      )
    }
  }
}

fn generate_for_directory(
  db: sqlight.Connection,
  sql_dir: String,
  config: project.Config,
) -> Nil {
  let sql_files = project.list_sql_files(sql_dir)
  let queries =
    list.filter_map(sql_files, fn(file_path) { process_sql_file(db, file_path) })

  case queries {
    [] -> Nil
    _ -> {
      let output = project.output_path(sql_dir, config.output)
      let module_content = codegen.generate_module(queries)
      ensure_parent_dir(output)
      case simplifile.write(output, module_content) {
        Ok(_) -> io.println("  wrote " <> output)
        Error(_) -> io.println_error("error: Could not write to " <> output)
      }
    }
  }
}

fn ensure_parent_dir(path: String) -> Nil {
  let parent =
    path
    |> string.split("/")
    |> list.reverse
    |> list.rest
    |> result.unwrap([])
    |> list.reverse
    |> string.join("/")

  case parent {
    "" -> Nil
    dir -> {
      let _mkdir_result = simplifile.create_directory_all(dir)
      Nil
    }
  }
}

fn process_sql_file(
  db: sqlight.Connection,
  file_path: String,
) -> Result(query.Query, Nil) {
  use content <- result.try(
    simplifile.read(file_path)
    |> result.map_error(fn(_) {
      io.println_error(
        error.to_string(error.FileReadError(
          path: file_path,
          message: "Could not read file",
        )),
      )
      Nil
    }),
  )

  let trimmed = string.trim(content)
  use sql <- result.try(validate_sql(trimmed, file_path))
  use query_info <- result.try(
    sqlite.introspect_query(db, sql)
    |> result.map_error(fn(err) {
      io.println_error(
        error.to_string(error.SqlError(path: file_path, message: err.message)),
      )
      Nil
    }),
  )

  let filename =
    file_path
    |> string.split("/")
    |> list.last
    |> result.unwrap("query.sql")
  let name = query.function_name(filename)
  Ok(query.Query(
    name: name,
    sql: sql,
    path: file_path,
    parameters: query_info.parameters,
    columns: query_info.columns,
  ))
}

fn validate_sql(trimmed: String, file_path: String) -> Result(String, Nil) {
  case trimmed {
    "" -> {
      io.println_error(error.to_string(error.EmptySqlFile(path: file_path)))
      Error(Nil)
    }
    sql -> {
      let stripped = case string.ends_with(string.trim(sql), ";") {
        True ->
          sql
          |> string.trim
          |> string.drop_end(1)
          |> string.trim
        False -> string.trim(sql)
      }
      case string.contains(stripped, ";") {
        True -> {
          io.println_error(
            error.to_string(error.MultipleQueries(path: file_path)),
          )
          Error(Nil)
        }
        False -> Ok(stripped)
      }
    }
  }
}

fn run_check() -> Nil {
  let args = argv.load().arguments
  let env_database = get_env("DATABASE_URL")
  let toml_content =
    simplifile.read("gleam.toml")
    |> result.unwrap("")
  let config = project.parse_config(toml_content, args, env_database)

  let db_path = case config.database {
    option.Some(path) -> path
    option.None -> {
      io.println_error(error.to_string(error.DatabaseNotConfigured))
      halt(1)
      panic as "unreachable"
    }
  }

  case sqlight.open(db_path) {
    Ok(db) -> {
      let stale = check_all(db, config)
      // Connection cleanup — error is non-actionable
      let _close_result = sqlight.close(db)
      case stale {
        [] -> {
          io.println("All generated code is up to date.")
          halt(0)
        }
        files -> {
          io.println_error(
            error.to_string(error.StaleGeneratedCode(files: files)),
          )
          halt(1)
        }
      }
    }
    Error(err) -> {
      io.println_error(
        error.to_string(error.DatabaseOpenError(
          path: db_path,
          message: err.message,
        )),
      )
      halt(1)
    }
  }
}

fn check_all(db: sqlight.Connection, config: project.Config) -> List(String) {
  let sql_dirs = project.find_sql_directories("src")
  list.filter_map(sql_dirs, fn(dir) {
    let sql_files = project.list_sql_files(dir)
    let queries =
      list.filter_map(sql_files, fn(file_path) {
        process_sql_file(db, file_path)
      })

    case queries {
      [] -> Error(Nil)
      _ -> {
        let output = project.output_path(dir, config.output)
        let expected = codegen.generate_module(queries)
        let current =
          simplifile.read(output)
          |> result.unwrap("")
        case expected == current {
          True -> Error(Nil)
          False -> Ok(output)
        }
      }
    }
  })
}

@external(erlang, "erlang", "halt")
fn halt(code: Int) -> Nil

@external(erlang, "os", "getenv")
fn getenv_ffi(name: String) -> Dynamic

fn get_env(name: String) -> Option(String) {
  let raw = getenv_ffi(name)
  case decode.run(raw, decode.string) {
    Ok(value) -> option.Some(value)
    Error(_) -> option.None
  }
}
