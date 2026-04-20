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
    ["check", ..] -> run_check(args)
    _ -> run_generate(args)
  }
}

fn with_database(
  args: List(String),
  callback: fn(sqlight.Connection, project.Config) -> Nil,
) -> Nil {
  let env_database = get_env("DATABASE_URL")
  let toml_content =
    simplifile.read("gleam.toml")
    |> result.unwrap("")
  let config = project.parse_config(toml_content, args, env_database)

  case config.database {
    option.None -> {
      io.println_error(error.to_string(error.DatabaseNotConfigured))
      halt(1)
    }
    option.Some(db_path) ->
      case sqlight.open(db_path) {
        Ok(db) -> {
          callback(db, config)
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
}

fn run_generate(args: List(String)) -> Nil {
  with_database(args, fn(db, config) {
    generate_all(db, config)
  })
}

fn generate_all(db: sqlight.Connection, config: project.Config) -> Nil {
  let sql_dirs = project.find_sql_directories("src")
  case sql_dirs {
    [] -> io.println("No sql/ directories found under src/")
    dirs -> {
      // Detect output path collisions when using configured output directory
      let outputs =
        list.map(dirs, fn(dir) { project.output_path(dir, config.output) })
      case list.length(list.unique(outputs)) == list.length(outputs) {
        False -> {
          io.println_error(
            "error: Multiple sql/ directories would write to the same output file."
            <> "\n  Remove the output configuration or restructure your sql/ directories.",
          )
          halt(1)
        }
        True -> {
          let success_count =
            list.fold(dirs, 0, fn(count, dir) {
              case generate_for_directory(db, dir, config) {
                True -> count + 1
                False -> count
              }
            })
          case success_count == list.length(dirs) {
            True ->
              io.println(
                "Generated "
                <> int.to_string(list.length(dirs))
                <> " module(s)",
              )
            False -> {
              io.println_error("error: Some files could not be written")
              halt(1)
            }
          }
        }
      }
    }
  }
}

fn generate_for_directory(
  db: sqlight.Connection,
  sql_dir: String,
  config: project.Config,
) -> Bool {
  let sql_files = project.list_sql_files(sql_dir)
  let queries =
    list.filter_map(sql_files, fn(file_path) { process_sql_file(db, file_path) })

  case queries {
    [] -> True
    _ -> {
      let output = project.output_path(sql_dir, config.output)
      let module_content = codegen.generate_module(queries)
      case ensure_parent_dir(output) {
        Error(msg) -> {
          io.println_error("error: " <> msg)
          False
        }
        Ok(_) ->
          case simplifile.write(output, module_content) {
            Ok(_) -> {
              io.println("  wrote " <> output)
              True
            }
            Error(_) -> {
              io.println_error("error: Could not write to " <> output)
              False
            }
          }
      }
    }
  }
}

fn ensure_parent_dir(path: String) -> Result(Nil, String) {
  let parent =
    path
    |> string.split("/")
    |> list.reverse
    |> list.rest
    |> result.unwrap([])
    |> list.reverse
    |> string.join("/")

  case parent {
    "" -> Ok(Nil)
    dir ->
      case simplifile.create_directory_all(dir) {
        Ok(_) -> Ok(Nil)
        Error(_) -> Error("Could not create directory: " <> dir)
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
      case contains_semicolon_outside_strings(stripped) {
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

/// Check for semicolons outside of single-quoted SQL string literals.
/// Handles escaped quotes ('') inside strings.
fn contains_semicolon_outside_strings(sql: String) -> Bool {
  do_check_semicolon(sql, False)
}

fn do_check_semicolon(s: String, in_string: Bool) -> Bool {
  case string.pop_grapheme(s) {
    Error(_) -> False
    Ok(#("'", rest)) ->
      case in_string {
        True ->
          // Check for escaped quote ''
          case string.pop_grapheme(rest) {
            Ok(#("'", rest2)) -> do_check_semicolon(rest2, True)
            _ -> do_check_semicolon(rest, False)
          }
        False -> do_check_semicolon(rest, True)
      }
    Ok(#(";", rest)) ->
      case in_string {
        True -> do_check_semicolon(rest, True)
        False -> True
      }
    Ok(#(_, rest)) -> do_check_semicolon(rest, in_string)
  }
}

fn run_check(args: List(String)) -> Nil {
  with_database(args, fn(db, config) {
    let stale = check_all(db, config)
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
  })
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

fn halt(code: Int) -> Nil {
  // init:stop/1 performs a graceful shutdown that flushes I/O buffers,
  // unlike erlang:halt/1 which exits immediately and may lose stderr output
  init_stop(code)
  // init:stop is async; block until the VM shuts down
  timer_sleep(100_000_000)
}

@external(erlang, "init", "stop")
fn init_stop(code: Int) -> Nil

@external(erlang, "timer", "sleep")
fn timer_sleep(ms: Int) -> Nil

@external(erlang, "os", "getenv")
fn getenv_ffi(name: String) -> Dynamic

fn get_env(name: String) -> Option(String) {
  let raw = getenv_ffi(name)
  case decode.run(raw, decode.string) {
    Ok(value) -> option.Some(value)
    Error(_) -> option.None
  }
}
