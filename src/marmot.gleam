import argv
import gleam/dict
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
import marmot/internal/sqlite/tokenize
import simplifile
import sqlight

pub fn main() -> Nil {
  let args = argv.load().arguments
  run_generate(args)
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

  case project.validate_output(config) {
    Error(_) -> {
      let output = option.unwrap(config.output, "")
      io.println_error(error.to_string(error.OutputNotUnderSrc(output:)))
      halt(1)
    }
    Ok(_) -> Nil
  }

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
  with_database(args, fn(db, config) { generate_all(db, config) })
}

fn generate_all(db: sqlight.Connection, config: project.Config) -> Nil {
  // Warn if query_function is configured but malformed
  case config.query_function {
    option.Some(value) ->
      case codegen.parse_query_function(config.query_function) {
        option.None ->
          io.println_error(
            "warning: query_function \""
            <> value
            <> "\" is malformed, expected \"module/path.function\" format."
            <> "\n  Falling back to sqlight.query",
          )
        option.Some(_) -> Nil
      }
    option.None -> Nil
  }
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
            "error: Multiple sql/ directories would write to the same output file.\n  Remove the output configuration or restructure your sql/ directories.",
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
                "Generated " <> int.to_string(list.length(dirs)) <> " module(s)",
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
  warn_subdirectories(sql_dir)
  let queries =
    list.filter_map(sql_files, fn(file_path) { process_sql_file(db, file_path) })

  case queries {
    [] ->
      // If there were SQL files but none produced queries, that means they all
      // had errors. Only return True (success) when the directory was empty.
      list.is_empty(sql_files)
    _ -> {
      let output = project.output_path(sql_dir, config.output)
      case codegen.generate_module_with_config(queries, config.query_function) {
        Error(err) -> {
          io.println_error(error.to_string(err))
          False
        }
        Ok(raw_content) -> {
          let module_content = format_gleam(raw_content)
          case ensure_parent_dir(output) {
            Error(_) -> {
              io.println_error(
                "error: Could not create parent directory for " <> output,
              )
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
  }
}

fn warn_subdirectories(sql_dir: String) -> Nil {
  case simplifile.read_directory(sql_dir) {
    Ok(entries) -> {
      let subdirs =
        list.filter(entries, fn(e) {
          case simplifile.is_directory(sql_dir <> "/" <> e) {
            Ok(True) -> True
            _ -> False
          }
        })
      case subdirs {
        [] -> Nil
        dirs ->
          io.println_error(
            "warning: "
            <> sql_dir
            <> " contains subdirectories that will be ignored: "
            <> string.join(dirs, ", ")
            <> "\n  hint: Marmot only reads .sql files directly inside sql/ directories",
          )
      }
    }
    Error(_) -> Nil
  }
}

fn ensure_parent_dir(path: String) -> Result(Nil, Nil) {
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
        Error(_) -> Error(Nil)
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

  let filename =
    file_path
    |> string.split("/")
    |> list.last
    |> result.unwrap("query.sql")
  use name <- result.try(
    query.function_name(filename)
    |> result.map_error(fn(_) {
      io.println_error(error.to_string(error.InvalidFilename(path: file_path)))
      Nil
    }),
  )

  let trimmed = string.trim(content)
  use sql <- result.try(validate_sql(trimmed, file_path))
  use custom_type_name <- result.try(
    sqlite.parse_returns_annotation(sql)
    |> result.map_error(fn(err) {
      let sqlite.InvalidReturnsTypeName(name: n, reason: r) = err
      io.println_error(
        error.to_string(error.InvalidReturnsAnnotation(
          path: file_path,
          name: n,
          reason: r,
        )),
      )
      Nil
    }),
  )
  use query_info <- result.try(
    sqlite.introspect_query(db, sql)
    |> result.map_error(fn(err) {
      io.println_error(
        error.to_string(error.SqlError(path: file_path, message: err.message)),
      )
      Nil
    }),
  )
  use _ <- result.try(check_duplicate_columns(query_info.columns, file_path))
  Ok(query.Query(
    name: name,
    sql: sqlite.strip_nullability_suffixes(sql),
    path: file_path,
    parameters: query_info.parameters,
    columns: query_info.columns,
    custom_type_name: custom_type_name,
  ))
}

fn validate_sql(trimmed: String, file_path: String) -> Result(String, Nil) {
  case trimmed {
    "" -> {
      io.println_error(error.to_string(error.EmptySqlFile(path: file_path)))
      Error(Nil)
    }
    sql -> {
      let stripped = case string.ends_with(sql, ";") {
        True ->
          sql
          |> string.drop_end(1)
          |> string.trim
        False -> sql
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

fn check_duplicate_columns(
  columns: List(query.Column),
  file_path: String,
) -> Result(Nil, Nil) {
  let names = list.map(columns, fn(c) { c.name })
  let dupes = find_duplicates(names)
  case dupes {
    [] -> Ok(Nil)
    _ -> {
      io.println_error(
        error.to_string(error.DuplicateColumns(path: file_path, columns: dupes)),
      )
      Error(Nil)
    }
  }
}

fn find_duplicates(names: List(String)) -> List(String) {
  let counts =
    list.fold(names, dict.new(), fn(acc, name) {
      let count = result.unwrap(dict.get(acc, name), 0)
      dict.insert(acc, name, count + 1)
    })
  names
  |> list.unique
  |> list.filter(fn(name) {
    case dict.get(counts, name) {
      Ok(n) if n > 1 -> True
      _ -> False
    }
  })
}

/// Check for semicolons outside of quoted SQL contexts, line comments, and
/// block comments. Delegates to the tokenizer which already handles all
/// quoting and comment styles correctly.
fn contains_semicolon_outside_strings(sql: String) -> Bool {
  tokenize.tokenize(sql)
  |> list.any(fn(t) { t == tokenize.Semicolon })
}

/// Run `gleam format` on generated code. Falls back to the original string
/// if formatting fails (e.g., gleam not on PATH) and prints a warning.
fn format_gleam(code: String) -> String {
  let suffix =
    int.to_string(int.absolute_value(unique_integer()))
    <> "_"
    <> int.to_string(random_integer(999_999_999))
  let tmp_dir = get_tmp_dir()
  let tmp = tmp_dir <> "/marmot_fmt_" <> suffix <> ".gleam"
  case simplifile.write(tmp, code) {
    Error(_) -> {
      io.println_error(
        "warning: Could not write temp file for formatting, skipping gleam format",
      )
      code
    }
    Ok(_) -> {
      let exit_code = run_executable("gleam", ["format", tmp])
      let formatted = case exit_code {
        0 ->
          case simplifile.read(tmp) {
            Ok(result) -> result
            Error(_) -> {
              io.println_error(
                "warning: Could not read formatted file, using unformatted output",
              )
              code
            }
          }
        _ -> {
          io.println_error(
            "warning: gleam format failed (exit code "
            <> int.to_string(exit_code)
            <> "), using unformatted output",
          )
          code
        }
      }
      let _delete_result = simplifile.delete(tmp)
      formatted
    }
  }
}

fn get_tmp_dir() -> String {
  get_env("TMPDIR")
  |> option.lazy_or(fn() { get_env("TMP") })
  |> option.lazy_or(fn() { get_env("TEMP") })
  |> option.unwrap("/tmp")
}

/// Run an executable with arguments, bypassing the shell.
/// Uses erlang:open_port with spawn_executable to avoid shell injection.
/// Returns the exit status code, or -1 if the executable was not found.
fn run_executable(executable: String, args: List(String)) -> Int {
  case find_executable(executable) {
    option.None -> -1
    option.Some(path) -> run_executable_ffi(path, args)
  }
}

@external(erlang, "marmot_ffi", "run_executable")
fn run_executable_ffi(path: String, args: List(String)) -> Int

@external(erlang, "marmot_ffi", "find_executable")
fn find_executable(name: String) -> Option(String)

fn halt(code: Int) -> Nil {
  // init:stop/1 performs a graceful shutdown that flushes I/O buffers,
  // unlike erlang:halt/1 which exits immediately and may lose stderr output
  init_stop(code)
  // init:stop is async; wait for graceful shutdown, then force exit
  timer_sleep(5000)
  erlang_halt(code)
}

@external(erlang, "init", "stop")
fn init_stop(code: Int) -> Nil

@external(erlang, "erlang", "halt")
fn erlang_halt(code: Int) -> Nil

@external(erlang, "erlang", "unique_integer")
fn unique_integer() -> Int

@external(erlang, "rand", "uniform")
fn random_integer(max: Int) -> Int

@external(erlang, "timer", "sleep")
fn timer_sleep(ms: Int) -> Nil

// Uses os:getenv/0 + linear scan rather than os:getenv/1, because on OTP 27
// the /1 variant requires a charlist argument and crashes with badarg when
// passed a Gleam String (binary).
@external(erlang, "os", "getenv")
fn getenv_list_ffi() -> Dynamic

fn get_env(name: String) -> Option(String) {
  let raw = getenv_list_ffi()
  case decode.run(raw, decode.list(decode.string)) {
    Ok(entries) -> find_env(entries, name <> "=")
    Error(_) -> option.None
  }
}

fn find_env(entries: List(String), prefix: String) -> Option(String) {
  case entries {
    [] -> option.None
    [entry, ..rest] ->
      case string.starts_with(entry, prefix) {
        True -> option.Some(string.drop_start(entry, string.length(prefix)))
        False -> find_env(rest, prefix)
      }
  }
}
